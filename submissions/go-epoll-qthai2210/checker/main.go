// Command checker hammers a running server with concurrent requests, checks every answer, then checks Postgres
// against the answers: a like is counted once, a post is written once, reads match the database, and pipelined,
// byte-by-byte and slowly read requests are answered in order. With -kill it also terminates the server's Postgres
// backends every so often while it runs. Start the server on a fresh copy of the seed.
//
//	go run ./checker -db postgres://postgres:postgres@pg:5432/feed_check -duration 20s -kill 2s
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	likeFirst, likeLast = 1, 200         // posts the like workers like, again and again
	pipeFirst, pipeLast = 201, 400       // posts the pipelining checks like (statuses only)
	readFirst, readLast = 400000, 400999 // posts nobody likes during the run: reads must match Postgres exactly
	missingPost         = 999999999
)

var (
	host     = flag.String("host", "127.0.0.1", "server host")
	ports    = flag.String("ports", "3000,3001", "server ports")
	dbURL    = flag.String("db", "", "the server's DATABASE_URL, to check the results and to kill backends")
	tokens   = flag.String("tokens", "../../seed/tokens.json", "seed/tokens.json")
	workers  = flag.Int("workers", 32, "concurrent request workers")
	duration = flag.Duration("duration", 20*time.Second, "how long to run")
	kill     = flag.Duration("kill", 0, "terminate the server's Postgres backends this often (0: never)")
)

type token struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Token    string `json:"token"`
}

// post is the wire format of a post object, in the spec's key order.
type post struct {
	ID        int64  `json:"id"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
	Author    string `json:"author"`
	LikeCount int64  `json:"like_count"`
}

type checker struct {
	runID      string
	toks       []token
	db         *pgx.Conn
	expected   map[int64]string // read-only posts: exact response body
	client     *http.Client
	stats      sync.Map // name -> *atomic.Int64
	mu         sync.Mutex
	violations []string
}

func (c *checker) count(name string) {
	v, _ := c.stats.LoadOrStore(name, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (c *checker) fail(format string, args ...any) {
	c.mu.Lock()
	c.violations = append(c.violations, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

func (c *checker) base(i int) string {
	ps := strings.Split(*ports, ",")
	return "http://" + net.JoinHostPort(*host, ps[i%len(ps)])
}

func (c *checker) do(method, url, tok, body string) (int, []byte, error) {
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, bytes.TrimRight(b, "\n"), err
}

func formatTime(us int64) string {
	return time.UnixMicro(us).UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
}

func encode(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false) // like JSON.stringify
	enc.Encode(v)
	return strings.TrimRight(b.String(), "\n")
}

// likeState follows one (user, post) pair; one worker owns each user, so its likes are sequential.
type likeState struct {
	pre     bool // the like was in the seed
	known   bool // the server told us the like exists
	unknown bool // a 500: the like may or may not have been written
}

type created struct {
	id      int64
	body    string
	user    token
	created string
}

func main() {
	flag.Parse()
	if *dbURL == "" {
		fmt.Fprintln(os.Stderr, "-db is required")
		os.Exit(2)
	}
	ctx := context.Background()
	c := &checker{runID: strconv.FormatInt(time.Now().UnixNano(), 36), expected: map[int64]string{}}
	raw, err := os.ReadFile(*tokens)
	must(err)
	must(json.Unmarshal(raw, &c.toks))
	c.db, err = connectDB()
	must(err)
	c.client = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		MaxIdleConnsPerHost: *workers * 2, DisableCompression: true}}

	// The read-only posts, as the server must return them.
	rows, err := c.db.Query(ctx, `SELECT p.id, p.body, (extract(epoch FROM p.created_at) * 1000000)::bigint, u.username,
		(SELECT count(*) FROM likes l WHERE l.post_id = p.id) FROM posts p JOIN users u ON u.id = p.user_id
		WHERE p.id BETWEEN $1 AND $2`, readFirst, readLast)
	must(err)
	for rows.Next() {
		var p post
		var us int64
		must(rows.Scan(&p.ID, &p.Body, &us, &p.Author, &p.LikeCount))
		p.CreatedAt = formatTime(us)
		c.expected[p.ID] = encode(map[string]post{"post": p})
	}
	must(rows.Err())

	// Like workers own users 1001..1000+workers; post writers own the next ones.
	likers := c.toks[1000 : 1000+*workers]
	pre := map[[2]int64]bool{}
	ids := make([]int64, len(likers))
	for i, t := range likers {
		ids[i] = t.ID
	}
	rows, err = c.db.Query(ctx, `SELECT user_id, post_id FROM likes WHERE user_id = ANY($1) AND post_id BETWEEN $2 AND $3`,
		ids, likeFirst, likeLast)
	must(err)
	for rows.Next() {
		var u, p int64
		must(rows.Scan(&u, &p))
		pre[[2]int64{u, p}] = true
	}
	must(rows.Err())

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	states := make([]map[int64]*likeState, len(likers))
	var posts []created
	var unknownBodies []string
	var postsMu sync.Mutex

	for w, t := range likers {
		states[w] = map[int64]*likeState{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for i := 0; time.Now().Before(deadline); i++ {
				switch n := r.IntN(100); {
				case n < 45:
					c.like(w, t, likeFirst+r.Int64N(likeLast-likeFirst+1), states[w], pre)
				case n < 50:
					st, b, err := c.do("POST", c.base(w)+"/posts/"+strconv.Itoa(missingPost)+"/like", t.Token, "")
					c.expect("like missing post", err, st, b, 404, `{"error":"post not found"}`)
				case n < 60:
					body := fmt.Sprintf("checker %s w%d #%d", c.runID, w, i)
					st, b, err := c.do("POST", c.base(w)+"/posts", t.Token, `{"body":"  `+body+` "}`)
					switch {
					case err == nil && st == 201:
						var got map[string]post
						if json.Unmarshal(b, &got) != nil || got["post"].Body != body || got["post"].Author != t.Username || got["post"].LikeCount != 0 {
							c.fail("create: bad 201 body %s", b)
							continue
						}
						postsMu.Lock()
						posts = append(posts, created{got["post"].ID, body, t, got["post"].CreatedAt})
						postsMu.Unlock()
						c.count("create 201")
					case err == nil && st == 500:
						postsMu.Lock()
						unknownBodies = append(unknownBodies, body)
						postsMu.Unlock()
						c.count("create 500")
					default:
						c.fail("create: status %d err %v body %s", st, err, b)
					}
				case n < 95:
					id := readFirst + r.Int64N(readLast-readFirst+1)
					st, b, err := c.do("GET", c.base(w)+"/posts/"+strconv.FormatInt(id, 10), "", "")
					c.expect("read", err, st, b, 200, c.expected[id])
				default:
					st, b, err := c.do("GET", c.base(w)+"/feed", "", "")
					var feed struct{ Posts []post }
					if err == nil && st == 200 && (json.Unmarshal(b, &feed) != nil || len(feed.Posts) != 20) {
						c.fail("feed: not 20 posts: %.200s", b)
					}
					c.expect("feed", err, st, nil, 200, "")
				}
			}
		}()
	}
	wg.Add(3)
	go func() { defer wg.Done(); c.loopUntil(deadline, c.pipelined) }()
	go func() { defer wg.Done(); c.loopUntil(deadline, c.byteByByte) }()
	go func() { defer wg.Done(); c.loopUntil(deadline, c.slowReader) }()
	stopKill := make(chan struct{})
	if *kill > 0 {
		go c.killer(*kill, stopKill)
	}
	wg.Wait()
	close(stopKill)
	time.Sleep(200 * time.Millisecond)

	// Likes in Postgres versus the answers.
	rows, err = c.db.Query(ctx, `SELECT user_id, post_id FROM likes WHERE user_id = ANY($1) AND post_id BETWEEN $2 AND $3`,
		ids, likeFirst, likeLast)
	must(err)
	inDB := map[[2]int64]bool{}
	for rows.Next() {
		var u, p int64
		must(rows.Scan(&u, &p))
		inDB[[2]int64{u, p}] = true
	}
	must(rows.Err())
	for w, t := range likers {
		for p, s := range states[w] {
			has := inDB[[2]int64{t.ID, p}]
			if (s.pre || s.known) && !has {
				c.fail("like user %d post %d: answered as liked, missing in postgres", t.ID, p)
			}
			if !s.pre && !s.known && !s.unknown && has {
				c.fail("like user %d post %d: in postgres but never answered 201", t.ID, p)
			}
		}
	}
	// Created posts: exactly one row each, as answered. One scan reads every post this run wrote.
	type row struct {
		id, user, us int64
	}
	written := map[string][]row{}
	rows, err = c.db.Query(ctx, `SELECT id, user_id, body, (extract(epoch FROM created_at) * 1000000)::bigint FROM posts
		WHERE body LIKE $1`, "checker "+c.runID+" %")
	must(err)
	for rows.Next() {
		var r row
		var body string
		must(rows.Scan(&r.id, &r.user, &body, &r.us))
		written[body] = append(written[body], r)
	}
	must(rows.Err())
	for _, p := range posts {
		rs := written[p.body]
		switch {
		case len(rs) != 1:
			c.fail("post %q (answered 201, id %d) written %d times", p.body, p.id, len(rs))
		case rs[0].id != p.id || rs[0].user != p.user.ID || formatTime(rs[0].us) != p.created:
			c.fail("post %d: postgres has id %d user %d created %s, answered user %d created %s",
				p.id, rs[0].id, rs[0].user, formatTime(rs[0].us), p.user.ID, p.created)
		}
	}
	for _, b := range unknownBodies {
		if n := len(written[b]); n > 1 {
			c.fail("post %q (answered 500) written %d times", b, n)
		}
	}
	if n := len(written) - len(posts) - len(unknownBodies); n > 0 {
		c.fail("%d posts in postgres that the server never answered for", n)
	}
	// A sample of the created posts read back through the server.
	for i, p := range posts {
		if i >= 200 {
			break
		}
		want := encode(map[string]post{"post": {p.id, p.body, p.created, p.user.Username, 0}})
		st, b, err := c.do("GET", c.base(i)+"/posts/"+strconv.FormatInt(p.id, 10), "", "")
		c.expect("read back", err, st, b, 200, want)
	}

	var names []string
	c.stats.Range(func(k, _ any) bool { names = append(names, k.(string)); return true })
	for _, n := range sortStrings(names) {
		v, _ := c.stats.Load(n)
		fmt.Printf("%-28s %d\n", n, v.(*atomic.Int64).Load())
	}
	fmt.Printf("%-28s %d\n", "created posts checked", len(posts))
	fmt.Printf("%-28s %d\n", "like pairs checked", func() (n int) {
		for _, s := range states {
			n += len(s)
		}
		return
	}())
	if len(c.violations) > 0 {
		fmt.Printf("VIOLATIONS: %d\n", len(c.violations))
		for i, v := range c.violations {
			if i == 30 {
				fmt.Println("...")
				break
			}
			fmt.Println("  " + v)
		}
		os.Exit(1)
	}
	fmt.Println("OK: 0 violations")
}

// expect checks one answer: the wanted status and, when want is not empty, the exact body. A 500 is allowed (it is
// what a request gets when its Postgres connection is killed under it) and counted.
func (c *checker) expect(name string, err error, st int, body []byte, wantSt int, want string) {
	switch {
	case err != nil:
		c.fail("%s: %v", name, err)
	case st == 500:
		c.count(name + " 500")
	case st != wantSt || (want != "" && string(body) != want):
		c.fail("%s: got %d %.300s, want %d %.300s", name, st, body, wantSt, want)
	default:
		c.count(name + " ok")
	}
}

func (c *checker) like(w int, t token, p int64, states map[int64]*likeState, pre map[[2]int64]bool) {
	s := states[p]
	if s == nil {
		s = &likeState{pre: pre[[2]int64{t.ID, p}]}
		states[p] = s
	}
	st, b, err := c.do("POST", c.base(w)+"/posts/"+strconv.FormatInt(p, 10)+"/like", t.Token, "")
	want := func(already bool) string {
		return fmt.Sprintf(`{"liked":true,"already_liked":%v,"post_id":%d}`, already, p)
	}
	switch {
	case err != nil:
		c.fail("like: %v", err)
	case st == 201 && string(b) == want(false):
		if s.pre || s.known {
			c.fail("like user %d post %d: 201 for a like that already existed", t.ID, p)
		}
		s.known = true
		c.count("like 201")
	case st == 200 && string(b) == want(true):
		if !s.pre && !s.known && !s.unknown {
			c.fail("like user %d post %d: already_liked before any like", t.ID, p)
		}
		s.known = true
		c.count("like 200")
	case st == 500:
		s.unknown = true
		c.count("like 500")
	default:
		c.fail("like user %d post %d: got %d %s", t.ID, p, st, b)
	}
}

func (c *checker) loopUntil(deadline time.Time, f func(int)) {
	for i := 0; time.Now().Before(deadline); i++ {
		f(i)
	}
}

// rawResponses reads n HTTP responses (Content-Length framed) from r.
func rawResponses(r *bufio.Reader, n int) ([]int, [][]byte, error) {
	var sts []int
	var bodies [][]byte
	for i := 0; i < n; i++ {
		line, err := r.ReadString('\n')
		if err != nil {
			return sts, bodies, err
		}
		st, _ := strconv.Atoi(strings.Fields(line + " x x")[1])
		cl := 0
		for {
			h, err := r.ReadString('\n')
			if err != nil {
				return sts, bodies, err
			}
			if h == "\r\n" {
				break
			}
			if k, v, ok := strings.Cut(h, ":"); ok && strings.EqualFold(k, "content-length") {
				cl, _ = strconv.Atoi(strings.TrimSpace(v))
			}
		}
		b := make([]byte, cl)
		if _, err := io.ReadFull(r, b); err != nil {
			return sts, bodies, err
		}
		sts, bodies = append(sts, st), append(bodies, b)
	}
	return sts, bodies, nil
}

func (c *checker) dial(i int) (net.Conn, error) {
	ps := strings.Split(*ports, ",")
	return net.DialTimeout("tcp", net.JoinHostPort(*host, ps[i%len(ps)]), 5*time.Second)
}

// pipelined sends a read, a like and an unknown path in one write and checks the answers come back in that order.
func (c *checker) pipelined(i int) {
	conn, err := c.dial(i)
	if err != nil {
		c.fail("pipelined: %v", err)
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	t := c.toks[2000+i%50]
	id := readFirst + int64(i%1000)
	fmt.Fprintf(conn, "GET /posts/%d HTTP/1.1\r\nHost: c\r\n\r\nPOST /posts/%d/like HTTP/1.1\r\nHost: c\r\nAuthorization: Bearer %s\r\nContent-Length: 0\r\n\r\nGET /nope HTTP/1.1\r\nHost: c\r\n\r\n",
		id, pipeFirst+i%(pipeLast-pipeFirst+1), t.Token)
	sts, bodies, err := rawResponses(bufio.NewReader(conn), 3)
	if err != nil {
		c.fail("pipelined: %v after %v", err, sts)
		return
	}
	if (sts[0] != 200 && sts[0] != 500) || (sts[0] == 200 && string(bodies[0]) != c.expected[id]) ||
		(sts[1] != 200 && sts[1] != 201 && sts[1] != 500) || sts[2] != 404 {
		c.fail("pipelined: statuses %v, first body %.120s", sts, bodies[0])
		return
	}
	c.count("pipelined ok")
}

// byteByByte sends a request one byte per write.
func (c *checker) byteByByte(i int) {
	conn, err := c.dial(i)
	if err != nil {
		c.fail("byte by byte: %v", err)
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	id := readFirst + int64(i%1000)
	req := fmt.Sprintf("GET /posts/%d HTTP/1.1\r\nHost: c\r\n\r\n", id)
	for j := 0; j < len(req); j++ {
		conn.Write([]byte{req[j]})
		time.Sleep(200 * time.Microsecond)
	}
	sts, bodies, err := rawResponses(bufio.NewReader(conn), 1)
	if err != nil {
		c.fail("byte by byte: %v", err)
		return
	}
	if sts[0] != 500 && (sts[0] != 200 || string(bodies[0]) != c.expected[id]) {
		c.fail("byte by byte: %d %.120s", sts[0], bodies[0])
		return
	}
	c.count("byte by byte ok")
}

// slowReader pipelines 30 feeds, then reads the ~150 KB of answers a little at a time, so the server's socket buffer
// fills and it has to wait for EPOLLOUT.
func (c *checker) slowReader(i int) {
	conn, err := c.dial(i)
	if err != nil {
		c.fail("slow reader: %v", err)
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(60 * time.Second))
	conn.(*net.TCPConn).SetReadBuffer(4096)
	conn.Write([]byte(strings.Repeat("GET /feed HTTP/1.1\r\nHost: c\r\n\r\n", 30)))
	time.Sleep(300 * time.Millisecond)
	sts, bodies, err := rawResponses(bufio.NewReaderSize(slowConn{conn}, 1024), 30)
	if err != nil {
		c.fail("slow reader: %v after %d answers", err, len(sts))
		return
	}
	for k, st := range sts {
		var feed struct{ Posts []post }
		if st != 500 && (st != 200 || json.Unmarshal(bodies[k], &feed) != nil || len(feed.Posts) != 20) {
			c.fail("slow reader: answer %d: %d %.120s", k, st, bodies[k])
			return
		}
	}
	c.count("slow reader ok")
}

type slowConn struct{ net.Conn }

func (s slowConn) Read(p []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return s.Conn.Read(p[:min(len(p), 1024)])
}

// killer terminates every Postgres backend of the server's database (but its own) every interval.
func (c *checker) killer(every time.Duration, stop chan struct{}) {
	conn, err := connectDB()
	if err != nil {
		c.fail("killer: %v", err)
		return
	}
	defer conn.Close(context.Background())
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			var n int64
			if err := conn.QueryRow(context.Background(), `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
				WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'
				AND application_name <> 'checker'`).Scan(&n); err != nil {
				c.fail("killer: %v", err)
				return
			}
			c.count("backends killed")
			c.stats.LoadOrStore("backends killed (connections)", new(atomic.Int64))
			v, _ := c.stats.Load("backends killed (connections)")
			v.(*atomic.Int64).Add(n)
		}
	}
}

// connectDB connects as application "checker", which the killer spares.
func connectDB() (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(*dbURL)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["application_name"] = "checker"
	return pgx.ConnectConfig(context.Background(), cfg)
}

func sortStrings(s []string) []string {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	return s
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
