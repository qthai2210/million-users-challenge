package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"
)

// app holds what handlers share within one loop. Only the HMAC key, a scratch buffer and the process start time
// live here: nothing derived from requests or data (rule 7).
type app struct {
	started time.Time
	jwt     *jwtVerifier
	body    []byte // scratch buffer for the response body, reused by every response of the loop
	free    []*op  // answered ops, reused: one op is made per request that needs Postgres
	gz      *gzip.Writer
	gzOut   byteWriter
}

// gzipMin is the smallest body worth compressing: in practice the feed (~5 KB, ~2 KB gzipped). Smaller answers
// cost more CPU to compress than the bytes they save.
const gzipMin = 1024

type byteWriter struct{ b []byte }

func (w *byteWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// appendBody appends a response, gzipped (fastest level, one reused compressor per loop) when the client accepts
// it and the body is large enough.
func (a *app) appendBody(out []byte, status int, body []byte, keepAlive, gz bool) []byte {
	if !gz || len(body) < gzipMin {
		return appendJSON(out, status, body, keepAlive)
	}
	if a.gz == nil {
		a.gz, _ = gzip.NewWriterLevel(&a.gzOut, gzip.BestSpeed)
	}
	a.gzOut.b = a.gzOut.b[:0]
	a.gz.Reset(&a.gzOut)
	a.gz.Write(body)
	a.gz.Close()
	return appendResponse(out, status, a.gzOut.b, keepAlive, true)
}

const maxFreeOps = 4096

// newOp takes an op from the free list, or makes one.
func (a *app) newOp(kind opKind, stmt string) *op {
	var o *op
	if n := len(a.free); n > 0 {
		o, a.free = a.free[n-1], a.free[:n-1]
	} else {
		o = new(op)
	}
	o.kind, o.stmt = kind, stmt
	return o
}

// freeOp returns an answered op to the free list, keeping its buffers.
func (a *app) freeOp(o *op) {
	*o = op{body: o.body[:0], textBuf: o.textBuf[:0]}
	if len(a.free) < maxFreeOps {
		a.free = append(a.free, o)
	}
}

type opKind uint8

const (
	opHealth opKind = iota
	opFeed
	opPost
	opCreate
	opLike
)

// Internal codes for ops that never got an answer from Postgres.
const (
	codeNoConn = "no-connection"
	codeLost   = "connection-lost"
)

// op is one request waiting for Postgres. It holds copies of what its response needs, since the request bytes it was
// parsed from are reused once its connection reads again.
type op struct {
	kind      opKind
	c         *conn
	keepAlive bool
	gzip      bool // the client accepts a gzipped answer
	stmt      string
	params    [][]byte
	formats   []int16

	postID int64
	user   user
	text   string

	body     []byte // post objects, written row by row (feed, post)
	rows     int64  // DataRows received
	affected int64  // rows in the CommandComplete tag
	id       int64  // insert_post RETURNING id
	created  int64  // insert_post RETURNING created_at
	errCode  string // SQLSTATE of an ErrorResponse, or codeNoConn / codeLost
	errMsg   string

	ibuf    [2][8]byte // storage for binary int8 parameters
	pbuf    [2][]byte  // params points here
	textBuf []byte     // storage for the text parameter of insert_post

	since   time.Time // first queued, for the wait for a connection
	tries   int       // times sent
	syncEnd int64     // stream offset, on its connection, of the end of its batch's Sync
	single  bool      // send under its own Sync (its batch failed once)
}

func (o *op) write() bool { return o.kind == opCreate || o.kind == opLike }

// setInt8 sets parameter i (and the ones before it stay) to a binary int8, without allocating.
func (o *op) setInt8(i int, v int64) {
	binary.BigEndian.PutUint64(o.ibuf[i][:], uint64(v))
	o.pbuf[i] = o.ibuf[i][:]
	o.params = o.pbuf[:i+1]
}

// reset clears the results before the op is sent again (since, tries and single stay).
func (o *op) reset() {
	o.body, o.rows, o.affected, o.id, o.created, o.errCode, o.errMsg = o.body[:0], 0, 0, 0, 0, "", ""
}

// handle answers one request by appending the response to out, or returns an op when the answer needs Postgres. A
// panic answers 500 instead of killing the process (a crashed server is not restarted).
func (a *app) handle(r *request, out []byte) (res []byte, o *op) {
	n := len(out)
	defer func() {
		if p := recover(); p != nil {
			log.Printf("panic serving %s %s: %v", r.method, r.path, p)
			res, o = appendError(out[:n], 500, "internal server error", r.keepAlive), nil
		}
	}()
	return a.route(r, out)
}

func (a *app) route(r *request, out []byte) ([]byte, *op) {
	path, method := string(r.path), string(r.method)
	switch {
	case path == "/health" && method == "GET":
		return out, a.newOp(opHealth, stmtHealth)
	case path == "/feed" && method == "GET":
		return out, a.newOp(opFeed, stmtFeed)
	case path == "/posts" && method == "POST":
		return a.createPost(r, out)
	case strings.HasPrefix(path, "/posts/"):
		rest := path[len("/posts/"):]
		if id, tail, ok := strings.Cut(rest, "/"); ok {
			if tail == "like" && method == "POST" {
				return a.likePost(r, id, out)
			}
		} else if method == "GET" {
			return a.getPost(r, rest, out)
		}
	}
	return appendError(out, 404, "not found", r.keepAlive), nil
}

// authenticate checks the token; on failure it also returns the 401 response.
func (a *app) authenticate(r *request, out []byte) (user, []byte, bool) {
	u, res := a.jwt.verify(r.auth, time.Now().Unix())
	if res != authOK {
		return user{}, appendError(out, 401, authMessage[res], r.keepAlive), false
	}
	return u, out, true
}

func (a *app) getPost(r *request, rawID string, out []byte) ([]byte, *op) {
	id, ok := parsePositiveInt(rawID)
	if !ok {
		return appendError(out, 400, "invalid post id", r.keepAlive), nil
	}
	o := a.newOp(opPost, stmtPostByID)
	o.setInt8(0, id)
	o.formats, o.postID = allBinary, id
	return out, o
}

func (a *app) createPost(r *request, out []byte) ([]byte, *op) {
	u, out, ok := a.authenticate(r, out)
	if !ok {
		return out, nil
	}
	text, msg := parsePostBody(r.body)
	if msg != "" {
		return appendError(out, 400, msg, r.keepAlive), nil
	}
	o := a.newOp(opCreate, stmtInsertPost)
	o.setInt8(0, u.id)
	o.textBuf = append(o.textBuf[:0], text...)
	o.pbuf[1] = o.textBuf
	o.params = o.pbuf[:2]
	o.formats, o.user, o.text = idAndText, u, text
	return out, o
}

func (a *app) likePost(r *request, rawID string, out []byte) ([]byte, *op) {
	u, out, ok := a.authenticate(r, out)
	if !ok {
		return out, nil
	}
	postID, ok := parsePositiveInt(rawID)
	if !ok {
		return appendError(out, 400, "invalid post id", r.keepAlive), nil
	}
	o := a.newOp(opLike, stmtInsertLike)
	o.setInt8(0, u.id)
	o.setInt8(1, postID)
	o.formats, o.user, o.postID = allBinary, u, postID
	return out, o
}

// row takes one DataRow of o's result (binary columns).
func (a *app) row(o *op, v [][]byte) {
	switch o.kind {
	case opFeed, opPost: // id int8, body text, created_at timestamptz, username text, like_count int4
		if o.rows > 0 {
			o.body = append(o.body, ',')
		}
		o.body = appendPost(o.body, readInt8(v[0]), v[1], readInt8(v[2]), v[3], readInt4(v[4]))
	case opCreate: // RETURNING id, created_at
		o.id, o.created = readInt8(v[0]), readInt8(v[1])
	}
	o.rows++
}

// finish appends o's response to out once Postgres has answered it, or failed to.
func (a *app) finish(o *op, out []byte) (res []byte) {
	n, ka := len(out), o.keepAlive
	defer func() {
		if p := recover(); p != nil {
			log.Printf("panic finishing %s: %v", o.stmt, p)
			res = appendError(out[:n], 500, "internal server error", ka)
		}
	}()
	if o.errCode != "" {
		switch {
		case o.kind == opHealth:
			msg, _ := json.Marshal(o.errMsg)
			return appendJSON(out, 503, `{"status":"degraded","db":"unreachable","error":`+string(msg)+`}`, ka)
		case o.kind == opLike && o.errCode == "23503": // foreign key: the post doesn't exist
			return appendError(out, 404, "post not found", ka)
		}
		log.Printf("%s: %s %s", o.stmt, o.errCode, o.errMsg)
		return appendError(out, 500, "internal server error", ka)
	}
	b := a.body[:0]
	status := 200
	switch o.kind {
	case opHealth:
		b = append(b, `{"status":"ok","db":"ok","uptime_s":`...)
		b = strconv.AppendInt(b, int64(time.Since(a.started)/time.Second), 10)
		b = append(b, '}')
	case opFeed:
		b = append(append(append(b, `{"posts":[`...), o.body...), ']', '}')
	case opPost:
		if o.rows == 0 {
			return appendError(out, 404, "post not found", ka)
		}
		b = append(append(append(b, `{"post":`...), o.body...), '}')
	case opCreate:
		status = 201
		b = append(b, `{"post":`...)
		b = appendPost(b, o.id, o.text, o.created, o.user.username, 0)
		b = append(b, '}')
	case opLike:
		already := "true"
		if o.affected == 1 {
			status, already = 201, "false"
		}
		b = append(b, `{"liked":true,"already_liked":`...)
		b = append(b, already...)
		b = append(b, `,"post_id":`...)
		b = strconv.AppendInt(b, o.postID, 10)
		b = append(b, '}')
	}
	a.body = b
	return a.appendBody(out, status, b, ka, o.gzip)
}
