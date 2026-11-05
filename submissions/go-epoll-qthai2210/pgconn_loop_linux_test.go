package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/sys/unix"
)

// The loop's Postgres side, driven through socketpairs: a fake client per op, and a fake Postgres whose answers the
// test writes by hand.

func testLoop(t *testing.T) *loop {
	t.Helper()
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(epfd) })
	return &loop{epfd: epfd, conns: map[int]*conn{}, dbs: map[int]*dbConn{}, rbuf: make([]byte, 64<<10), now: time.Now(),
		app: &app{started: time.Now(), jwt: newJWTVerifier([]byte(testSecret))}}
}

func socketpair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	unix.SetNonblock(fds[0], true)
	unix.SetNonblock(fds[1], true)
	t.Cleanup(func() { unix.Close(fds[0]); unix.Close(fds[1]) })
	return fds[0], fds[1]
}

// testClient is a client connection with one op waiting; reading peer gives its response.
func testClient(t *testing.T, l *loop, o *op) (peer int) {
	fd, peer := socketpair(t)
	c := &conn{fd: fd, waiting: true}
	l.conns[fd] = c
	o.c, o.keepAlive = c, true
	return peer
}

func testDB(t *testing.T, l *loop, role dbRole) (*dbConn, int) {
	fd, peer := socketpair(t)
	d := &dbConn{fd: fd, slot: &dbSlot{loop: l, role: role, lost: make(chan struct{}, 1)}, failed: -1, active: l.now}
	l.dbs[fd] = d
	l.dbList[role] = append(l.dbList[role], d)
	return d, peer
}

func drain(fd int) string {
	var out []byte
	buf := make([]byte, 64<<10)
	for {
		n, err := unix.Read(fd, buf)
		if n <= 0 || err != nil {
			return string(out)
		}
		out = append(out, buf[:n]...)
	}
}

// answer writes backend messages to the fake Postgres end and lets the loop read them.
func answer(t *testing.T, l *loop, d *dbConn, peer int, msgs ...pgproto3.BackendMessage) {
	t.Helper()
	var b []byte
	for _, m := range msgs {
		var err error
		if b, err = m.Encode(b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := unix.Write(peer, b); err != nil {
		t.Fatal(err)
	}
	l.readDB(d)
}

func status(resp string) string {
	if len(resp) < 12 {
		return resp
	}
	return resp[9:12]
}

func likeOp(postID int64) *op {
	return &op{kind: opLike, stmt: stmtInsertLike, params: [][]byte{int8Param(1), int8Param(postID)}, formats: allBinary, postID: postID}
}

func TestBatchOfReads(t *testing.T) {
	l := testLoop(t)
	d, pg := testDB(t, l, roleRead)
	var ops []*op
	var peers []int
	for id := int64(1); id <= 3; id++ {
		o := &op{kind: opPost, stmt: stmtPostByID, params: [][]byte{int8Param(id)}, formats: allBinary, postID: id}
		peers = append(peers, testClient(t, l, o))
		ops = append(ops, o)
		l.enqueue(o)
	}
	l.dispatch()
	if got := drain(pg); strings.Count(got, "B") < 3 || !strings.HasSuffix(got, "S\x00\x00\x00\x04") || strings.Count(got, "S\x00\x00\x00\x04") != 1 {
		t.Fatalf("want 3 Bind+Execute under one Sync, got %q", got)
	}
	row := func(id int64) pgproto3.BackendMessage {
		return &pgproto3.DataRow{Values: [][]byte{int8Param(id), []byte("b"), int8Param(0), []byte("u"), {0, 0, 0, 7}}}
	}
	answer(t, l, d, pg,
		&pgproto3.BindComplete{}, row(1), &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.BindComplete{}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")}, // post 2 doesn't exist
		&pgproto3.BindComplete{}, row(3), &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})
	want := []string{"200", "404", "200"}
	for i, p := range peers {
		resp := drain(p)
		if status(resp) != want[i] {
			t.Errorf("op %d: got %q", i+1, resp)
		}
	}
	if len(d.sent) != 0 || len(d.batches) != 0 {
		t.Fatalf("connection should be idle: sent=%d batches=%d", len(d.sent), len(d.batches))
	}
}

func TestWriteBatchWithForeignKeyError(t *testing.T) {
	l := testLoop(t)
	d, pg := testDB(t, l, roleWrite)
	ops := []*op{likeOp(10), likeOp(999999999), likeOp(30)}
	var peers []int
	for _, o := range ops {
		peers = append(peers, testClient(t, l, o))
		l.enqueue(o)
	}
	l.dispatch()
	drain(pg)
	answer(t, l, d, pg,
		&pgproto3.BindComplete{}, &pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
		&pgproto3.BindComplete{}, &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23503", Message: "violates foreign key"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if resp := drain(peers[1]); status(resp) != "404" || !strings.Contains(resp, "post not found") {
		t.Fatalf("failed like: got %q", resp)
	}
	// The like before the error was rolled back with the batch, the one after it skipped: both are queued again,
	// each to go under its own Sync, with no response yet.
	if len(l.pendWrite) != 2 || l.pendWrite[0] != ops[0] || l.pendWrite[1] != ops[2] {
		t.Fatalf("pendWrite = %v", l.pendWrite)
	}
	for _, i := range []int{0, 2} {
		if !ops[i].single || ops[i].affected != 0 || ops[i].errCode != "" || drain(peers[i]) != "" {
			t.Fatalf("op %d not reset for a resend: %+v", i, ops[i])
		}
	}
	l.dispatch()
	if got := drain(pg); strings.Count(got, "S\x00\x00\x00\x04") != 2 {
		t.Fatalf("want two single-op batches, got %q", got)
	}
	answer(t, l, d, pg,
		&pgproto3.BindComplete{}, &pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")}, &pgproto3.ReadyForQuery{TxStatus: 'I'},
		&pgproto3.BindComplete{}, &pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 0")}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	if s0, s2 := status(drain(peers[0])), status(drain(peers[2])); s0 != "201" || s2 != "200" {
		t.Fatalf("resent likes: got %s and %s", s0, s2)
	}
}

func TestLostConnection(t *testing.T) {
	l := testLoop(t)
	d, pg := testDB(t, l, roleWrite)
	sentWrite, read := likeOp(1), &op{kind: opFeed, stmt: stmtFeed}
	pw := testClient(t, l, sentWrite)
	l.enqueue(sentWrite)
	l.dispatch() // its Sync is written: Postgres may have committed it
	unsentWrite := likeOp(2)
	pu := testClient(t, l, unsentWrite)
	l.enqueue(unsentWrite)
	l.dispatch()
	d.written -= 2     // pretend the last two bytes of the second Sync never left the process
	read.since = l.now // as enqueue would have set it
	d.sent = append(d.sent, read)
	pr := testClient(t, l, read)
	drain(pg)

	l.closeDB(d, unix.ECONNRESET)
	if resp := drain(pw); status(resp) != "500" {
		t.Fatalf("write whose Sync was sent: got %q", resp)
	}
	if drain(pu) != "" || drain(pr) != "" || len(l.pendWrite) != 1 || l.pendWrite[0] != unsentWrite || len(l.pendRead) != 1 || l.pendRead[0] != read {
		t.Fatalf("unsent write and read must wait for another connection: writes=%v reads=%v", l.pendWrite, l.pendRead)
	}
	select {
	case <-d.slot.lost:
	default:
		t.Fatal("slot not told to reconnect")
	}
	if len(l.dbList[roleWrite]) != 0 {
		t.Fatal("closed connection still listed")
	}
}

func TestNoConnection(t *testing.T) {
	l := testLoop(t)
	health, fresh, old := &op{kind: opHealth, stmt: stmtHealth}, &op{kind: opFeed, stmt: stmtFeed}, &op{kind: opFeed, stmt: stmtFeed}
	ph, pf, po := testClient(t, l, health), testClient(t, l, fresh), testClient(t, l, old)
	old.since = l.now.Add(-6 * time.Second)
	l.enqueue(health)
	l.enqueue(fresh)
	l.enqueue(old)
	l.dispatch()
	if resp := drain(ph); status(resp) != "503" {
		t.Fatalf("health: %q", resp)
	}
	if resp := drain(po); status(resp) != "500" {
		t.Fatalf("op older than 5 s: %q", resp)
	}
	if drain(pf) != "" || len(l.pendRead) != 1 || l.pendRead[0] != fresh {
		t.Fatal("a fresh op must keep waiting")
	}
}

// A connection that stays silent for silentMax is dropped. Its op has by then waited longer than noConnWait, so it
// gets a 500 rather than waiting more.
func TestSilentConnection(t *testing.T) {
	l := testLoop(t)
	d, _ := testDB(t, l, roleRead)
	o := &op{kind: opFeed, stmt: stmtFeed}
	p := testClient(t, l, o)
	l.enqueue(o)
	l.dispatch()
	l.now = l.now.Add(silentMax + time.Second)
	l.checkSilent()
	if resp := drain(p); !d.closed || len(l.pendRead) != 0 || status(resp) != "500" {
		t.Fatalf("closed=%v pendRead=%d response %q", d.closed, len(l.pendRead), resp)
	}
}

// A read whose connection is lost is sent again, for up to noConnWait after it was first queued, however many
// connections it went through.
func TestLostReadRetriedWithinWait(t *testing.T) {
	l := testLoop(t)
	o := &op{kind: opFeed, stmt: stmtFeed}
	p := testClient(t, l, o)
	l.enqueue(o)
	for i := 0; i < 5; i++ { // five lost connections in a row, well past maxTries sends
		d, _ := testDB(t, l, roleRead)
		l.dispatch()
		l.closeDB(d, unix.ECONNRESET)
		if len(l.pendRead) != 1 || drain(p) != "" {
			t.Fatalf("loss %d: the read must still be queued", i+1)
		}
	}
	l.now = l.now.Add(noConnWait + time.Second)
	d, _ := testDB(t, l, roleRead)
	l.dispatch()
	l.closeDB(d, unix.ECONNRESET)
	if resp := drain(p); status(resp) != "500" {
		t.Fatalf("after noConnWait: %q", resp)
	}
}

// Sanity check on the stream the loop writes for a batch: it must decode as Bind/Execute pairs and one Sync.
func TestBatchWireFormat(t *testing.T) {
	l := testLoop(t)
	_, pg := testDB(t, l, roleRead)
	for i := 0; i < batchRun+3; i++ {
		o := &op{kind: opFeed, stmt: stmtFeed}
		testClient(t, l, o)
		l.enqueue(o)
	}
	l.dispatch()
	got := []byte(drain(pg))
	be := pgproto3.NewBackend(bytes.NewReader(got), nil)
	var types []byte
	for {
		m, err := be.Receive()
		if err != nil {
			break
		}
		switch m.(type) {
		case *pgproto3.Bind:
			types = append(types, 'B')
		case *pgproto3.Execute:
			types = append(types, 'E')
		case *pgproto3.Sync:
			types = append(types, 'S')
		}
	}
	want := strings.Repeat("BE", batchRun) + "S" + strings.Repeat("BE", 3) + "S"
	if string(types) != want {
		t.Fatalf("got %s\nwant %s", types, want)
	}
}
