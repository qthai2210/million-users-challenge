package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"golang.org/x/sys/unix"
)

const (
	batchRun   = 16              // ops per Sync: a loop pass's reads (or writes) are cut into runs of this size
	noConnWait = 5 * time.Second // how long an op waits for a connection before it gets a 500
	silentMax  = 10 * time.Second
	maxTries   = 3 // sends of one op before it gets a 500
)

// dbConn is a Postgres connection owned by one loop, after pgconn has logged it in and prepared the statements.
// Each op is sent as Bind + Execute, and each batch of ops ends with one Sync: Postgres runs a batch as one implicit
// transaction, so a batch of writes is a group commit. Postgres answers strictly in order, so the ops waiting for an
// answer are a FIFO: sent[0:batches[0]] is the batch being answered, and sent[cur] is the op the next result belongs
// to.
type dbConn struct {
	fd       int
	slot     *dbSlot
	in, out  []byte
	outArmed bool
	sent     []*op
	batches  []int
	cur      int
	failed   int       // index in sent of the op of the current batch that got an ErrorResponse, or -1
	written  int64     // bytes written to the socket so far; out holds the ones not written yet
	active   time.Time // last time this connection answered, or was given work while idle
	vals     [][]byte
	closed   bool
}

// pick returns the live connection of a role with the fewest ops in flight, or nil.
func (l *loop) pick(role dbRole) *dbConn {
	var best *dbConn
	for _, d := range l.dbList[role] {
		if best == nil || len(d.sent) < len(best.sent) {
			best = d
		}
	}
	return best
}

// enqueue queues an op to be sent at the end of the loop pass.
func (l *loop) enqueue(o *op) {
	if o.since.IsZero() {
		o.since = l.now
	}
	if o.write() {
		l.pendWrite = append(l.pendWrite, o)
	} else {
		l.pendRead = append(l.pendRead, o)
	}
}

// retry queues an op again after its connection failed or its batch was rolled back, unless it was tried too often.
func (l *loop) retry(o *op, single bool, why string) {
	if o.tries >= maxTries {
		o.errCode, o.errMsg = codeLost, why
		l.complete(o)
		return
	}
	o.reset()
	o.single = o.single || single
	l.enqueue(o)
}

// dispatch sends the queued ops. It runs at the end of every loop pass, and again as long as answering an op (which
// can parse the next pipelined request) queues more.
func (l *loop) dispatch() {
	for len(l.pendRead)+len(l.pendWrite) > 0 {
		reads, writes := l.pendRead, l.pendWrite
		l.pendRead, l.pendWrite = nil, nil
		progress := l.send(reads, roleRead)
		progress = l.send(writes, roleWrite) || progress
		if !progress {
			return
		}
	}
}

// send cuts ops into runs of batchRun under one Sync each, gives each run to the least busy connection of the role,
// then writes each connection's bytes with one write. An op marked single (resent after its batch failed) goes alone.
// It reports whether anything happened.
func (l *loop) send(ops []*op, role dbRole) bool {
	if len(ops) == 0 {
		return false
	}
	if len(l.dbList[role]) == 0 {
		return l.noConnection(ops)
	}
	for len(ops) > 0 {
		k := min(batchRun, len(ops))
		for i := 0; i < k; i++ {
			if ops[i].single {
				k = max(i, 1)
				break
			}
		}
		batch := ops[:k]
		ops = ops[k:]
		d := l.pick(role)
		if len(d.sent) == 0 {
			d.active = l.now
		}
		for _, o := range batch {
			d.out = appendBind(d.out, o.stmt, o.formats, o.params, allBinary)
			d.out = appendExecute(d.out)
			o.tries++
		}
		d.out = appendSync(d.out)
		syncEnd := d.written + int64(len(d.out))
		for _, o := range batch {
			o.syncEnd = syncEnd
		}
		d.sent = append(d.sent, batch...)
		d.batches = append(d.batches, k)
	}
	for _, d := range append([]*dbConn(nil), l.dbList[role]...) {
		if len(d.out) > 0 && !d.outArmed {
			l.flushDB(d)
		}
	}
	return true
}

// noConnection handles ops while the loop has no connection of their role: health checks fail at once, other ops
// wait up to noConnWait and then get a 500.
func (l *loop) noConnection(ops []*op) bool {
	progress := false
	for _, o := range ops {
		switch {
		case o.kind == opHealth:
			o.errCode, o.errMsg = codeNoConn, "no connection to postgres"
		case l.now.Sub(o.since) > noConnWait:
			o.errCode, o.errMsg = codeNoConn, "no connection to postgres for 5 s"
		default:
			l.enqueue(o)
			continue
		}
		l.complete(o)
		progress = true
	}
	return progress
}

func (l *loop) flushDB(d *dbConn) {
	for len(d.out) > 0 {
		n, err := unix.Write(d.fd, d.out)
		if err == unix.EAGAIN {
			if !d.outArmed {
				d.outArmed = true
				unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, d.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP, Fd: int32(d.fd)})
			}
			return
		}
		if err != nil {
			l.closeDB(d, err)
			return
		}
		d.written += int64(n)
		d.out = d.out[:copy(d.out, d.out[n:])]
	}
	if d.outArmed {
		d.outArmed = false
		unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, d.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(d.fd)})
	}
}

func (l *loop) readDB(d *dbConn) {
	n, err := unix.Read(d.fd, l.rbuf)
	if err == unix.EAGAIN {
		return
	}
	if n <= 0 || err != nil {
		if err == nil {
			err = errors.New("closed by postgres")
		}
		l.closeDB(d, err)
		return
	}
	d.active = l.now
	d.in = append(d.in, l.rbuf[:n]...)
	off := 0
	for {
		typ, body, m, err := nextMessage(d.in[off:])
		if err != nil {
			l.closeDB(d, err)
			return
		}
		if m == 0 {
			break
		}
		off += m
		if err := l.dbMessage(d, typ, body); err != nil {
			l.closeDB(d, err)
			return
		}
	}
	d.in = d.in[:copy(d.in, d.in[off:])]
}

// dbMessage applies one backend message to the op it belongs to.
func (l *loop) dbMessage(d *dbConn, typ byte, body []byte) error {
	switch typ {
	case 'D', 'C', 'E':
		if typ == 'E' {
			if code, msg := errorFields(body); connectionLost(code) {
				return fmt.Errorf("%s %s", code, msg)
			}
		}
		if len(d.batches) == 0 || d.cur >= d.batches[0] {
			return fmt.Errorf("unexpected message %q", typ)
		}
		o := d.sent[d.cur]
		switch typ {
		case 'D':
			vals, err := dataRowValues(body, d.vals)
			d.vals = vals
			if err != nil {
				return err
			}
			l.app.row(o, vals)
		case 'C':
			o.affected = commandRows(body)
			d.cur++
		case 'E': // Postgres skips the rest of the batch up to its Sync
			o.errCode, o.errMsg = errorFields(body)
			d.failed = d.cur
			d.cur = d.batches[0]
		}
	case 'Z': // ReadyForQuery: the batch is over, and committed if it wrote
		if len(d.batches) == 0 {
			return errors.New("unexpected ReadyForQuery")
		}
		k, failed := d.batches[0], d.failed
		batch := d.sent[:k]
		d.sent, d.batches, d.cur, d.failed = d.sent[k:], d.batches[1:], 0, -1
		l.answered.Add(int64(k))
		l.syncs.Add(1)
		l.finishBatch(batch, failed)
	case '2', 'n', 'N', 'S', 'A', 'I': // BindComplete, NoData, Notice, ParameterStatus, Notification, EmptyQuery
	default:
		return fmt.Errorf("unexpected message %q", typ)
	}
	return nil
}

// finishBatch answers the ops of a batch once its ReadyForQuery arrived. When one op failed, Postgres skipped the
// ops after it and rolled back the writes before it: those are sent again, each under its own Sync, so one bad
// request can't take the others down with it.
func (l *loop) finishBatch(batch []*op, failed int) {
	for i, o := range batch {
		switch {
		case failed < 0, i == failed, i < failed && !o.write():
			l.complete(o)
		default:
			l.retry(o, true, "batch failed: "+batch[failed].errMsg)
		}
	}
}

// closeDB drops a connection that failed and asks its slot for a new one. Its reads in flight are sent again on
// another connection. A write is sent again only if its batch's Sync never fully left this process, so Postgres
// cannot have committed it; otherwise its fate is unknown and it gets a 500.
func (l *loop) closeDB(d *dbConn, err error) {
	if d.closed {
		return
	}
	d.closed = true
	role := d.slot.role
	log.Printf("loop %d: %s connection lost: %v", l.id, role, err)
	unix.Close(d.fd)
	delete(l.dbs, d.fd)
	list := l.dbList[role]
	for i, x := range list {
		if x == d {
			l.dbList[role] = append(list[:i], list[i+1:]...)
			break
		}
	}
	sent := d.sent
	d.sent, d.batches = nil, nil
	for _, o := range sent {
		if o.write() && d.written >= o.syncEnd {
			o.errCode, o.errMsg = codeLost, err.Error()
			l.complete(o)
		} else {
			l.retry(o, false, err.Error())
		}
	}
	select {
	case d.slot.lost <- struct{}{}:
	default:
	}
}

// checkSilent drops connections that owe answers and have said nothing for silentMax.
func (l *loop) checkSilent() {
	for _, d := range l.dbs {
		if len(d.sent) > 0 && l.now.Sub(d.active) > silentMax {
			l.closeDB(d, fmt.Errorf("no answer for %s", silentMax))
		}
	}
}

// complete writes an op's response to its client connection, then lets that connection continue with the requests
// it had pipelined behind it.
func (l *loop) complete(o *op) {
	c := o.c
	if !c.closed {
		if c.out == nil {
			c.out = l.getBuf()
		}
		c.out = l.app.finish(o, c.out)
		c.waiting = false
	}
	l.app.freeOp(o)
	if c.closed {
		return
	}
	if !c.close {
		l.process(c)
	}
	l.flush(c)
}
