package main

import (
	"errors"
	"fmt"
	"log"

	"golang.org/x/sys/unix"
)

// dbConn is a Postgres connection owned by one loop, after pgconn has logged it in and prepared the statements.
// Each op is sent as Bind + Execute, and each batch ends with a Sync. Postgres answers strictly in order, so the ops
// waiting for an answer are a FIFO: sent[0:batches[0]] is the batch being answered, and sent[cur] is the op the next
// result belongs to.
type dbConn struct {
	fd       int
	slot     *dbSlot
	in, out  []byte
	outArmed bool
	sent     []*op
	batches  []int
	cur      int
	failed   int // index in sent of the op of the current batch that got an ErrorResponse, or -1
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
	if o.write() {
		l.pendWrite = append(l.pendWrite, o)
	} else {
		l.pendRead = append(l.pendRead, o)
	}
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

// send hands ops to a connection of the given role, one Sync per op, and reports whether anything happened. With no
// connection, health checks fail at once and the other ops go back to the queue to wait for one.
func (l *loop) send(ops []*op, role dbRole) bool {
	if len(ops) == 0 {
		return false
	}
	d := l.pick(role)
	if d == nil {
		progress := false
		for _, o := range ops {
			if o.kind == opHealth {
				o.errCode, o.errMsg = codeNoConn, "no connection to postgres"
				l.complete(o)
				progress = true
			} else {
				l.enqueue(o)
			}
		}
		return progress
	}
	for _, o := range ops {
		d.out = appendBind(d.out, o.stmt, o.formats, o.params, allBinary)
		d.out = appendExecute(d.out)
		d.out = appendSync(d.out)
		d.sent = append(d.sent, o)
		d.batches = append(d.batches, 1)
	}
	l.flushDB(d)
	return true
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
		l.finishBatch(batch, failed)
	case '2', 'n', 'N', 'S', 'A', 'I': // BindComplete, NoData, Notice, ParameterStatus, Notification, EmptyQuery
	default:
		return fmt.Errorf("unexpected message %q", typ)
	}
	return nil
}

// finishBatch answers the ops of a batch once its ReadyForQuery arrived. When one op failed, Postgres skipped the
// ops after it and rolled back the writes before it: those are sent again, each under its own Sync.
func (l *loop) finishBatch(batch []*op, failed int) {
	for i, o := range batch {
		switch {
		case failed < 0, i == failed, i < failed && !o.write():
			l.complete(o)
		default:
			o.reset()
			l.enqueue(o)
		}
	}
}

// closeDB drops a connection that failed and asks its slot for a new one. Reads in flight are sent again elsewhere;
// writes in flight get a 500, since they may have committed.
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
	for _, o := range d.sent {
		if o.write() {
			o.errCode, o.errMsg = codeLost, err.Error()
			l.complete(o)
		} else {
			o.reset()
			l.enqueue(o)
		}
	}
	d.sent, d.batches = nil, nil
	select {
	case d.slot.lost <- struct{}{}:
	default:
	}
}

// complete writes an op's response to its client connection, then lets that connection continue with the requests
// it had pipelined behind it.
func (l *loop) complete(o *op) {
	c := o.c
	if c.closed {
		return
	}
	c.out = l.app.finish(o, c.out)
	c.waiting = false
	if !c.close {
		l.process(c)
	}
	l.flush(c)
}
