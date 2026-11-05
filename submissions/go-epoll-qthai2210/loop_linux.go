package main

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// conn is a client connection. P3 keeps it in a map; P4 moves it to an off-heap slot table indexed by fd.
type conn struct {
	fd        int
	in        []byte // received bytes not yet parsed; nil (back in the loop's pool) when there are none
	out       []byte // response bytes not yet written; nil when there are none
	close     bool   // close once out is flushed
	outArmed  bool   // EPOLLOUT is registered (out did not fit in the socket buffer)
	continued bool   // "100 Continue" already sent for the request being received
	waiting   bool   // a request is waiting for Postgres; later pipelined requests wait in in, so answers stay in order
	closed    bool
}

type handoff struct {
	fd   int
	slot *dbSlot
}

// loop is one epoll event loop on one locked OS thread, with its own SO_REUSEPORT listeners on every port, its own
// client connections and its own Postgres connections, all in one epoll set. A request never leaves the loop that
// accepted its connection, and loops share nothing, so there are no locks, except for the hand-off of newly opened
// Postgres connections from their slot goroutines (mu, incoming, and the eventfd that wakes the loop).
type loop struct {
	id        int
	epfd      int
	efd       int
	listeners map[int]bool
	conns     map[int]*conn
	dbs       map[int]*dbConn
	dbList    [2][]*dbConn // live connections by role
	pendRead  []*op
	pendWrite []*op
	app       *app
	rbuf      []byte
	bufs      [][]byte  // free connection buffers
	now       time.Time // start of the current pass
	checked   time.Time // last checkSilent

	// read by the stats logger
	nconns   atomic.Int64 // open client connections
	answered atomic.Int64 // ops answered by Postgres
	syncs    atomic.Int64 // batches (Syncs) answered

	mu       sync.Mutex
	incoming []handoff
}

// Connection buffers come from a per-loop pool and go back as soon as they are empty, so an idle connection holds
// none, whatever its last response was. Buffers that grew large are left to the GC.
const (
	bufSize      = 8 << 10
	maxPooledBuf = 64 << 10
	maxFreeBufs  = 1024
)

func (l *loop) getBuf() []byte {
	if n := len(l.bufs); n > 0 {
		b := l.bufs[n-1]
		l.bufs = l.bufs[:n-1]
		return b
	}
	return make([]byte, 0, bufSize)
}

func (l *loop) putBuf(b []byte) {
	if cap(b) == 0 || cap(b) > maxPooledBuf || len(l.bufs) >= maxFreeBufs {
		return
	}
	l.bufs = append(l.bufs, b[:0])
}

// loopCount is one loop per CPU this process may run on. runtime.NumCPU follows the affinity mask (taskset,
// cpusets), not the machine's core count, which is what the 8- and 4-core runs restrict.
func loopCount() int { return runtime.NumCPU() }

func newLoop(id int, host net.IP, ports []int, a *app) (*loop, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	efd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("eventfd: %w", err)
	}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, efd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(efd)}); err != nil {
		return nil, fmt.Errorf("epoll_ctl eventfd: %w", err)
	}
	l := &loop{id: id, epfd: epfd, efd: efd, listeners: map[int]bool{}, conns: map[int]*conn{}, dbs: map[int]*dbConn{},
		app: a, rbuf: make([]byte, 64<<10)}
	for _, p := range ports {
		fd, err := listen(host, p)
		if err != nil {
			return nil, err
		}
		if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)}); err != nil {
			return nil, fmt.Errorf("epoll_ctl listener: %w", err)
		}
		l.listeners[fd] = true
	}
	return l, nil
}

func listen(host net.IP, port int) (int, error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	addr := &unix.SockaddrInet4{Port: port}
	copy(addr.Addr[:], host)
	if err := unix.Bind(fd, addr); err != nil {
		return -1, fmt.Errorf("bind %s:%d: %w", host, port, err)
	}
	if err := unix.Listen(fd, 65535); err != nil {
		return -1, fmt.Errorf("listen %s:%d: %w", host, port, err)
	}
	return fd, nil
}

// handoff gives the loop a Postgres connection that a slot goroutine just opened. It is the only method called from
// another goroutine.
func (l *loop) handoff(fd int, s *dbSlot) {
	l.mu.Lock()
	l.incoming = append(l.incoming, handoff{fd, s})
	l.mu.Unlock()
	one := [8]byte{1} // eventfd counter += 1 (little-endian on amd64 and arm64)
	unix.Write(l.efd, one[:])
}

func (l *loop) takeHandoffs() {
	var buf [8]byte
	unix.Read(l.efd, buf[:])
	l.mu.Lock()
	in := l.incoming
	l.incoming = nil
	l.mu.Unlock()
	for _, h := range in {
		if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, h.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(h.fd)}); err != nil {
			unix.Close(h.fd)
			h.slot.lost <- struct{}{}
			continue
		}
		d := &dbConn{fd: h.fd, slot: h.slot, failed: -1}
		l.dbs[h.fd] = d
		l.dbList[h.slot.role] = append(l.dbList[h.slot.role], d)
	}
}

func (l *loop) run() {
	runtime.LockOSThread()
	events := make([]unix.EpollEvent, 1024)
	for {
		// Wake at least every second to time out ops waiting for a connection and connections that stopped answering.
		n, err := unix.EpollWait(l.epfd, events, 1000)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			log.Fatalf("epoll_wait: %v", err)
		}
		l.pass(events[:n])
		// Batching window: a pass that found work starts at most every batchWindow, so requests and Postgres answers
		// pile up and share the next pass's syscalls and Syncs. A pass that took the whole window, or didn't get every
		// ready event, doesn't wait.
		if batchWindow > 0 && n > 0 && n < len(events) {
			if rest := batchWindow - time.Since(l.now); rest > 0 {
				ts := unix.NsecToTimespec(int64(rest))
				unix.Nanosleep(&ts, nil)
			}
		}
	}
}

// pass handles one batch of events, then sends the Postgres requests they produced. A panic is logged and the loop
// keeps running: a crashed server is not restarted.
func (l *loop) pass(events []unix.EpollEvent) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("loop %d: panic: %v", l.id, p)
		}
	}()
	l.now = time.Now()
	for _, ev := range events {
		fd := int(ev.Fd)
		switch {
		case l.listeners[fd]:
			l.accept(fd)
		case fd == l.efd:
			l.takeHandoffs()
		case l.dbs[fd] != nil:
			d := l.dbs[fd]
			if ev.Events&(unix.EPOLLIN|unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
				l.readDB(d)
			}
			if ev.Events&unix.EPOLLOUT != 0 && !d.closed {
				l.flushDB(d)
			}
		default:
			c := l.conns[fd]
			if c == nil {
				continue
			}
			switch {
			case ev.Events&unix.EPOLLIN != 0:
				l.read(c) // also sees EOF and errors through read()
			case ev.Events&unix.EPOLLOUT != 0:
				l.flush(c)
			case ev.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0:
				l.closeConn(c)
			}
		}
	}
	if l.now.Sub(l.checked) >= time.Second {
		l.checked = l.now
		l.checkSilent()
	}
	l.dispatch()
}

func (l *loop) accept(lfd int) {
	for {
		fd, _, err := unix.Accept4(lfd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		if err != nil {
			if err != unix.EAGAIN && err != unix.ECONNABORTED {
				log.Printf("accept4: %v", err)
			}
			return
		}
		unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
		if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(fd)}); err != nil {
			unix.Close(fd)
			continue
		}
		l.conns[fd] = &conn{fd: fd}
		l.nconns.Add(1)
	}
}

func (l *loop) read(c *conn) {
	n, err := unix.Read(c.fd, l.rbuf)
	if err == unix.EAGAIN {
		return
	}
	if n <= 0 || err != nil {
		l.closeConn(c)
		return
	}
	if c.in == nil {
		c.in = l.getBuf()
	}
	c.in = append(c.in, l.rbuf[:n]...)
	if !c.close {
		l.process(c)
	}
	l.flush(c)
}

// process answers the complete requests in c.in, in order (pipelining), until one has to wait for Postgres.
func (l *loop) process(c *conn) {
	if c.out == nil {
		c.out = l.getBuf() // flush gives it back if nothing was written to it
	}
	off := 0
	var r request
	for off < len(c.in) && !c.waiting {
		used := parseRequest(c.in[off:], &r)
		if used == 0 {
			if r.needContinue && !c.continued {
				c.out = append(c.out, "HTTP/1.1 100 Continue\r\n\r\n"...)
				c.continued = true
			}
			break
		}
		c.continued = false
		if used < 0 {
			c.out = appendJSON(c.out, 400, `{"error":"bad request"}`, false)
			c.close = true
			off = len(c.in)
			break
		}
		out, o := l.app.handle(&r, c.out)
		c.out = out
		off += used
		if o != nil {
			o.c, o.keepAlive = c, r.keepAlive
			c.waiting = true
			l.enqueue(o)
		}
		if !r.keepAlive {
			c.close = true
			off = len(c.in)
			break
		}
	}
	c.in = c.in[:copy(c.in, c.in[off:])]
	if len(c.in) == 0 {
		l.putBuf(c.in)
		c.in = nil
	}
}

func (l *loop) flush(c *conn) {
	if c.closed {
		return
	}
	for len(c.out) > 0 {
		n, err := unix.Write(c.fd, c.out)
		if err == unix.EAGAIN {
			if !c.outArmed {
				c.outArmed = true
				unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, c.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP, Fd: int32(c.fd)})
			}
			return
		}
		if err != nil {
			l.closeConn(c)
			return
		}
		c.out = c.out[:copy(c.out, c.out[n:])]
	}
	if c.out != nil {
		l.putBuf(c.out)
		c.out = nil
	}
	if c.close && !c.waiting {
		l.closeConn(c)
		return
	}
	if c.outArmed {
		c.outArmed = false
		unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, c.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(c.fd)})
	}
}

// closeConn closes a client connection. An op still waiting for Postgres keeps the conn and sees closed when its
// answer arrives; the fd number may by then belong to a new connection.
func (l *loop) closeConn(c *conn) {
	if c.closed {
		return
	}
	c.closed = true
	l.putBuf(c.in)
	l.putBuf(c.out)
	c.in, c.out = nil, nil
	delete(l.conns, c.fd)
	l.nconns.Add(-1)
	unix.Close(c.fd) // also removes fd from the epoll set
}
