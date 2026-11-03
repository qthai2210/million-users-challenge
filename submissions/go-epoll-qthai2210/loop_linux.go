package main

import (
	"fmt"
	"log"
	"net"
	"runtime"

	"golang.org/x/sys/unix"
)

// conn is the per-connection state. P1 keeps it in a map; P4 moves it to an off-heap slot table indexed by fd.
type conn struct {
	in       []byte // received bytes not yet parsed
	out      []byte // response bytes not yet written
	close    bool   // close once out is flushed
	outArmed bool   // EPOLLOUT is registered (out did not fit in the socket buffer)
}

// loop is one epoll event loop on one locked OS thread. P1 runs a single loop; P3 runs one per core.
type loop struct {
	epfd      int
	listeners map[int]bool
	conns     map[int]*conn
	handle    func(*request, []byte) []byte
	rbuf      []byte
}

func newLoop(host net.IP, ports []int, handle func(*request, []byte) []byte) (*loop, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	l := &loop{epfd: epfd, listeners: map[int]bool{}, conns: map[int]*conn{}, handle: handle, rbuf: make([]byte, 64<<10)}
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

func (l *loop) run() {
	runtime.LockOSThread()
	events := make([]unix.EpollEvent, 1024)
	for {
		n, err := unix.EpollWait(l.epfd, events, -1)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			log.Fatalf("epoll_wait: %v", err)
		}
		for _, ev := range events[:n] {
			fd := int(ev.Fd)
			switch {
			case l.listeners[fd]:
				l.accept(fd)
			case ev.Events&unix.EPOLLIN != 0:
				l.read(fd) // also sees EOF and errors through read()
			case ev.Events&unix.EPOLLOUT != 0:
				l.flush(fd)
			case ev.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0:
				l.closeConn(fd)
			}
		}
	}
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
		l.conns[fd] = &conn{}
	}
}

func (l *loop) read(fd int) {
	c := l.conns[fd]
	if c == nil {
		return
	}
	n, err := unix.Read(fd, l.rbuf)
	if err == unix.EAGAIN {
		return
	}
	if n <= 0 || err != nil {
		l.closeConn(fd)
		return
	}
	c.in = append(c.in, l.rbuf[:n]...)
	if !c.close {
		l.process(c)
	}
	l.flush(fd)
}

// process answers every complete request in c.in, in order (pipelining).
func (l *loop) process(c *conn) {
	off := 0
	var r request
	for off < len(c.in) {
		used := parseRequest(c.in[off:], &r)
		if used == 0 {
			break
		}
		if used < 0 {
			c.out = appendJSON(c.out, 400, `{"error":"bad request"}`, false)
			c.close = true
			off = len(c.in)
			break
		}
		c.out = l.handle(&r, c.out)
		off += used
		if !r.keepAlive {
			c.close = true
			off = len(c.in)
			break
		}
	}
	c.in = c.in[:copy(c.in, c.in[off:])]
}

func (l *loop) flush(fd int) {
	c := l.conns[fd]
	if c == nil {
		return
	}
	for len(c.out) > 0 {
		n, err := unix.Write(fd, c.out)
		if err == unix.EAGAIN {
			if !c.outArmed {
				c.outArmed = true
				unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP, Fd: int32(fd)})
			}
			return
		}
		if err != nil {
			l.closeConn(fd)
			return
		}
		c.out = c.out[:copy(c.out, c.out[n:])]
	}
	if c.close {
		l.closeConn(fd)
		return
	}
	if c.outArmed {
		c.outArmed = false
		unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(fd)})
	}
}

func (l *loop) closeConn(fd int) {
	if _, ok := l.conns[fd]; !ok {
		return
	}
	delete(l.conns, fd)
	unix.Close(fd) // also removes fd from the epoll set
}
