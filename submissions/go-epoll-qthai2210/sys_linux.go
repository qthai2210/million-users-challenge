package main

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// rawRead, rawWrite and rawSleep call read(2), write(2) and nanosleep(2) without telling the Go scheduler. unix.Read
// and unix.Write go through entersyscall/exitsyscall, and a call that takes a little long lets the scheduler hand the
// loop's P to another thread and wake one up later. Every descriptor the loops read and write is non-blocking, and the
// sleep is at most the batching window, so these calls are always short: the scheduler doesn't need to know. A stop
// of the world (for the GC) waits at most that long for them. epoll_wait, which does block, keeps going through
// unix.EpollWait.

func rawRead(fd int, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, _, e := unix.RawSyscall(unix.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	if e != 0 {
		return 0, e
	}
	return int(n), nil
}

func rawWrite(fd int, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, _, e := unix.RawSyscall(unix.SYS_WRITE, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	if e != 0 {
		return 0, e
	}
	return int(n), nil
}

func rawSleep(ts *unix.Timespec) {
	unix.RawSyscall(unix.SYS_NANOSLEEP, uintptr(unsafe.Pointer(ts)), 0, 0)
}
