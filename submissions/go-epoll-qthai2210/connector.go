package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sys/unix"
)

type dbRole int

const (
	roleRead dbRole = iota
	roleWrite
)

func (r dbRole) String() string {
	if r == roleWrite {
		return "write"
	}
	return "read"
}

// parseDBConfig reads DATABASE_URL once. TLS is turned off: the loops speak the protocol on the raw socket, and the
// database box runs with ssl = off anyway.
func parseDBConfig(url string) (*pgconn.Config, error) {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.TLSConfig = nil
	cfg.Fallbacks = nil
	return cfg, nil
}

// dialDB logs in with pgconn (SCRAM and the rest of the startup), prepares the statements, then takes the socket
// away from pgconn: it returns a duplicate of its file descriptor, non-blocking, for an epoll loop to own.
func dialDB(cfg *pgconn.Config) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pc, err := pgconn.ConnectConfig(ctx, cfg.Copy())
	if err != nil {
		return -1, err
	}
	for _, s := range statements {
		if _, err := pc.Prepare(ctx, s.name, s.sql, nil); err != nil {
			pc.Close(context.Background())
			return -1, fmt.Errorf("prepare %s: %w", s.name, err)
		}
	}
	if err := pc.SyncConn(ctx); err != nil {
		pc.Close(context.Background())
		return -1, err
	}
	hc, err := pc.Hijack()
	if err != nil {
		pc.Close(context.Background())
		return -1, err
	}
	tcp, ok := hc.Conn.(*net.TCPConn)
	if !ok {
		hc.Conn.Close()
		return -1, fmt.Errorf("postgres connection is %T, not plain TCP", hc.Conn)
	}
	defer tcp.Close() // closes Go's descriptor; the duplicate keeps the socket open
	raw, err := tcp.SyscallConn()
	if err != nil {
		return -1, err
	}
	fd, dupErr := -1, error(nil)
	if err := raw.Control(func(s uintptr) { fd, dupErr = unix.Dup(int(s)) }); err != nil {
		return -1, err
	}
	if dupErr != nil {
		return -1, dupErr
	}
	unix.CloseOnExec(fd)
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// dbSlot is one Postgres connection a loop should have. Its goroutine opens the connection, hands it to the loop,
// waits until the loop reports it lost, and opens a new one, retrying every second while Postgres is unreachable.
type dbSlot struct {
	loop *loop
	role dbRole
	lost chan struct{}
}

func (s *dbSlot) keep(cfg *pgconn.Config) {
	for {
		fd, err := dialDB(cfg)
		if err != nil {
			log.Printf("loop %d: %s connection: %v (retrying in 1 s)", s.loop.id, s.role, err)
			time.Sleep(time.Second)
			continue
		}
		s.loop.handoff(fd, s)
		<-s.lost
	}
}
