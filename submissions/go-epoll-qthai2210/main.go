// Feed API server for the Million Users Challenge: own epoll loop, hand-written HTTP/1.1, Postgres via pgconn.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type config struct {
	host        net.IP
	ports       []int
	databaseURL string
	jwtSecret   []byte
}

func loadConfig() config {
	c := config{
		databaseURL: os.Getenv("DATABASE_URL"),
		jwtSecret:   []byte(os.Getenv("JWT_SECRET")),
	}
	if c.databaseURL == "" || len(c.jwtSecret) == 0 {
		log.Fatal("DATABASE_URL and JWT_SECRET are required")
	}
	host := os.Getenv("HOST")
	if host == "" {
		host = "0.0.0.0"
	}
	if c.host = net.ParseIP(host).To4(); c.host == nil {
		log.Fatalf("HOST %q is not an IPv4 address", host)
	}
	for _, name := range []string{"PORT", "EXTRA_PORT"} {
		v := os.Getenv(name)
		if v == "" {
			continue
		}
		p, err := strconv.Atoi(v)
		if err != nil || p <= 0 || p > 65535 {
			log.Fatalf("%s %q is not a port", name, v)
		}
		c.ports = append(c.ports, p)
	}
	if len(c.ports) == 0 {
		log.Fatal("PORT is required")
	}
	return c
}

// app holds what handlers share. Only connections and process start time live here: nothing derived from requests or
// data (rule 7).
type app struct {
	cfg     config
	started time.Time
	db      *pgconn.PgConn // P1: one synchronous connection, used by /health only
}

func (a *app) handle(r *request, out []byte) []byte {
	if string(r.method) == "GET" && string(r.path) == "/health" {
		return a.health(r, out)
	}
	return appendJSON(out, 404, `{"error":"not found"}`, r.keepAlive)
}

func (a *app) health(r *request, out []byte) []byte {
	if err := a.ping(); err != nil {
		msg, _ := json.Marshal(err.Error())
		return appendJSON(out, 503, `{"status":"degraded","db":"unreachable","error":`+string(msg)+`}`, r.keepAlive)
	}
	uptime := strconv.FormatInt(int64(time.Since(a.started)/time.Second), 10)
	return appendJSON(out, 200, `{"status":"ok","db":"ok","uptime_s":`+uptime+`}`, r.keepAlive)
}

func (a *app) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if a.db == nil || a.db.IsClosed() {
		db, err := pgconn.Connect(ctx, a.cfg.databaseURL)
		if err != nil {
			return err
		}
		a.db = db
	}
	if _, err := a.db.Exec(ctx, "SELECT 1").ReadAll(); err != nil {
		a.db.Close(context.Background())
		a.db = nil
		return err
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	a := &app{cfg: loadConfig(), started: time.Now()}
	l, err := newLoop(a.cfg.host, a.cfg.ports, a.handle)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s ports %v", a.cfg.host, a.cfg.ports)
	l.run()
}
