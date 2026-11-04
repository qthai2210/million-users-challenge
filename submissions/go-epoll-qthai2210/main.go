// Feed API server for the Million Users Challenge: own epoll loop, hand-written HTTP/1.1, Postgres via pgconn.
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"
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

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := loadConfig()
	started := time.Now()
	loops := make([]*loop, loopCount())
	for i := range loops {
		// Everything a handler touches is per loop (scratch buffer, HMAC, database connection), so loops share nothing.
		a := &app{started: started, db: &db{url: cfg.databaseURL}, jwt: newJWTVerifier(cfg.jwtSecret)}
		l, err := newLoop(i, cfg.host, cfg.ports, a.handle)
		if err != nil {
			log.Fatal(err)
		}
		loops[i] = l
	}
	log.Printf("listening on %s ports %v with %d loops", cfg.host, cfg.ports, len(loops))
	go logStats(loops)
	for _, l := range loops[1:] {
		go l.run()
	}
	loops[0].run()
}

// logStats prints the open connections per loop every 10 s, when they changed.
func logStats(loops []*loop) {
	counts := make([]int64, len(loops))
	last := ""
	for range time.Tick(10 * time.Second) {
		for i, l := range loops {
			counts[i] = l.nconns.Load()
		}
		if line := fmt.Sprint(counts); line != last {
			log.Printf("connections per loop: %s", line)
			last = line
		}
	}
}
