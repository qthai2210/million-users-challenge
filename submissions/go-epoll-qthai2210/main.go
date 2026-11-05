// Feed API server for the Million Users Challenge: own epoll loop, hand-written HTTP/1.1, Postgres via pgconn.
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
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
	tuneGC()
	if v := os.Getenv("BATCH_WINDOW_US"); v != "" { // tuning knob for measurements; the default is what was measured best
		us, err := strconv.Atoi(v)
		if err != nil || us < 0 {
			log.Fatalf("BATCH_WINDOW_US %q is not a number of microseconds", v)
		}
		batchWindow = time.Duration(us) * time.Microsecond
	}
	dbCfg, err := parseDBConfig(cfg.databaseURL)
	if err != nil {
		log.Fatalf("DATABASE_URL: %v", err)
	}
	started := time.Now()
	loops := make([]*loop, loopCount())
	for i := range loops {
		// Everything a handler touches is per loop (scratch buffer, HMAC, Postgres connections), so loops share nothing.
		a := &app{started: started, jwt: newJWTVerifier(cfg.jwtSecret)}
		l, err := newLoop(i, cfg.host, cfg.ports, a)
		if err != nil {
			log.Fatal(err)
		}
		loops[i] = l
		for role, total := range [...]int{roleRead: dbReadConns, roleWrite: dbWriteConns} {
			for range share(total, len(loops), i) {
				go (&dbSlot{loop: l, role: dbRole(role), lost: make(chan struct{}, 1)}).keep(dbCfg)
			}
		}
	}
	log.Printf("listening on %s ports %v with %d loops", cfg.host, cfg.ports, len(loops))
	go logStats(loops)
	for _, l := range loops[1:] {
		go l.run()
	}
	loops[0].run()
}

// tuneGC sets the collector in the process (rule 13 allows it). The live heap is small (connection buffers go back
// to the loops' pools), so the GC may let it grow 5x before collecting; the soft limit at 75% of RAM keeps that from
// ever crowding out the kernel's socket memory.
func tuneGC() {
	debug.SetGCPercent(400)
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		var kb int64
		if _, err := fmt.Sscanf(string(b), "MemTotal: %d kB", &kb); err == nil && kb > 0 {
			debug.SetMemoryLimit(kb * 1024 * 3 / 4)
		}
	}
}

// batchWindow is the shortest time between the starts of two loop passes that found work.
var batchWindow = time.Millisecond

// Postgres connections in total, whatever the number of loops: the database box has 16 cores either way. Reads and
// writes are apart so a read never waits behind a commit. 80 of the 512 allowed (rule 5).
const (
	dbReadConns  = 64
	dbWriteConns = 16
)

// share is loop i's part of total connections spread over n loops, at least 1.
func share(total, n, i int) int {
	k := total / n
	if i < total%n {
		k++
	}
	return max(k, 1)
}

// logStats prints, every 10 s when something changed, the open client connections per loop and the Postgres
// answers per second and per Sync.
func logStats(loops []*loop) {
	counts := make([]int64, len(loops))
	var lastAnswered, lastSyncs int64
	last := ""
	for range time.Tick(10 * time.Second) {
		var answered, syncs int64
		for i, l := range loops {
			counts[i] = l.nconns.Load()
			answered += l.answered.Load()
			syncs += l.syncs.Load()
		}
		line := fmt.Sprintf("connections per loop: %v", counts)
		if da, ds := answered-lastAnswered, syncs-lastSyncs; ds > 0 {
			line += fmt.Sprintf(", postgres: %d answers/s, %.1f per Sync", da/10, float64(da)/float64(ds))
		}
		if line != last {
			log.Print(line)
			last = line
		}
		lastAnswered, lastSyncs = answered, syncs
	}
}
