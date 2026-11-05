package main

import (
	"testing"
	"time"
)

// The request path of each endpoint without the network: route and validate, the rows Postgres would send, then
// the response. Run with -benchmem to see what a request allocates.

func benchRow(id int64) [][]byte {
	return [][]byte{int8Param(id), []byte("Steal disk fast big reading an watching but rarely after that maybe..."),
		int8Param(789007368825000), []byte("golden_comet_615"), {0, 0, 0, 5}}
}

func benchEndpoint(b *testing.B, method, path, auth, body string, rows int, affected int64) {
	a := &app{started: time.Now(), jwt: newJWTVerifier([]byte(testSecret))}
	r := &request{method: []byte(method), path: []byte(path), auth: []byte(auth), body: []byte(body), keepAlive: true}
	row := benchRow(41)
	out := make([]byte, 0, 16<<10)
	b.ReportAllocs()
	for b.Loop() {
		res, o := a.handle(r, out[:0])
		if o == nil {
			b.Fatalf("no op: %s", res)
		}
		for i := 0; i < rows; i++ {
			a.row(o, row)
		}
		o.affected = affected
		out = a.finish(o, res)
		a.freeOp(o)
	}
}

var benchToken = "Bearer " + signJWT(`{"alg":"HS256","typ":"JWT"}`, `{"sub":"1","username":"golden_ember_1","iat":1793718651,"exp":2109078651}`)

func BenchmarkFeed(b *testing.B)   { benchEndpoint(b, "GET", "/feed", "", "", 20, 20) }
func BenchmarkPost(b *testing.B)   { benchEndpoint(b, "GET", "/posts/41", "", "", 1, 1) }
func BenchmarkHealth(b *testing.B) { benchEndpoint(b, "GET", "/health", "", "", 1, 1) }
func BenchmarkLike(b *testing.B)   { benchEndpoint(b, "POST", "/posts/41/like", benchToken, "", 0, 1) }
func BenchmarkCreate(b *testing.B) {
	a := &app{started: time.Now(), jwt: newJWTVerifier([]byte(testSecret))}
	r := &request{method: []byte("POST"), path: []byte("/posts"), auth: []byte(benchToken), body: []byte(`{"body":"  hello world "}`), keepAlive: true}
	out := make([]byte, 0, 16<<10)
	b.ReportAllocs()
	for b.Loop() {
		res, o := a.handle(r, out[:0])
		a.row(o, [][]byte{int8Param(500001), int8Param(789007368825000)})
		out = a.finish(o, res)
		a.freeOp(o)
	}
}
