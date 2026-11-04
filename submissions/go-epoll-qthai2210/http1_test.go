package main

import (
	"strings"
	"testing"
)

func TestParseRequest(t *testing.T) {
	var r request
	in := "POST /posts?x=1 HTTP/1.1\r\nhost: a\r\nauthorization: Bearer t\r\ncontent-length: 13\r\n\r\n{\"body\":\"x\"}"
	if n := parseRequest([]byte(in), &r); n != 0 { // 12 of 13 body bytes
		t.Fatalf("incomplete body: want 0, got %d", n)
	}
	in += "!"
	n := parseRequest([]byte(in), &r)
	if n != len(in) || string(r.method) != "POST" || string(r.path) != "/posts" || string(r.auth) != "Bearer t" ||
		string(r.body) != "{\"body\":\"x\"}!" || !r.keepAlive {
		t.Fatalf("got n=%d %+v", n, r)
	}
}

func TestParseRequestEverySplit(t *testing.T) {
	req := "GET /feed HTTP/1.1\r\nHost: a\r\n\r\n"
	var r request
	for i := 0; i < len(req); i++ {
		if n := parseRequest([]byte(req[:i]), &r); n != 0 {
			t.Fatalf("prefix of %d bytes: got %d, want 0 (incomplete)", i, n)
		}
	}
	if n := parseRequest([]byte(req), &r); n != len(req) {
		t.Fatalf("full request: got %d", n)
	}
}

func TestParseRequestPipelined(t *testing.T) {
	in := []byte("GET /health HTTP/1.1\r\n\r\nGET /feed HTTP/1.1\r\nConnection: close\r\n\r\nGET /x HT")
	var r request
	var paths []string
	off := 0
	for {
		n := parseRequest(in[off:], &r)
		if n <= 0 {
			break
		}
		paths = append(paths, string(r.path)+map[bool]string{true: "+ka", false: "+close"}[r.keepAlive])
		off += n
	}
	if got := strings.Join(paths, " "); got != "/health+ka /feed+close" {
		t.Fatalf("got %q", got)
	}
}

func TestParseRequestVersionsAndErrors(t *testing.T) {
	var r request
	if parseRequest([]byte("GET / HTTP/1.0\r\n\r\n"), &r); r.keepAlive {
		t.Error("HTTP/1.0 defaults to close")
	}
	if parseRequest([]byte("GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n"), &r); !r.keepAlive {
		t.Error("HTTP/1.0 with keep-alive")
	}
	for _, bad := range []string{
		"GET\r\n\r\n",
		"GET / SPDY/3\r\n\r\n",
		"GET / HTTP/1.1\r\nno-colon\r\n\r\n",
		"POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
		"POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n",
		"GET / HTTP/1.1\r\nX: " + strings.Repeat("a", maxHead),
	} {
		if n := parseRequest([]byte(bad), &r); n != -1 {
			t.Errorf("%.40q: got %d, want -1", bad, n)
		}
	}
}

func TestParseRequestExpectContinue(t *testing.T) {
	var r request
	head := "POST /posts HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n"
	if n := parseRequest([]byte(head), &r); n != 0 || !r.needContinue {
		t.Fatalf("head only: n=%d needContinue=%v", n, r.needContinue)
	}
	if n := parseRequest([]byte(head+"hello"), &r); n != len(head)+5 || r.needContinue {
		t.Fatalf("with body: n=%d needContinue=%v", n, r.needContinue)
	}
}
