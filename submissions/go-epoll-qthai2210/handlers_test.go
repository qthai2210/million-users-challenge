package main

import (
	"strings"
	"testing"
)

// Routing, auth, id and body validation happen before Postgres: those requests are answered at once, and the rest
// become an op for the loop to send.
func TestRouteWithoutDatabase(t *testing.T) {
	a := &app{jwt: newJWTVerifier([]byte(testSecret))}
	token := "Bearer " + signJWT(`{"alg":"HS256"}`, `{"sub":"1","username":"golden_ember_1","exp":99999999999}`)
	tests := []struct {
		method, path, auth, body string
		want                     string
	}{
		{"GET", "/nope", "", "", `404 {"error":"not found"}`},
		{"GET", "/feed/", "", "", `404 {"error":"not found"}`},
		{"GET", "/posts/1/like", "", "", `404 {"error":"not found"}`},
		{"POST", "/posts/1/likes", token, "", `404 {"error":"not found"}`},
		{"GET", "/posts/0", "", "", `400 {"error":"invalid post id"}`},
		{"GET", "/posts/-1", "", "", `400 {"error":"invalid post id"}`},
		{"GET", "/posts/abc", "", "", `400 {"error":"invalid post id"}`},
		{"GET", "/posts/1.5", "", "", `400 {"error":"invalid post id"}`},
		{"POST", "/posts", "", `{"body":"x"}`, `401 {"error":"missing bearer token"}`},
		{"POST", "/posts", "Bearer abc", `{"body":"x"}`, `401 {"error":"invalid or expired token"}`},
		{"POST", "/posts/abc/like", "", "", `401 {"error":"missing bearer token"}`}, // auth before id
		{"POST", "/posts/abc/like", token, "", `400 {"error":"invalid post id"}`},
		{"POST", "/posts", token, `{"body":"   "}`, `400 {"error":"body is required"}`},
		{"POST", "/posts", token, `{bad`, `400 {"error":"malformed JSON body"}`},
		{"GET", "/feed", "", "", `op feed`},
		{"GET", "/health", "", "", `op health`},
		{"GET", "/posts/41", "", "", `op post_by_id`},
		{"POST", "/posts", token, `{"body":"  hi "}`, `op insert_post`},
		{"POST", "/posts/41/like", token, "", `op insert_like`},
	}
	for _, tc := range tests {
		r := &request{method: []byte(tc.method), path: []byte(tc.path), auth: []byte(tc.auth), body: []byte(tc.body), keepAlive: true}
		out, o := a.handle(r, nil)
		got := "op " + func() string {
			if o != nil {
				return o.stmt
			}
			return ""
		}()
		if o == nil {
			res := string(out)
			_, body, _ := strings.Cut(res, "\r\n\r\n")
			got = res[len("HTTP/1.1 "):len("HTTP/1.1 ")+3] + " " + body
		}
		if got != tc.want {
			t.Errorf("%s %s: got %s, want %s", tc.method, tc.path, got, tc.want)
		}
	}
}
