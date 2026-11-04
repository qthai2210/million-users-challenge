package main

import (
	"strings"
	"testing"
)

// Requests that are answered before any database access (routing, auth, id and body validation). The db field is
// nil, so reaching Postgres would panic and come back as a 500.
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
		{"GET", "/feed", "", "", `500 {"error":"internal server error"}`}, // nil db: panic recovered
	}
	for _, tc := range tests {
		r := &request{method: []byte(tc.method), path: []byte(tc.path), auth: []byte(tc.auth), body: []byte(tc.body), keepAlive: true}
		res := string(a.handle(r, nil))
		status := res[len("HTTP/1.1 ") : len("HTTP/1.1 ")+3]
		_, body, _ := strings.Cut(res, "\r\n\r\n")
		if got := status + " " + body; got != tc.want {
			t.Errorf("%s %s: got %s, want %s", tc.method, tc.path, got, tc.want)
		}
	}
}
