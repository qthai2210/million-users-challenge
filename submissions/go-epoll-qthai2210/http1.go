package main

import (
	"bytes"
	"strconv"
)

// maxHead caps the request line plus headers; a request still incomplete past this is malformed.
const maxHead = 16 << 10

// request points into the connection's input buffer and is only valid until the next read.
type request struct {
	method    []byte
	path      []byte // without the query string
	body      []byte
	keepAlive bool
}

// parseRequest parses one request from the start of b. It returns the bytes consumed, 0 if b holds no complete
// request yet, or -1 if the request is malformed.
func parseRequest(b []byte, r *request) int {
	end := bytes.Index(b, []byte("\r\n\r\n"))
	if end < 0 {
		if len(b) > maxHead {
			return -1
		}
		return 0
	}
	head := b[:end]
	line, rest, _ := bytes.Cut(head, []byte("\r\n"))
	method, line, ok1 := bytes.Cut(line, []byte(" "))
	target, proto, ok2 := bytes.Cut(line, []byte(" "))
	if !ok1 || !ok2 || len(method) == 0 || len(target) == 0 || !bytes.HasPrefix(proto, []byte("HTTP/1.")) {
		return -1
	}
	if i := bytes.IndexByte(target, '?'); i >= 0 {
		target = target[:i]
	}
	r.method, r.path = method, target
	r.keepAlive = string(proto) != "HTTP/1.0"

	contentLength := 0
	for len(rest) > 0 {
		var h []byte
		h, rest, _ = bytes.Cut(rest, []byte("\r\n"))
		name, value, ok := bytes.Cut(h, []byte(":"))
		if !ok {
			return -1
		}
		value = bytes.TrimSpace(value)
		switch {
		case bytes.EqualFold(name, []byte("Content-Length")):
			n, err := strconv.Atoi(string(value))
			if err != nil || n < 0 {
				return -1
			}
			contentLength = n
		case bytes.EqualFold(name, []byte("Connection")):
			if bytes.EqualFold(value, []byte("close")) {
				r.keepAlive = false
			} else if bytes.EqualFold(value, []byte("keep-alive")) {
				r.keepAlive = true
			}
		}
	}
	total := end + 4 + contentLength
	if len(b) < total {
		return 0
	}
	r.body = b[end+4 : total]
	return total
}

var statusText = map[int]string{
	200: "OK", 201: "Created", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
	500: "Internal Server Error", 503: "Service Unavailable",
}

// appendJSON appends a complete response with a JSON body to out.
func appendJSON(out []byte, status int, body string, keepAlive bool) []byte {
	out = append(out, "HTTP/1.1 "...)
	out = strconv.AppendInt(out, int64(status), 10)
	out = append(out, ' ')
	out = append(out, statusText[status]...)
	out = append(out, "\r\nContent-Type: application/json\r\nContent-Length: "...)
	out = strconv.AppendInt(out, int64(len(body)), 10)
	if !keepAlive {
		out = append(out, "\r\nConnection: close"...)
	}
	out = append(out, "\r\n\r\n"...)
	return append(out, body...)
}
