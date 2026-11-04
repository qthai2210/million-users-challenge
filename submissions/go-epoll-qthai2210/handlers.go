package main

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"
)

// app holds what handlers share. Only connections, prepared statements, the HMAC key and the process start time
// live here: nothing derived from requests or data (rule 7).
type app struct {
	started time.Time
	db      *db
	jwt     *jwtVerifier
	body    []byte // scratch buffer for the response body, reused by every request of the loop
}

// handle answers one request by appending the full response to out. A panic answers 500 instead of killing the
// process (a crashed server is not restarted).
func (a *app) handle(r *request, out []byte) (res []byte) {
	n := len(out)
	defer func() {
		if p := recover(); p != nil {
			log.Printf("panic serving %s %s: %v", r.method, r.path, p)
			res = appendError(out[:n], 500, "internal server error", r.keepAlive)
		}
	}()
	return a.route(r, out)
}

func (a *app) route(r *request, out []byte) []byte {
	path, method := string(r.path), string(r.method)
	switch {
	case path == "/health" && method == "GET":
		return a.health(r, out)
	case path == "/feed" && method == "GET":
		return a.feed(r, out)
	case path == "/posts" && method == "POST":
		return a.createPost(r, out)
	case strings.HasPrefix(path, "/posts/"):
		rest := path[len("/posts/"):]
		if id, tail, ok := strings.Cut(rest, "/"); ok {
			if tail == "like" && method == "POST" {
				return a.likePost(r, id, out)
			}
		} else if method == "GET" {
			return a.getPost(r, rest, out)
		}
	}
	return appendError(out, 404, "not found", r.keepAlive)
}

// authenticate checks the token; on failure it also returns the 401 response.
func (a *app) authenticate(r *request, out []byte) (user, []byte, bool) {
	u, res := a.jwt.verify(r.auth, time.Now().Unix())
	if res != authOK {
		return user{}, appendError(out, 401, authMessage[res], r.keepAlive), false
	}
	return u, out, true
}

func internalError(r *request, out []byte, err error) []byte {
	log.Printf("%s %s: %v", r.method, r.path, err)
	return appendError(out, 500, "internal server error", r.keepAlive)
}

func (a *app) health(r *request, out []byte) []byte {
	if err := a.db.ping(); err != nil {
		msg, _ := json.Marshal(err.Error())
		return appendJSON(out, 503, `{"status":"degraded","db":"unreachable","error":`+string(msg)+`}`, r.keepAlive)
	}
	b := append(a.body[:0], `{"status":"ok","db":"ok","uptime_s":`...)
	b = strconv.AppendInt(b, int64(time.Since(a.started)/time.Second), 10)
	a.body = append(b, '}')
	return appendJSON(out, 200, a.body, r.keepAlive)
}

// appendPostRow appends a post object from a binary row: id int8, body text, created_at timestamptz,
// username text, like_count int4.
func appendPostRow(dst []byte, v [][]byte) []byte {
	return appendPost(dst, readInt8(v[0]), v[1], readInt8(v[2]), v[3], readInt4(v[4]))
}

func (a *app) feed(r *request, out []byte) []byte {
	b := append(a.body[:0], `{"posts":[`...)
	first := true
	_, err := a.db.exec(stmtFeed, nil, nil, func(v [][]byte) {
		if !first {
			b = append(b, ',')
		}
		first = false
		b = appendPostRow(b, v)
	})
	if err != nil {
		return internalError(r, out, err)
	}
	a.body = append(b, ']', '}')
	return appendJSON(out, 200, a.body, r.keepAlive)
}

func (a *app) getPost(r *request, rawID string, out []byte) []byte {
	id, ok := parsePositiveInt(rawID)
	if !ok {
		return appendError(out, 400, "invalid post id", r.keepAlive)
	}
	b := append(a.body[:0], `{"post":`...)
	found := false
	_, err := a.db.exec(stmtPostByID, [][]byte{int8Param(id)}, allBinary, func(v [][]byte) {
		found = true
		b = appendPostRow(b, v)
	})
	if err != nil {
		return internalError(r, out, err)
	}
	if !found {
		return appendError(out, 404, "post not found", r.keepAlive)
	}
	a.body = append(b, '}')
	return appendJSON(out, 200, a.body, r.keepAlive)
}

func (a *app) createPost(r *request, out []byte) []byte {
	u, out, ok := a.authenticate(r, out)
	if !ok {
		return out
	}
	text, msg := parsePostBody(r.body)
	if msg != "" {
		return appendError(out, 400, msg, r.keepAlive)
	}
	var id, created int64
	_, err := a.db.exec(stmtInsertPost, [][]byte{int8Param(u.id), []byte(text)}, idAndText, func(v [][]byte) {
		id, created = readInt8(v[0]), readInt8(v[1])
	})
	if err != nil {
		return internalError(r, out, err)
	}
	b := append(a.body[:0], `{"post":`...)
	b = appendPost(b, id, text, created, u.username, 0)
	a.body = append(b, '}')
	return appendJSON(out, 201, a.body, r.keepAlive)
}

func (a *app) likePost(r *request, rawID string, out []byte) []byte {
	u, out, ok := a.authenticate(r, out)
	if !ok {
		return out
	}
	postID, ok := parsePositiveInt(rawID)
	if !ok {
		return appendError(out, 400, "invalid post id", r.keepAlive)
	}
	tag, err := a.db.exec(stmtInsertLike, [][]byte{int8Param(u.id), int8Param(postID)}, allBinary, nil)
	if isForeignKeyViolation(err) {
		return appendError(out, 404, "post not found", r.keepAlive)
	}
	if err != nil {
		return internalError(r, out, err)
	}
	status, already := 201, "false"
	if tag.RowsAffected() == 0 {
		status, already = 200, "true"
	}
	b := append(a.body[:0], `{"liked":true,"already_liked":`...)
	b = append(b, already...)
	b = append(b, `,"post_id":`...)
	b = strconv.AppendInt(b, postID, 10)
	a.body = append(b, '}')
	return appendJSON(out, status, a.body, r.keepAlive)
}
