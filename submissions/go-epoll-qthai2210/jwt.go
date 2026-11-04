package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"hash"
)

// authResult is the outcome of checking a request's Authorization header, in the order the spec's error table
// checks them.
type authResult int

const (
	authOK      authResult = iota
	authMissing            // 401 missing bearer token
	authInvalid            // 401 invalid or expired token
	authPayload            // 401 invalid token payload
)

var authMessage = [...]string{
	authMissing: "missing bearer token",
	authInvalid: "invalid or expired token",
	authPayload: "invalid token payload",
}

type user struct {
	id       int64
	username string
}

// jwtVerifier checks HS256 tokens. It keeps one HMAC keyed with the secret (the key pads, not any token) and is
// owned by a single loop, so it needs no locking. Every token is fully verified on every request (rule 9).
type jwtVerifier struct {
	mac hash.Hash
	sig [sha256.Size]byte
}

func newJWTVerifier(secret []byte) *jwtVerifier {
	return &jwtVerifier{mac: hmac.New(sha256.New, secret)}
}

var bearer = []byte("Bearer ")

// verify checks the Authorization header value at unix time now.
func (v *jwtVerifier) verify(authorization []byte, now int64) (user, authResult) {
	if !bytes.HasPrefix(authorization, bearer) {
		return user{}, authMissing
	}
	token := bytes.TrimSpace(authorization[len(bearer):])
	head, rest, ok1 := bytes.Cut(token, []byte("."))
	payload, sig, ok2 := bytes.Cut(rest, []byte("."))
	if !ok1 || !ok2 || bytes.IndexByte(sig, '.') >= 0 {
		return user{}, authInvalid
	}

	var header struct {
		Alg string `json:"alg"`
	}
	if !decodeSegment(head, &header) || header.Alg != "HS256" {
		return user{}, authInvalid
	}
	if base64.RawURLEncoding.DecodedLen(len(sig)) != sha256.Size {
		return user{}, authInvalid
	}
	if _, err := base64.RawURLEncoding.Decode(v.sig[:], sig); err != nil {
		return user{}, authInvalid
	}
	v.mac.Reset()
	v.mac.Write(token[:len(head)+1+len(payload)])
	if !hmac.Equal(v.mac.Sum(nil), v.sig[:]) {
		return user{}, authInvalid
	}

	var claims map[string]any
	if !decodeSegment(payload, &claims) {
		return user{}, authInvalid
	}
	exp, ok := claims["exp"].(float64)
	if !ok || float64(now) >= exp {
		return user{}, authInvalid
	}
	sub, okSub := claims["sub"].(string)
	name, okName := claims["username"].(string)
	id, okID := parsePositiveInt(sub)
	if !okSub || !okID || !okName {
		return user{}, authPayload
	}
	return user{id: id, username: name}, authOK
}

// decodeSegment base64url-decodes one JWT segment and unmarshals it as JSON into dst.
func decodeSegment(seg []byte, dst any) bool {
	raw := make([]byte, base64.RawURLEncoding.DecodedLen(len(seg)))
	n, err := base64.RawURLEncoding.Decode(raw, seg)
	return err == nil && json.Unmarshal(raw[:n], dst) == nil
}

// parsePositiveInt parses a decimal integer > 0 made only of digits (no sign, no spaces), up to 18 digits.
func parsePositiveInt(s string) (int64, bool) {
	if len(s) == 0 || len(s) > 18 {
		return 0, false
	}
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, n > 0
}
