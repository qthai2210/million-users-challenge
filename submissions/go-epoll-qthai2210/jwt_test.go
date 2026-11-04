package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

const testSecret = "million-users-challenge"

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func signJWT(header, payload string) string {
	h, p := b64(header), b64(payload)
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(h + "." + p))
	return h + "." + p + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestJWTVerify(t *testing.T) {
	const now = 1_800_000_000
	const hs256 = `{"alg":"HS256","typ":"JWT"}`
	good := signJWT(hs256, `{"sub":"42","username":"golden_ember_1","iat":1799990000,"exp":1800003600}`)
	goodNoTyp := signJWT(`{"alg":"HS256"}`, `{"sub":"7","username":"x","exp":1800000001}`)
	tampered := good[:len(good)-4] + "AAAA"
	none := b64(`{"alg":"none","typ":"JWT"}`) + "." + b64(`{"sub":"42","username":"a","exp":1800003600}`) + "."

	tests := []struct {
		name   string
		header string
		want   authResult
		user   user
	}{
		{"valid", "Bearer " + good, authOK, user{42, "golden_ember_1"}},
		{"valid, header without typ", "Bearer " + goodNoTyp, authOK, user{7, "x"}},
		{"no header", "", authMissing, user{}},
		{"basic auth", "Basic abc", authMissing, user{}},
		{"lowercase bearer", "bearer " + good, authMissing, user{}},
		{"garbage", "Bearer abc", authInvalid, user{}},
		{"four segments", "Bearer " + good + ".x", authInvalid, user{}},
		{"bad signature", "Bearer " + tampered, authInvalid, user{}},
		{"other secret", "Bearer " + good[:len(good)-43] + b64("signature-is-not-valid-at-all-at-all-1234"), authInvalid, user{}},
		{"alg none", "Bearer " + none, authInvalid, user{}},
		{"alg HS512", "Bearer " + signJWT(`{"alg":"HS512"}`, `{"sub":"1","username":"a","exp":1800003600}`), authInvalid, user{}},
		{"expired", "Bearer " + signJWT(hs256, `{"sub":"1","username":"a","exp":1799996400}`), authInvalid, user{}},
		{"exp equals now", "Bearer " + signJWT(hs256, `{"sub":"1","username":"a","exp":1800000000}`), authInvalid, user{}},
		{"no exp", "Bearer " + signJWT(hs256, `{"sub":"1","username":"a"}`), authInvalid, user{}},
		{"exp is a string", "Bearer " + signJWT(hs256, `{"sub":"1","username":"a","exp":"1800003600"}`), authInvalid, user{}},
		{"payload not JSON", "Bearer " + signJWT(hs256, `not json`), authInvalid, user{}},
		{"sub not a number", "Bearer " + signJWT(hs256, `{"sub":"abc","username":"a","exp":1800003600}`), authPayload, user{}},
		{"sub zero", "Bearer " + signJWT(hs256, `{"sub":"0","username":"a","exp":1800003600}`), authPayload, user{}},
		{"sub negative", "Bearer " + signJWT(hs256, `{"sub":"-1","username":"a","exp":1800003600}`), authPayload, user{}},
		{"sub is a JSON number", "Bearer " + signJWT(hs256, `{"sub":1,"username":"a","exp":1800003600}`), authPayload, user{}},
		{"no username", "Bearer " + signJWT(hs256, `{"sub":"1","exp":1800003600}`), authPayload, user{}},
		{"username not a string", "Bearer " + signJWT(hs256, `{"sub":"1","username":5,"exp":1800003600}`), authPayload, user{}},
	}
	v := newJWTVerifier([]byte(testSecret))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, got := v.verify([]byte(tc.header), now)
			if got != tc.want || u != tc.user {
				t.Fatalf("verify = (%+v, %d), want (%+v, %d)", u, got, tc.user, tc.want)
			}
		})
	}
}

func TestParsePositiveInt(t *testing.T) {
	for in, want := range map[string]int64{"1": 1, "42": 42, "500000": 500000, "007": 7} {
		if got, ok := parsePositiveInt(in); !ok || got != want {
			t.Errorf("parsePositiveInt(%q) = %d, %v", in, got, ok)
		}
	}
	for _, in := range []string{"", "0", "-1", "abc", "1.5", "+1", " 1", "1e3", "1234567890123456789"} {
		if got, ok := parsePositiveInt(in); ok {
			t.Errorf("parsePositiveInt(%q) = %d, want invalid", in, got)
		}
	}
}
