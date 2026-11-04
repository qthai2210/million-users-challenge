package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// usSince2000 converts a time to binary timestamptz microseconds.
func usSince2000(t time.Time) int64 { return t.UnixMicro() - pgEpochMs*1000 }

func TestAppendTimestamp(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"2025-12-31T23:59:19.409999Z", `"2025-12-31T23:59:19.409Z"`}, // truncated, not rounded
		{"2025-12-31T23:59:19.409000Z", `"2025-12-31T23:59:19.409Z"`},
		{"2026-01-01T00:00:00Z", `"2026-01-01T00:00:00.000Z"`},
		{"2000-01-01T00:00:00.000001Z", `"2000-01-01T00:00:00.000Z"`},
		{"1999-12-31T23:59:59.999999Z", `"1999-12-31T23:59:59.999Z"`}, // negative offset floors
		{"2026-11-04T07:05:06.123456+07:00", `"2026-11-04T00:05:06.123Z"`},
	}
	for _, tc := range tests {
		tm, err := time.Parse(time.RFC3339Nano, tc.in)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(appendTimestamp(nil, usSince2000(tm))); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestAppendJSONString(t *testing.T) {
	tests := map[string]string{
		`plain`:                 `"plain"`,
		`say "hi"`:              `"say \"hi\""`,
		`back\slash`:            `"back\\slash"`,
		"line\nbreak\ttab\r":    `"line\nbreak\ttab\r"`,
		"\x00\x01\x1f":          `"\u0000\u0001\u001f"`,
		`<b>&amp;</b> / café ✓`: `"<b>&amp;</b> / café ✓"`,
	}
	for in, want := range tests {
		got := appendJSONString(nil, in)
		if string(got) != want {
			t.Errorf("appendJSONString(%q) = %s, want %s", in, got, want)
		}
		var back string
		if err := json.Unmarshal(got, &back); err != nil || back != in {
			t.Errorf("round-trip of %q: %q, %v", in, back, err)
		}
	}
}

func TestAppendPost(t *testing.T) {
	created := usSince2000(time.Date(2025, 12, 31, 23, 59, 19, 409_999_000, time.UTC))
	got := string(appendPost(nil, int64(500000), "TIL \"quoted\"", created, "swift_ember_22228", 5))
	want := `{"id":500000,"body":"TIL \"quoted\"","created_at":"2025-12-31T23:59:19.409Z","author":"swift_ember_22228","like_count":5}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if !strings.HasPrefix(got, `{"id":`) {
		t.Fatal("key order")
	}
}
