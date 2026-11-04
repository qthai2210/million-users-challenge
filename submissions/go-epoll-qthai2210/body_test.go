package main

import (
	"strings"
	"testing"
)

func TestParsePostBody(t *testing.T) {
	tests := []struct {
		name, in, text, msg string
	}{
		{"plain", `{"body":"hello"}`, "hello", ""},
		{"trimmed", `{"body":"  test: <b>&amp;</b> \"quoted\" / café ✓  "}`, `test: <b>&amp;</b> "quoted" / café ✓`, ""},
		{"escaped newline kept inside", `{"body":" a\nb "}`, "a\nb", ""},
		{"unicode escape", `{"body":"café"}`, "café", ""},
		{"extra keys ignored", `{"x":1,"body":"hi"}`, "hi", ""},
		{"empty", `{"body":""}`, "", "body is required"},
		{"whitespace", `{"body":"   "}`, "", "body is required"},
		{"number", `{"body":123}`, "", "body is required"},
		{"null", `{"body":null}`, "", "body is required"},
		{"missing", `{}`, "", "body is required"},
		{"empty request body", ``, "", "body is required"},
		{"json null", `null`, "", "body is required"},
		{"malformed", `{bad json`, "", "malformed JSON body"},
		{"array", `[]`, "", "malformed JSON body"},
		{"trailing garbage", `{"body":"x"} y`, "", "malformed JSON body"},
		{"500 chars", `{"body":"` + strings.Repeat("b", 500) + `"}`, strings.Repeat("b", 500), ""},
		{"500 chars after trim", `{"body":"  ` + strings.Repeat("b", 500) + ` "}`, strings.Repeat("b", 500), ""},
		{"501 chars", `{"body":"` + strings.Repeat("a", 501) + `"}`, "", "body must be at most 500 characters"},
		{"500 accented", `{"body":"` + strings.Repeat("é", 500) + `"}`, strings.Repeat("é", 500), ""},
		{"250 emoji = 500 UTF-16 units", `{"body":"` + strings.Repeat("😀", 250) + `"}`, strings.Repeat("😀", 250), ""},
		{"251 emoji", `{"body":"` + strings.Repeat("😀", 251) + `"}`, "", "body must be at most 500 characters"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			text, msg := parsePostBody([]byte(tc.in))
			if text != tc.text || msg != tc.msg {
				t.Fatalf("got (%q, %q), want (%q, %q)", text, msg, tc.text, tc.msg)
			}
		})
	}
}
