package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf16"
)

const maxPostBodyLength = 500

// parsePostBody validates the JSON body of POST /posts. It returns the trimmed text, or the error message for a 400.
// Like the reference servers, an empty request body counts as a missing "body", and the length is counted in UTF-16
// code units (JavaScript's .length), which is never less than the code points Postgres's char_length counts.
func parsePostBody(raw []byte) (string, string) {
	var req struct {
		Body any `json:"body"`
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return "", "malformed JSON body"
		}
	}
	text, _ := req.Body.(string)
	text = strings.TrimSpace(text)
	if text == "" {
		return "", "body is required"
	}
	if utf16Len(text) > maxPostBodyLength {
		return "", "body must be at most 500 characters"
	}
	return text, ""
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}
