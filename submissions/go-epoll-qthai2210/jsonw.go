package main

import (
	"strconv"
	"time"
)

// pgEpochMs is 2000-01-01T00:00:00Z in unix milliseconds: binary timestamptz counts microseconds from there.
const pgEpochMs = 946684800000

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string. Only what JSON requires is escaped (quote, backslash, control
// characters), the same output as JavaScript's JSON.stringify; non-ASCII stays raw UTF-8.
func appendJSONString[T string | []byte](dst []byte, s T) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// appendTimestamp appends a binary timestamptz (microseconds since 2000-01-01 UTC) as a quoted UTC string with
// milliseconds, truncated (not rounded): "2025-12-31T23:59:19.409Z".
func appendTimestamp(dst []byte, us int64) []byte {
	ms := us / 1000
	if us%1000 < 0 { // floor for instants before 2000
		ms--
	}
	dst = append(dst, '"')
	dst = time.UnixMilli(pgEpochMs+ms).UTC().AppendFormat(dst, "2006-01-02T15:04:05.000Z")
	return append(dst, '"')
}

// appendPost appends a post object with keys in the spec's order.
func appendPost[T string | []byte](dst []byte, id int64, body T, createdUs int64, author T, likes int64) []byte {
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, id, 10)
	dst = append(dst, `,"body":`...)
	dst = appendJSONString(dst, body)
	dst = append(dst, `,"created_at":`...)
	dst = appendTimestamp(dst, createdUs)
	dst = append(dst, `,"author":`...)
	dst = appendJSONString(dst, author)
	dst = append(dst, `,"like_count":`...)
	dst = strconv.AppendInt(dst, likes, 10)
	return append(dst, '}')
}
