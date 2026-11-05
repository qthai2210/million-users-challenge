package main

import (
	"strconv"
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
	ms := floorDiv(us, 1000) + pgEpochMs // unix milliseconds
	days := floorDiv(ms, 86400000)
	rem := ms - days*86400000
	y, m, d := civil(days)
	dst = append(dst, '"')
	dst = append4(dst, y)
	dst = append(dst, '-', byte('0'+m/10), byte('0'+m%10), '-', byte('0'+d/10), byte('0'+d%10), 'T')
	h, mi, s, f := rem/3600000, rem/60000%60, rem/1000%60, rem%1000
	dst = append(dst, byte('0'+h/10), byte('0'+h%10), ':', byte('0'+mi/10), byte('0'+mi%10), ':', byte('0'+s/10), byte('0'+s%10),
		'.', byte('0'+f/100), byte('0'+f/10%10), byte('0'+f%10), 'Z')
	return append(dst, '"')
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b < 0 {
		q--
	}
	return q
}

// civil converts days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's days_from_civil, inverted).
func civil(days int64) (y, m, d int64) {
	z := days + 719468
	era := floorDiv(z, 146097)
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = doy - (153*mp+2)/5 + 1
	m = mp + 3
	if m > 12 {
		m -= 12
	}
	y = yoe + era*400
	if m <= 2 {
		y++
	}
	return y, m, d
}

// append4 appends a year as four digits (0000-9999).
func append4(dst []byte, y int64) []byte {
	return append(dst, byte('0'+y/1000%10), byte('0'+y/100%10), byte('0'+y/10%10), byte('0'+y%10))
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
