package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// The few Postgres protocol v3 messages the loops speak once pgconn has logged in and prepared the statements:
// Bind, Execute and Sync out; BindComplete, DataRow, CommandComplete, ErrorResponse and ReadyForQuery in.

// appendBind appends a Bind message for a prepared statement, with the unnamed portal. A nil parameter is NULL.
func appendBind(dst []byte, stmt string, paramFormats []int16, params [][]byte, resultFormats []int16) []byte {
	start := len(dst)
	dst = append(dst, 'B', 0, 0, 0, 0, 0) // type, length (patched below), unnamed portal
	dst = append(dst, stmt...)
	dst = append(dst, 0)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(paramFormats)))
	for _, f := range paramFormats {
		dst = binary.BigEndian.AppendUint16(dst, uint16(f))
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(params)))
	for _, p := range params {
		if p == nil {
			dst = binary.BigEndian.AppendUint32(dst, 0xFFFFFFFF)
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(p)))
		dst = append(dst, p...)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(resultFormats)))
	for _, f := range resultFormats {
		dst = binary.BigEndian.AppendUint16(dst, uint16(f))
	}
	binary.BigEndian.PutUint32(dst[start+1:], uint32(len(dst)-start-1))
	return dst
}

// appendExecute appends an Execute of the unnamed portal with no row limit.
func appendExecute(dst []byte) []byte { return append(dst, 'E', 0, 0, 0, 9, 0, 0, 0, 0, 0) }

// appendSync appends a Sync: the end of an implicit transaction, answered by ReadyForQuery.
func appendSync(dst []byte) []byte { return append(dst, 'S', 0, 0, 0, 4) }

// maxBackendMessage bounds one backend message; a feed row set is a few KB.
const maxBackendMessage = 16 << 20

var errBadMessage = errors.New("malformed message from postgres")

// nextMessage splits one backend message off the front of b: its type, its body and the bytes it used. n is 0 when
// b doesn't hold a whole message yet.
func nextMessage(b []byte) (typ byte, body []byte, n int, err error) {
	if len(b) < 5 {
		return 0, nil, 0, nil
	}
	l := int(binary.BigEndian.Uint32(b[1:5]))
	if l < 4 || l > maxBackendMessage {
		return 0, nil, 0, errBadMessage
	}
	if len(b) < 1+l {
		return 0, nil, 0, nil
	}
	return b[0], b[5 : 1+l], 1 + l, nil
}

// dataRowValues splits a DataRow body into its column values (nil for NULL), reusing vals.
func dataRowValues(body []byte, vals [][]byte) ([][]byte, error) {
	vals = vals[:0]
	if len(body) < 2 {
		return vals, errBadMessage
	}
	n := int(binary.BigEndian.Uint16(body))
	p := 2
	for i := 0; i < n; i++ {
		if len(body) < p+4 {
			return vals, errBadMessage
		}
		l := int(int32(binary.BigEndian.Uint32(body[p:])))
		p += 4
		if l < 0 {
			vals = append(vals, nil)
			continue
		}
		if len(body) < p+l {
			return vals, errBadMessage
		}
		vals = append(vals, body[p:p+l:p+l])
		p += l
	}
	return vals, nil
}

// commandRows returns the row count that ends a CommandComplete tag: "INSERT 0 1" is 1, "SELECT 20" is 20.
func commandRows(body []byte) int64 {
	tag := bytes.TrimRight(body, "\x00")
	var n int64
	for i := bytes.LastIndexByte(tag, ' ') + 1; i < len(tag); i++ {
		c := tag[i]
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// errorFields returns the SQLSTATE (field 'C') and message (field 'M') of an ErrorResponse.
func errorFields(body []byte) (code, msg string) {
	for len(body) > 0 && body[0] != 0 {
		field := body[0]
		end := bytes.IndexByte(body[1:], 0)
		if end < 0 {
			break
		}
		value := body[1 : 1+end]
		switch field {
		case 'C':
			code = string(value)
		case 'M':
			msg = string(value)
		}
		body = body[2+end:]
	}
	return code, msg
}

// connectionLost reports SQLSTATEs that mean the connection is gone: class 08 (connection exception) and 57P
// (operator intervention: admin shutdown, crash shutdown, cannot connect now...).
func connectionLost(code string) bool {
	return len(code) == 5 && (code[:2] == "08" || code[:3] == "57P")
}
