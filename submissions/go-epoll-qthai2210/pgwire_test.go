package main

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The frontend messages must be byte for byte what pgx's own encoder produces.
func TestFrontendMessagesMatchPgproto3(t *testing.T) {
	params := [][]byte{int8Param(1), nil, []byte("hello")}
	formats := []int16{1, 0, 0}
	got := appendBind(nil, "insert_post", formats, params, allBinary)
	want, err := (&pgproto3.Bind{PreparedStatement: "insert_post", ParameterFormatCodes: formats, Parameters: params, ResultFormatCodes: allBinary}).Encode(nil)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Bind:\n got %x\nwant %x (%v)", got, want, err)
	}
	got = appendBind(nil, "feed", nil, nil, allBinary)
	want, _ = (&pgproto3.Bind{PreparedStatement: "feed", ResultFormatCodes: allBinary}).Encode(nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("Bind without params:\n got %x\nwant %x", got, want)
	}
	if want, _ := (&pgproto3.Execute{}).Encode(nil); !bytes.Equal(appendExecute(nil), want) {
		t.Fatalf("Execute: got %x want %x", appendExecute(nil), want)
	}
	if want, _ := (&pgproto3.Sync{}).Encode(nil); !bytes.Equal(appendSync(nil), want) {
		t.Fatalf("Sync: got %x want %x", appendSync(nil), want)
	}
}

// A batch of backend messages, as Postgres sends them for one read and one failed write.
func backendStream(t *testing.T) []byte {
	var b []byte
	for _, m := range []pgproto3.BackendMessage{
		&pgproto3.BindComplete{},
		&pgproto3.DataRow{Values: [][]byte{int8Param(41), []byte("body"), nil}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23503", Message: "violates foreign key constraint"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	} {
		var err error
		if b, err = m.Encode(b); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func TestBackendMessages(t *testing.T) {
	b := backendStream(t)
	var types []byte
	for len(b) > 0 {
		typ, body, n, err := nextMessage(b)
		if err != nil || n == 0 {
			t.Fatalf("nextMessage: n=%d err=%v", n, err)
		}
		types = append(types, typ)
		switch typ {
		case 'D':
			vals, err := dataRowValues(body, nil)
			if err != nil || len(vals) != 3 || readInt8(vals[0]) != 41 || string(vals[1]) != "body" || vals[2] != nil {
				t.Fatalf("DataRow: %q %v", vals, err)
			}
		case 'C':
			if commandRows(body) != 1 {
				t.Fatalf("CommandComplete rows: %d", commandRows(body))
			}
		case 'E':
			if code, msg := errorFields(body); code != "23503" || msg != "violates foreign key constraint" {
				t.Fatalf("ErrorResponse: %q %q", code, msg)
			}
		}
		b = b[n:]
	}
	if string(types) != "2DCEZ" {
		t.Fatalf("types %q", types)
	}
}

// Every prefix of a message stream must decode as "not complete yet", never as an error or a wrong message.
func TestBackendMessagesSplitAnywhere(t *testing.T) {
	stream := backendStream(t)
	for cut := 0; cut <= len(stream); cut++ {
		var got []byte
		buf := append([]byte(nil), stream[:cut]...)
		rest := stream[cut:]
		for {
			typ, _, n, err := nextMessage(buf)
			if err != nil {
				t.Fatalf("cut %d: %v", cut, err)
			}
			if n == 0 {
				if len(rest) == 0 {
					break
				}
				buf, rest = append(buf, rest...), nil
				continue
			}
			got = append(got, typ)
			buf = buf[n:]
		}
		if string(got) != "2DCEZ" {
			t.Fatalf("cut %d: types %q", cut, got)
		}
	}
}

func TestCommandRowsAndErrors(t *testing.T) {
	for tag, want := range map[string]int64{"INSERT 0 1\x00": 1, "INSERT 0 0\x00": 0, "SELECT 20\x00": 20, "BEGIN\x00": 0} {
		if got := commandRows([]byte(tag)); got != want {
			t.Errorf("commandRows(%q) = %d, want %d", tag, got, want)
		}
	}
	for code, want := range map[string]bool{"08006": true, "57P01": true, "57P03": true, "23503": false, "57014": false, "": false} {
		if connectionLost(code) != want {
			t.Errorf("connectionLost(%q) != %v", code, want)
		}
	}
	if _, _, _, err := nextMessage([]byte{'D', 0, 0, 0, 2}); err == nil {
		t.Error("length < 4 must be an error")
	}
}
