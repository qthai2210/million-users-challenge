package main

import (
	"encoding/binary"
)

// The SQL from SPEC.md, verbatim (rule 3). The feed keeps LIMIT 20 as a literal: binding it as $1 makes Postgres
// re-plan the generic statement.
const (
	sqlPostSelect = `SELECT p.id, p.body, p.created_at, u.username,
       (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count
  FROM posts p JOIN users u ON u.id = p.user_id`
	sqlFeed       = sqlPostSelect + "\n ORDER BY p.created_at DESC, p.id DESC LIMIT 20"
	sqlPostByID   = sqlPostSelect + "\n WHERE p.id = $1"
	sqlInsertPost = `INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at`
	sqlInsertLike = `INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING`
	sqlHealth     = `SELECT 1`
)

const (
	stmtFeed       = "feed"
	stmtPostByID   = "post_by_id"
	stmtInsertPost = "insert_post"
	stmtInsertLike = "insert_like"
	stmtHealth     = "health"
)

// statements are prepared on every connection when it is opened.
var statements = []struct{ name, sql string }{
	{stmtFeed, sqlFeed},
	{stmtPostByID, sqlPostByID},
	{stmtInsertPost, sqlInsertPost},
	{stmtInsertLike, sqlInsertLike},
	{stmtHealth, sqlHealth},
}

// Format codes: a single code applies to every parameter (or result column).
var (
	allBinary = []int16{1}
	idAndText = []int16{1, 0} // insert_post: user_id binary, body text
)

func int8Param(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func readInt8(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

func readInt4(b []byte) int64 { return int64(int32(binary.BigEndian.Uint32(b))) }
