package main

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
)

const (
	stmtFeed       = "feed"
	stmtPostByID   = "post_by_id"
	stmtInsertPost = "insert_post"
	stmtInsertLike = "insert_like"
)

var statements = []struct{ name, sql string }{
	{stmtFeed, sqlFeed},
	{stmtPostByID, sqlPostByID},
	{stmtInsertPost, sqlInsertPost},
	{stmtInsertLike, sqlInsertLike},
}

const dbTimeout = 5 * time.Second

// Format codes: a single code applies to every parameter (or result column).
var (
	allBinary = []int16{1}
	idAndText = []int16{1, 0} // insert_post: user_id binary, body text
)

// db is one synchronous Postgres connection (P2). It reconnects and re-prepares on the next call after the
// connection is lost. P3 replaces this with pipelined connections inside the epoll loop.
type db struct {
	url  string
	conn *pgconn.PgConn
}

func (d *db) connect(ctx context.Context) (*pgconn.PgConn, error) {
	if d.conn != nil && !d.conn.IsClosed() {
		return d.conn, nil
	}
	d.conn = nil
	c, err := pgconn.Connect(ctx, d.url)
	if err != nil {
		return nil, err
	}
	for _, s := range statements {
		if _, err := c.Prepare(ctx, s.name, s.sql, nil); err != nil {
			c.Close(context.Background())
			return nil, err
		}
	}
	d.conn = c
	return c, nil
}

// exec runs a prepared statement and calls row for every result row (binary format). The values passed to row are
// only valid during the call. A read that fails because the connection was lost is retried once on a new
// connection; a write is not, since it may have committed (the caller answers 500).
func (d *db) exec(stmt string, params [][]byte, formats []int16, row func([][]byte)) (pgconn.CommandTag, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	read := stmt == stmtFeed || stmt == stmtPostByID
	for attempt := 0; ; attempt++ {
		c, err := d.connect(ctx)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		rows := 0
		rr := c.ExecPrepared(ctx, stmt, params, formats, allBinary)
		for rr.NextRow() {
			rows++
			if row != nil {
				row(rr.Values())
			}
		}
		tag, err := rr.Close()
		if err == nil || !c.IsClosed() {
			return tag, err
		}
		d.conn = nil
		if !read || rows > 0 || attempt > 0 {
			return tag, err
		}
	}
}

func (d *db) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := d.connect(ctx)
	if err != nil {
		return err
	}
	if _, err := c.Exec(ctx, "SELECT 1").ReadAll(); err != nil {
		if c.IsClosed() {
			d.conn = nil
		}
		return err
	}
	return nil
}

func int8Param(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func readInt8(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

func readInt4(b []byte) int64 { return int64(int32(binary.BigEndian.Uint32(b))) }

// isForeignKeyViolation reports SQLSTATE 23503 (a like on a post that doesn't exist).
func isForeignKeyViolation(err error) bool {
	pe, ok := err.(*pgconn.PgError)
	return ok && pe.Code == "23503"
}
