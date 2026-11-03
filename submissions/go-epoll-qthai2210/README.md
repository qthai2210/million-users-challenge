# go-epoll-qthai2210

Go 1.27.1, no web framework: an own epoll event loop, a hand-written HTTP/1.1 parser and Postgres through
[pgconn](https://github.com/jackc/pgx) (pgx v5.11.0). Pure Go, `CGO_ENABLED=0`.

Work in progress: design notes, rule compliance and measurements will be added before submitting.

| File | |
|---|---|
| `install.sh` | Go 1.27.1 from the official tarball into `/usr/local/go` |
| `build.sh` | `go build` into `bin/server` with the pinned toolchain and `go.sum` |
| `start.sh` | Runs `bin/server` in the foreground |
