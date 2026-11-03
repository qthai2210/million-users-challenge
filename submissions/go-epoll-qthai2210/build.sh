#!/usr/bin/env bash
# Builds ./bin/server as a normal user. Dependencies are pinned in go.mod/go.sum; the toolchain is the one install.sh put
# in /usr/local/go (GOTOOLCHAIN=local, so nothing else is downloaded).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export PATH=/usr/local/go/bin:$PATH CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-mod=readonly
go mod download
go build -trimpath -ldflags='-s -w' -o bin/server .
echo "built bin/server ($(go env GOVERSION), $(go env GOARCH))"
