#!/usr/bin/env bash
# Run once as root on Ubuntu 24.04 (safe to repeat): Go 1.27.1 from the official tarball under /usr/local/go.
# Pure Go (CGO_ENABLED=0), so no compiler or C libraries are needed.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
GO_VERSION=1.27.1
ARCH=$(dpkg --print-architecture)   # amd64 on the benchmark box and CI; arm64 works for local dev

go_ok() { [ -x /usr/local/go/bin/go ] && [ "$(/usr/local/go/bin/go env GOVERSION)" = "go$GO_VERSION" ]; }
if ! go_ok; then
  command -v curl >/dev/null || { apt-get update -qq && apt-get install -y -qq --no-install-recommends curl ca-certificates >/dev/null; }
  tgz=$(mktemp)
  curl --fail --silent --show-error --location "https://go.dev/dl/go$GO_VERSION.linux-$ARCH.tar.gz" --output "$tgz"
  rm -rf /usr/local/go && tar -C /usr/local -xzf "$tgz" && rm -f "$tgz"
fi
/usr/local/go/bin/go version
