#!/usr/bin/env bash
# The server, in the foreground. Reads DATABASE_URL, JWT_SECRET, HOST, PORT and EXTRA_PORT; GC and thread settings are
# made inside the process.
set -euo pipefail
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bin/server"
