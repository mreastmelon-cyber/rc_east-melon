#!/bin/sh
set -eu
TASK_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$TASK_ROOT"
if [ -n "${GO_BINARY:-}" ]; then
  GO_COMMAND=$GO_BINARY
elif command -v go >/dev/null 2>&1; then
  GO_COMMAND=$(command -v go)
elif [ -x "$TASK_ROOT/.cache/tools/go/bin/go" ]; then
  GO_COMMAND="$TASK_ROOT/.cache/tools/go/bin/go"
else
  echo 'Go 1.26+ is required; install Go or set GO_BINARY.' >&2
  exit 1
fi
export GOCACHE="$TASK_ROOT/.cache/build"
export GOMODCACHE="$TASK_ROOT/.cache/mod"
export GOPATH="$TASK_ROOT/.cache/gopath"
export GOBIN="$TASK_ROOT/.cache/tools/bin"
export PATH="$TASK_ROOT/.cache/tools/bin:$PATH"
exec "$GO_COMMAND" "$@"
