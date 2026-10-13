#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
scripts/go.sh install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
scripts/go.sh install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
export PATH="$PWD/.cache/tools/bin:$PATH"
# protoc must be installed separately, including standard protobuf include files.
protoc -I . --go_out=. --go_opt=module=github.com/mreastmelon-cyber/right-capital-project \
  --go-grpc_out=. --go-grpc_opt=module=github.com/mreastmelon-cyber/right-capital-project \
  api/notification/v1/notification.proto
