#!/bin/sh
# Build using the host's Go installation when Docker Hub is unavailable.
set -eu
cd "$(dirname "$0")/.."
arch=$(docker info --format '{{.Architecture}}')
case "$arch" in
  x86_64|amd64) target=amd64 ;;
  aarch64|arm64) target=arm64 ;;
  *) echo "Unsupported Docker architecture: $arch" >&2; exit 1 ;;
esac
certs=${CA_BUNDLE:-/etc/ssl/cert.pem}
if [ ! -r "$certs" ]; then certs=/etc/ssl/certs/ca-certificates.crt; fi
if [ ! -r "$certs" ]; then echo 'Set CA_BUNDLE to a trusted PEM CA bundle' >&2; exit 1; fi
mkdir -p bin/container/configs
CGO_ENABLED=0 GOOS=linux GOARCH="$target" scripts/go.sh build -trimpath -o bin/container/notification-service ./cmd/notification-service
CGO_ENABLED=0 GOOS=linux GOARCH="$target" scripts/go.sh build -trimpath -o bin/container/mock-provider ./cmd/mock-provider
cp "$certs" bin/container/ca-certificates.crt
cp configs/docker.json bin/container/configs/docker.json
docker build -f deploy/Dockerfile.prebuilt -t right-capital-notification:local bin/container
