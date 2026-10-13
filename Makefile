.PHONY: build test race integration vet generate up down smoke
build:
	@mkdir -p bin
	scripts/go.sh build -o bin/notification-service ./cmd/notification-service
	scripts/go.sh build -o bin/mock-provider ./cmd/mock-provider
test:
	scripts/go.sh test ./... -count=1
race:
	scripts/go.sh test -race ./... -count=1
integration:
	scripts/go.sh test -race -tags integration ./... -count=1 -timeout 120s
vet:
	scripts/go.sh vet ./...
generate:
	scripts/generate.sh
up:
	docker compose --env-file .env -f deploy/compose.yaml up --build -d
down:
	docker compose --env-file .env -f deploy/compose.yaml down

smoke:
	scripts/go.sh run ./tests/smoke
