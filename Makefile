.PHONY: build test crash-test proto lint server clean

# Build every command into bin/
build:
	go build -o bin/ ./cmd/...

# All tests, with the race detector
test:
	go test -race ./...

# kill -9 the wal server at random moments and check no acked write is lost
ROUNDS ?= 50
CRASH_DIR ?= $(HOME)/.cache/raidkv-crashtest
crash-test:
	go test -tags crash -count=1 -v ./test/crash -rounds=$(ROUNDS) -dir=$(CRASH_DIR)

# Regenerate Go code from api/**/*.proto
proto:
	go tool buf generate

lint:
	go vet -tags crash ./...
	go tool staticcheck -tags crash ./...
	go tool buf lint

# Run a server on the default address
server: build
	./bin/raidkv-server

clean:
	rm -rf bin/
