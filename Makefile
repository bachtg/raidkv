.PHONY: build test proto lint server clean

# Build every command into bin/
build:
	go build -o bin/ ./cmd/...

# All tests, with the race detector
test:
	go test -race ./...

# Regenerate Go code from api/**/*.proto
proto:
	go tool buf generate

lint:
	go vet ./...
	go tool staticcheck ./...
	go tool buf lint

# Run a server on the default address
server: build
	./bin/raidkv-server

clean:
	rm -rf bin/
