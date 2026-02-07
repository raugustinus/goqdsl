BINARY  := goqdsl
CMD     := ./cmd/$(BINARY)
BIN_DIR := bin

.PHONY: all build test clean fmt vet lint run

all: fmt vet test build

## build: compile the binary into bin/
build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) $(CMD)

## test: run all tests
test:
	go test ./...

## clean: remove build artifacts
clean:
	rm -rf $(BIN_DIR)

## fmt: format all Go source files
fmt:
	go fmt ./...

## vet: run go vet on all packages
vet:
	go vet ./...

## lint: run staticcheck (if installed)
lint:
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed — skipping"

## run: build and run the demo
run: build
	$(BIN_DIR)/$(BINARY)

## help: show this help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
