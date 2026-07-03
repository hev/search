.PHONY: build install test vet fmt clean help

BINARY := hev
BINDIR := $(shell go env GOBIN)
ifeq ($(BINDIR),)
BINDIR := $(shell go env GOPATH)/bin
endif

VERSION ?= dev
LDFLAGS := -X github.com/hev/search/cli/cmd.version=$(VERSION)

build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) .

install:
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/$(BINARY) .
	@echo "Installed $(BINARY) to $(BINDIR)"

test:
	go test ./... -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -f $(BINARY)

help:
	@echo "Targets:"
	@echo "  build    - Build the hev binary in the current directory"
	@echo "  install  - Build and install hev to \$$GOBIN or ~/go/bin"
	@echo "  test     - Run unit tests with the race detector"
	@echo "  vet      - Run go vet"
	@echo "  fmt      - Run gofmt -w on all sources"
	@echo "  clean    - Remove the built binary"
