SHELL := /bin/bash

.PHONY: all check format vet lint build test generate tidy

help:
	@echo "Please use \`make <target>\` where <target> is one of"
	@echo "  check               to do static check"
	@echo "  build               to create bin directory and build"
	@echo "  generate            to generate code"
	@echo "  test                to run test"
	@echo "  integration_test    to run integration test"

check: vet

format:
	gofmt -w -l .

vet:
	go vet ./...

generate:
	go generate ./...
	gofmt -w -l .

build: tidy generate format check
	go build ./...

test:
	go test -race -coverprofile=coverage.txt -covermode=atomic -v ./...
	go tool cover -html="coverage.txt" -o "coverage.html"

tidy:
	go mod tidy
	go mod verify

clean:
	find . -type f -name 'generated.go' -delete
