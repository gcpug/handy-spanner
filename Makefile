all: test build

export GO111MODULE=on

.PHONY: build
build:
	go build -v ./cmd/handy-spanner

.PHONY: test
test:
	go test -v -race ./...

docker-build:
	docker build . -t handy-spanner
