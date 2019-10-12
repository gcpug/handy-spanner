all: test build

export GO111MODULE=on

.PHONY: build
build:
	go build -v --tags "json1" ./cmd/handy-spanner

.PHONY: test
test:
	go test -v --tags "json1" -race ./...

docker-build:
	docker build . -t handy-spanner
