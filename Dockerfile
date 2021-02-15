FROM golang:1.15.8-alpine3.13 AS builder

RUN set -eux \
	&& apk --no-cache add \
		g++ \
		gcc \
		git \
		make \
		musl-dev

COPY . /go/src/handy-spanner
WORKDIR /go/src/handy-spanner

RUN make build

FROM alpine:3.13.1

COPY --from=builder /go/src/handy-spanner/handy-spanner /usr/local/bin/handy-spanner

RUN apk --no-cache add \
	ca-certificates tzdata

USER nobody
EXPOSE 9999
ENTRYPOINT ["/usr/local/bin/handy-spanner"]
