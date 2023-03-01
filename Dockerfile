FROM golang:1.18-alpine3.16 AS builder

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

FROM alpine:3.16.1

COPY --from=builder /go/src/handy-spanner/handy-spanner /usr/local/bin/handy-spanner

RUN apk --no-cache add \
	ca-certificates tzdata

USER nobody
EXPOSE 9999
ENTRYPOINT ["/usr/local/bin/handy-spanner"]
