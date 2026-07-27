# Multi-stage build for smux container integration test.
# Both client and server use the same image, differentiated by CMD.

FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
COPY *.go ./
RUN go mod download
COPY container/ container/
RUN CGO_ENABLED=0 go build -o /smux-test ./container/smux_test/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates netcat-openbsd
COPY --from=builder /smux-test /usr/local/bin/smux-test
ENTRYPOINT ["/usr/local/bin/smux-test"]
