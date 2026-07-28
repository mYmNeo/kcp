FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /kcp-test ./container_test/

FROM alpine:latest
COPY --from=builder /kcp-test /kcp-test
ENTRYPOINT ["/kcp-test"]
