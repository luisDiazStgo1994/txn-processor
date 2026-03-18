# ---- Build stage ----
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o txn-processor ./cmd/main.go

# ---- Run stage ----
FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/txn-processor .
COPY --from=builder /app/templates ./templates

VOLUME /app/data

ENTRYPOINT ["./txn-processor"]
