.PHONY: build run test lint tidy

build:
	go build -o bin/txn-processor ./cmd/main.go

run:
	docker-compose up --build

test:
	go test ./... -v -count=1

tidy:
	go mod tidy

lint:
	golangci-lint run ./...
