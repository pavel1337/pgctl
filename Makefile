# test
.PHONY: test
test:
	go test ./...

test-db:
	docker run --name postgres-test -e POSTGRES_PASSWORD=password -p 55432:5432 -d postgres:latest
