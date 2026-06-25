.PHONY: build check fmt fmt-check test vet

build:
	go build ./...

check: fmt-check vet build test

fmt:
	go fmt ./...

fmt-check:
	@files="$$(find . -name '*.go' -not -path './vendor/*' -print | xargs gofmt -l)"; \
	if [ -n "$$files" ]; then \
		echo "$$files"; \
		exit 1; \
	fi

test:
	go test ./...

vet:
	go vet ./...
