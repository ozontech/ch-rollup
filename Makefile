GOLANGCI_LINT_VERSION = 2.4.0
ACTIONLINT_VERSION = 1.7.7
MODERNIZE_VERSION = 0.20.0

test:
	go test --timeout 10m -race ./...

coverage:
	go test -race -v -coverpkg=./... -coverprofile=profile.out ./...
	go tool cover -func profile.out

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION) run

actionlint:
	go run github.com/rhysd/actionlint/cmd/actionlint@v$(ACTIONLINT_VERSION)

modernize:
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v$(MODERNIZE_VERSION) -fix ./...
