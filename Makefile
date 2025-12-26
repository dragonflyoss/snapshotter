# Copyright 2025 The Dragonfly Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: all generate build test test-coverage lint fmt vet clean mod-tidy mod-download help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOFMT=gofmt
GOVET=$(GOCMD) vet
GOMOD=$(GOCMD) mod
GOLINT=golangci-lint
MOCKERY=mockery

# Tool versions
GOLANGCI_LINT_VERSION=v2.7.2
MOCKERY_VERSION=v3.6.1

# Build parameters
BINARY_NAME=snapshotter-example
EXAMPLES_DIR=./examples

# Test parameters
COVERAGE_FILE=coverage.out

all: fmt vet lint test build

## install-tools: Install required development tools
install-tools: install-golangci-lint install-mockery

## install-golangci-lint: Install golangci-lint if not present
install-golangci-lint:
	@which $(GOLINT) > /dev/null || { \
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."; \
		$(GOCMD) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	}

## install-mockery: Install mockery if not present
install-mockery:
	@which $(MOCKERY) > /dev/null || { \
		echo "Installing mockery $(MOCKERY_VERSION)..."; \
		$(GOCMD) install github.com/vektra/mockery/v3@$(MOCKERY_VERSION); \
	}

## generate: Generate mock files for testing.
generate: install-mockery
	$(MOCKERY)

## build: Build the example binary
build:
	$(GOBUILD) -o $(BINARY_NAME) $(EXAMPLES_DIR)/main.go

## test: Run all tests
test:
	$(GOTEST) -v -race ./...

## test-coverage: Run tests with coverage report
test-coverage:
	$(GOTEST) -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	$(GOCMD) tool cover -html=$(COVERAGE_FILE) -o coverage.html
	@echo "Coverage report generated: coverage.html"

## lint: Run linter (requires golangci-lint)
lint: install-golangci-lint
	$(GOLINT) run ./...

## fmt: Format Go source code
fmt:
	$(GOFMT) -s -w .

## fmt-check: Check if code is formatted
fmt-check:
	@test -z "$$($(GOFMT) -s -l . | tee /dev/stderr)" || (echo "Code is not formatted. Run 'make fmt'" && exit 1)

## vet: Run go vet
vet:
	$(GOVET) ./...

## mod-tidy: Tidy go modules
mod-tidy:
	$(GOMOD) tidy

## mod-download: Download go modules
mod-download:
	$(GOMOD) download

## mod-verify: Verify go modules
mod-verify:
	$(GOMOD) verify

## clean: Clean build artifacts
clean:
	rm -f $(BINARY_NAME)
	rm -f $(COVERAGE_FILE)
	rm -f coverage.html

## check: Run all checks (fmt-check, vet, lint, test)
check: fmt-check vet lint test

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
