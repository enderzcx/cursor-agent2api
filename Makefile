PLUGIN_ID ?= cursor-agent2api
GO ?= go
CGO_ENABLED ?= 1
VERSION ?= dev
IMAGE ?= ghcr.io/enderzcx/cursor-agent2api

ifeq ($(shell uname -s),Darwin)
PLUGIN_EXT := dylib
else
PLUGIN_EXT := so
endif

.PHONY: test plugin host dist proto tidy fmt docker run

test:
	$(GO) test ./cursoragentv1/... ./internal/... ./cmd/...

# Native CLIProxyAPI plugin. The host derives the plugin id from the file name,
# so keep it exactly $(PLUGIN_ID).<ext> (a "-v<digits>" suffix would be parsed as a version).
plugin:
	mkdir -p dist
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -buildmode=c-shared -o dist/$(PLUGIN_ID).$(PLUGIN_EXT) ./cmd/cursor-agent2api
	rm -f dist/$(PLUGIN_ID).h

# CLIProxyAPI server built from the pinned module version. -mod=mod is required
# because cmd/server pulls dependencies this library module does not list.
host:
	mkdir -p dist
	CGO_ENABLED=1 $(GO) build -mod=mod -trimpath -buildvcs=false -ldflags "-s -w -X 'main.Version=$(VERSION)'" -o dist/CLIProxyAPI github.com/router-for-me/CLIProxyAPI/v7/cmd/server

dist: plugin host
	cp deploy/config.yaml dist/config.template.yaml

# Local run against ./dist using a data dir at ./data (mirrors the Docker layout).
run: dist
	CA2A_DATA_DIR=$(CURDIR)/data PLUGIN_DIR=$(CURDIR)/dist bash deploy/run-local.sh

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

proto:
	bash cursoragentv1/proto/check.sh

tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...
