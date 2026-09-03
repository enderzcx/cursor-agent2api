# syntax=docker/dockerfile:1.7
# One image = CLIProxyAPI host + the cursor-agent2api plugin, pinned to the same
# CLIProxyAPI version through go.mod so the plugin ABI always matches the host.
FROM golang:1.26-bookworm AS build

WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

# The plugin must be a dynamic library; the host resolves plugin id from the file name.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath -buildvcs=false -buildmode=c-shared \
      -o /out/plugins/cursor-agent2api.so ./cmd/cursor-agent2api \
    && rm -f /out/plugins/*.h

# Build the CLIProxyAPI server from the exact module version this repo depends on.
# -mod=mod lets Go resolve cmd/server-only dependencies that `go mod tidy` does
# not record for a library module.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -mod=mod -trimpath -buildvcs=false \
      -ldflags="-s -w -X 'main.Version=${VERSION}' -X 'main.Commit=${COMMIT}' -X 'main.BuildDate=${BUILD_DATE}'" \
      -o /out/CLIProxyAPI github.com/router-for-me/CLIProxyAPI/v7/cmd/server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/CLIProxyAPI /app/CLIProxyAPI
COPY --from=build /out/plugins /app/plugins
COPY deploy/config.yaml /app/config.template.yaml
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh && mkdir -p /data

ENV CA2A_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8317

ENTRYPOINT ["/app/entrypoint.sh"]
