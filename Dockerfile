# syntax=docker/dockerfile:1

ARG GO_VERSION=1.21.3
FROM --platform=$BUILDPLATFORM pscale.dev/wolfi-prod/go:${GO_VERSION} AS build
ARG TARGETOS TARGETARCH
WORKDIR /airbyte-source
COPY . .

RUN go mod download
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /connect

FROM pscale.dev/wolfi-prod/base:latest

COPY --from=build /connect /usr/local/bin/
ENV AIRBYTE_ENTRYPOINT "/usr/local/bin/connect"
ENTRYPOINT ["/usr/local/bin/connect"]
