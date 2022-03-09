# syntax=docker/dockerfile:1

ARG GO_VERSION=1.17
FROM golang:${GO_VERSION}-bullseye AS build

ARG GITHUB_TOKEN=unset
RUN --mount=type=secret,id=github_token \
    bash -c 'git config --global --add url."https://$(cat /run/secrets/github_token || echo ${GITHUB_TOKEN})@github.com/".insteadOf "https://github.com"'

ENV GOPRIVATE=github.com/planetscale/edge-gateway,github.com/planetscale/log

WORKDIR /airbyte-source
COPY . .

RUN go mod download
RUN go build -o /connect

FROM debian:bullseye-slim

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y default-mysql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /connect /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/connect"]
