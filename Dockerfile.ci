# syntax=docker/dockerfile:1

ARG GO_VERSION=1.18rc1
FROM golang:${GO_VERSION}-bullseye AS build

ARG GITHUB_TOKEN=unset

RUN --mount=type=secret,id=github_token \
    bash -c 'git config --global --add url."https://$(echo ${GITHUB_TOKEN})@github.com/".insteadOf "https://github.com"'

RUN go env -w GOPRIVATE=github.com/planetscale/*

WORKDIR /airbyte-source
COPY . .

RUN go mod download
RUN go build -o /connect
