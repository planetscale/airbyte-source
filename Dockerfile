# syntax=docker/dockerfile:1

ARG GO_VERSION=1.20.1
FROM golang:${GO_VERSION}-bullseye AS build

WORKDIR /airbyte-source
COPY . .

RUN go mod download
RUN go build -trimpath -o /connect

FROM debian:bullseye-slim

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y default-mysql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /connect /usr/local/bin/
ENV AIRBYTE_ENTRYPOINT "/usr/local/bin/connect"
ENTRYPOINT ["/usr/local/bin/connect"]
