# syntax=docker/dockerfile:1

ARG GO_VERSION=1.17
FROM golang:${GO_VERSION}-bullseye AS build

WORKDIR /airbyte-source
COPY . .

RUN go build -o /connect

FROM debian:bullseye-slim

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y default-mysql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

#RUN groupadd -r airbyteplanetscalesource && useradd -r -m -g airbyteplanetscalesource airbyteplanetscalesource && \
#    mkdir -p /etc/airbyteplanetscalesourcesingularity && chown airbyteplanetscalesource:airbyteplanetscalesource /etc/airbyteplanetscalesource

COPY --from=build /connect /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/connect"]
#USER airbyteplanetscalesource