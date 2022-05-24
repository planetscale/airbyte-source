COMMIT := $(shell git rev-parse --short=7 HEAD 2>/dev/null)
VERSION := "0.1.3"
DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
NAME := "airbyte-source"
DOCKER_BUILD_PLATFORM := "linux/amd64"
ifeq ($(strip $(shell git status --porcelain 2>/dev/null)),)
  GIT_TREE_STATE=clean
else
  GIT_TREE_STATE=dirty
endif

BIN := bin
export GOPRIVATE := github.com/planetscale/*
export GOBIN := $(PWD)/$(BIN)

GO ?= go
GO_ENV ?= PS_LOG_LEVEL=debug PS_DEV_MODE=1 CGO_ENABLED=0
GO_RUN := env $(GO_ENV) $(GO) run

OS := $(shell uname)
PROTOC_VERSION=3.20.1
PROTOC_ARCH=x86_64
ifeq ($(OS),Linux)
	PROTOC_PLATFORM := linux
endif
ifeq ($(OS),Darwin)
	PROTOC_PLATFORM := osx
endif
PSDBCONNECT_PROTO_OUT := proto/psdbconnect

.PHONY: all
all: build test lint

.PHONY: test
test:
	@go test ./...

.PHONY: build
build:
	@go build ./...

.PHONY: lint
lint:
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	@staticcheck ./...

.PHONY: licensed
licensed:
	licensed cache
	licensed status

.PHONY: build-image
build-image:
	@echo "==> Building docker image ${REPO}/${NAME}:$(VERSION)"
	@docker build --platform ${DOCKER_BUILD_PLATFORM} --build-arg VERSION=$(VERSION:v%=%) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t ${REPO}/${NAME}:$(VERSION) .
	@docker tag ${REPO}/${NAME}:$(VERSION) ${REPO}/${NAME}:latest

.PHONY: push
push: build-image
	export REPO=$(REPO)
	@echo "==> Pushing docker image ${REPO}/${NAME}:$(VERSION)"
	@docker push ${REPO}/${NAME}:latest
	@docker push ${REPO}/${NAME}:$(VERSION)
	@echo "==> Your image is now available at $(REPO)/${NAME}:$(VERSION)"

.PHONY: clean
clean:
	@echo "==> Cleaning artifacts"
	@rm ${NAME}

proto: $(PSDBCONNECT_PROTO_OUT)/v1alpha1/psdbconnect.v1alpha1.pb.go
$(BIN):
	mkdir -p $(BIN)

$(BIN)/protoc-gen-go: | $(BIN)
	go install google.golang.org/protobuf/cmd/protoc-gen-go

$(BIN)/protoc-gen-go-grpc: | $(BIN)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

$(BIN)/protoc-gen-go-vtproto: | $(BIN)
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto

$(BIN)/protoc-gen-twirp: | $(BIN)
	go install github.com/twitchtv/twirp/protoc-gen-twirp

$(BIN)/protoc: | $(BIN)
	rm -rf tmp-protoc
	mkdir -p tmp-protoc
	wget -O tmp-protoc/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_PLATFORM)-$(PROTOC_ARCH).zip
	unzip -d tmp-protoc tmp-protoc/protoc.zip
	mv tmp-protoc/bin/protoc $(BIN)/
	rm -rf thirdparty/google/
	mv tmp-protoc/include/google/ thirdparty/
	rm -rf tmp-protoc

PROTO_TOOLS := $(BIN)/protoc $(BIN)/protoc-gen-go $(BIN)/protoc-gen-go-grpc $(BIN)/protoc-gen-go-vtproto $(BIN)/protoc-gen-twirp

$(PSDBCONNECT_PROTO_OUT)/v1alpha1/psdbconnect.v1alpha1.pb.go: $(PROTO_TOOLS) proto/psdbconnect.v1alpha1.proto
	mkdir -p $(PSDBCONNECT_PROTO_OUT)/v1alpha1
	$(BIN)/protoc \
	  --plugin=protoc-gen-go=$(BIN)/protoc-gen-go \
	  --plugin=protoc-gen-go-grpc=$(BIN)/protoc-gen-go-grpc \
	  --plugin=protoc-gen-go-vtproto=$(BIN)/protoc-gen-go-vtproto \
	  --go_out=$(PSDBCONNECT_PROTO_OUT)/v1alpha1 \
	  --go-grpc_out=$(PSDBCONNECT_PROTO_OUT)/v1alpha1 \
	  --go-vtproto_out=$(PSDBCONNECT_PROTO_OUT)/v1alpha1 \
	  --go_opt=paths=source_relative \
	  --go-grpc_opt=paths=source_relative \
	  --go-grpc_opt=require_unimplemented_servers=false \
	  --go-vtproto_opt=features=marshal+unmarshal+size \
	  --go-vtproto_opt=paths=source_relative \
	  -I thirdparty \
	  -I thirdparty/vitess/proto \
	  -I proto \
	  proto/psdbconnect.v1alpha1.proto
