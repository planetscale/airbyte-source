#! /bin/bash

TAG=$(git rev-parse --short HEAD)
docker build -t phanatic/private:${TAG} .

#docker push phanatic/private:${TAG}