#!/bin/sh
set -e

APP_NAME="ba-cpe-profile-dispatcher"

go get -u github.com/googleapis/googleapis || true
go get -u -d github.com/maxkondr/ba-proto || true

# build CPE profile Golang stubs
protoc --proto_path=$GOPATH/src/github.com/googleapis/googleapis \
    --proto_path=$GOPATH/src/github.com/maxkondr/ba-proto \
    --go_out=plugins=grpc:$GOPATH/src/ \
    $GOPATH/src/github.com/maxkondr/ba-proto/cpeProfile/cpe-profile.proto

# build CPE profile dispatcher Golang stubs
protoc --proto_path=$GOPATH/src/github.com/googleapis/googleapis \
    --proto_path=$GOPATH/src/github.com/maxkondr/ba-proto \
    --go_out=plugins=grpc:$GOPATH/src/\
    $GOPATH/src/github.com/maxkondr/ba-proto/cpeProfileDispatcher/cpe-profile-dispatcher.proto
    
CGO_ENABLED=0 GOOS=linux go build -ldflags '-w -s' -a -o $APP_NAME .
