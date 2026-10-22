#!/bin/sh
cd ..
DIRECTORIES="./"

"$(go env GOPATH)/bin/swag" init --pd --st -g main.go --dir "${DIRECTORIES}" -o docs
