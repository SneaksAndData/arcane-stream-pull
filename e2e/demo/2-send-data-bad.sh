#!/usr/bin/env bash

set -u -o pipefail -x

# We change a boolean value to an integer
curl -w '\n\n[%{http_code}]\n' -X POST http://localhost:8085/api/v2/delivery/data \
-H 'Content-Type: application/json' \
-d '{"event_id":"e1","payload":{"source":"curl","sequence":1,"amount":9.99,"settled":1,"attributes":{"k":"v"}},"tags":["live"]}'
