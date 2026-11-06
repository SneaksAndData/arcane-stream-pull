#!/usr/bin/env bash

set -u -o pipefail -x

curl -w '\n\n[%{http_code}]\n' -X POST http://localhost:8085/api/v2/delivery/data \
-H 'Content-Type: application/json' \
-d '{"event_id":{"string":"e1"},"payload":{"source":"curl","sequence":1,"amount":9.99,"settled":true},"tags":["live"]}'
