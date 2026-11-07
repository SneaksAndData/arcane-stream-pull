#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_REGION=us-east-1

set -euo pipefail
aws dynamodb scan --table-name arcane_ingestion --endpoint-url http://localhost:8000 --output json | jq
