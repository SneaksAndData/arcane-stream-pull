#!/usr/bin/env bash

set -u -o pipefail -x

kubectl run trino-cli-$$ --rm -i --restart=Never \
  --image=trinodb/trino:484 -- \
  trino --server http://trino:8080 --user demo \
  --execute 'SELECT source, sequence, amount, settled, attributes, timestampUTC
             FROM iceberg.arcane_pull_test.events
             ORDER BY timestampUTC DESC
             LIMIT 20'
