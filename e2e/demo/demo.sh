#!/usr/bin/env bash
# Local demo driver for arcane-push-stream + arcane-stream-pull on kind.
#
#   ./demo.sh up        full bring-up (cluster, data plane, both apps, DataRoute, forwards)
#   ./demo.sh forward   (re)start port-forwards only
#   ./demo.sh send [n]  POST n demo events through push-stream (default 1)
#   ./demo.sh query     run the demo SQL against trino
#   ./demo.sh status    show what is and isn't healthy
#   ./demo.sh logs <push|pull|operator|trino>
#   ./demo.sh reset     back to the empty starting state (no CRs, tables, events, S2 data)
#   ./demo.sh down      delete the kind cluster and stop forwards
#
# Design notes:
#   * The data plane (s2, postgres, lakekeeper, trino, dynamodb-local, bootstrap)
#     is NOT redefined here. It is applied from ../../deploy/k8s via kustomize,
#     which is the same bundle `just kind-data-up` uses. One source of truth.
#   * Both app images are built locally and `kind load`ed. Neither chart's dev
#     values point at a registry tag that exists remotely.
#   * Port-forwards are started with nohup and tracked in a pidfile. Backgrounding
#     them inside a script that then exits kills them instantly.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-arcane-push-stream}"
CTX="kind-${CLUSTER}"

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PULL_REPO="$(cd "${DEMO_DIR}/../.." && pwd)"
PUSH_REPO="${PUSH_REPO:-$(cd "${PULL_REPO}/../arcane-push-stream" 2>/dev/null && pwd || true)}"

PUSH_RELEASE="arcane-ingestion"      # matches arcane-push-stream/justfile
PULL_RELEASE="arcane-stream-pull"    # matches arcane-stream-pull/justfile
CRD_VERSION="v0.0.11"                # oci://ghcr.io/sneaksanddata/helm/arcane-crd
OPERATOR_VERSION="v1.2.0"            # oci://ghcr.io/sneaksanddata/helm/arcane-operator
PUSH_IMAGE="arcane-push-stream:latest"
PULL_IMAGE="arcane-stream-pull:latest"

# Keep in sync with dataroute.yaml spec.consumerId and values-pull-stream-demo.yaml
# source.configuration.pullIndexValue.
PRODUCER="producer1"
API_VERSION="v2"                     # settings.router.apiVersion in the push chart
TARGET_TABLE="iceberg.arcane_pull_test.events"

# host:service ports. Push-stream's Service port is 8085 (settings.server.port),
# lakekeeper is 8181, trino 8080, s2 console 9001.
PF_SPEC=(
  "8000:svc/dynamodb-local:8000:DynamoDB Local"
  "8080:svc/trino:8080:Trino UI + JDBC"
  "8085:svc/arcane-push-stream:8085:push-stream HTTP API"
  "8181:svc/lakekeeper:8181:Lakekeeper UI + REST catalog"
  "9000:svc/s2:9000:S2 S3 API (s2.localhost:9000, used by Lakekeeper UI preview)"
  "9001:svc/s2:9001:S2 console (minioadmin/minioadmin)"
)
PF_PIDFILE="${TMPDIR:-/tmp}/arcane-demo-portforward.pids"

c_reset=$'\033[0m'; c_bold=$'\033[1m'; c_green=$'\033[0;32m'
c_red=$'\033[0;31m'; c_yellow=$'\033[0;33m'; c_dim=$'\033[2m'

say()  { printf '%s==>%s %s\n' "${c_bold}" "${c_reset}" "$*"; }
ok()   { printf '  %s+%s %s\n' "${c_green}" "${c_reset}" "$*"; }
warn() { printf '  %s!%s %s\n' "${c_yellow}" "${c_reset}" "$*"; }
die()  { printf '  %sx%s %s\n' "${c_red}" "${c_reset}" "$*" >&2; exit 1; }

k() { kubectl --context "${CTX}" "$@"; }

require_tools() {
  local missing=()
  for t in docker kind kubectl helm; do command -v "$t" >/dev/null || missing+=("$t"); done
  [[ ${#missing[@]} -eq 0 ]] || die "missing required tools: ${missing[*]}"
}

require_push_repo() {
  [[ -n "${PUSH_REPO}" && -d "${PUSH_REPO}/.helm" ]] \
    || die "arcane-push-stream checkout not found. Set PUSH_REPO=/path/to/arcane-push-stream"
}

# ─── build ────────────────────────────────────────────────────────────────────

github_token() {
  local t="${GITHUB_TOKEN:-}"
  [[ -n "$t" ]] || t="$(gh auth token 2>/dev/null || true)"
  [[ -n "$t" ]] || die "No GITHUB_TOKEN. Export it or run 'gh auth login' (needs read:packages for maven.pkg.github.com)."
  printf '%s' "$t"
}

build_images() {
  local token; token="$(github_token)"

  if docker image inspect "${PUSH_IMAGE}" >/dev/null 2>&1 && [[ "${REBUILD:-}" != "1" ]]; then
    ok "${PUSH_IMAGE} present (REBUILD=1 to force)"
  else
    say "building ${PUSH_IMAGE}"
    docker build --build-arg GITHUB_TOKEN="${token}" \
      -f "${PUSH_REPO}/.container/Dockerfile" -t "${PUSH_IMAGE}" "${PUSH_REPO}"
  fi

  if docker image inspect "${PULL_IMAGE}" >/dev/null 2>&1 && [[ "${REBUILD:-}" != "1" ]]; then
    ok "${PULL_IMAGE} present (REBUILD=1 to force)"
  else
    say "building ${PULL_IMAGE}"
    docker build --build-arg GITHUB_TOKEN="${token}" \
      -f "${PULL_REPO}/.container/Dockerfile" -t "${PULL_IMAGE}" "${PULL_REPO}"
  fi
}

# ─── cluster + data plane ─────────────────────────────────────────────────────

create_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
    ok "kind cluster ${CLUSTER} exists"
  else
    say "creating kind cluster ${CLUSTER}"
    kind create cluster --name "${CLUSTER}"
  fi
  k cluster-info >/dev/null || die "cluster ${CLUSTER} unreachable"
}

load_images() {
  say "loading images into kind"
  kind load docker-image "${PUSH_IMAGE}" --name "${CLUSTER}"
  kind load docker-image "${PULL_IMAGE}" --name "${CLUSTER}"
}

data_plane_up() {
  # e2e/demo is a kustomize overlay over ../../deploy/k8s: same data plane the
  # `just kind-data-up` recipe uses, plus the lakekeeper BASE_URI patch that makes
  # its web UI usable through port-forward. See kustomization.yaml for why.
  say "applying data plane (s2, postgres, lakekeeper, trino, dynamodb-local)"
  k apply -k "${DEMO_DIR}"

  say "waiting for data plane deployments (this is the slow part, ~2-4 min cold)"
  k wait --for=condition=available --timeout=420s \
    deploy/s2 deploy/postgres deploy/lakekeeper deploy/trino deploy/dynamodb-local

  say "waiting for lakekeeper migrate + warehouse bootstrap"
  k wait --for=condition=complete --timeout=420s \
    job/lakekeeper-migrate job/bootstrap-lakekeeper
  ok "data plane ready"
  browser_storage_endpoint
}

# Lakekeeper gives every client, the browser included, the warehouse's S3 endpoint.
# The UI previews tables with DuckDB-WASM reading parquet straight from S2, so that
# endpoint must work from the host and from inside the cluster. bootstrap.yaml
# registers http://s2:9000, which only resolves in-cluster. s2.localhost resolves to
# 127.0.0.1 on the host with no hosts-file edit (RFC 6761), reaching the S2
# port-forward on :9000; the CoreDNS rewrite makes the same name resolve to the s2
# Service in-cluster. Tables keep their s3://tmp/... locations, so nothing moves.
browser_storage_endpoint() {
  say "pointing the warehouse at s2.localhost:9000 (browser + in-cluster)"
  k apply -f "${DEMO_DIR}/resources/coredns-s2-localhost.yaml" >/dev/null
  k -n kube-system rollout restart deploy/coredns >/dev/null
  k -n kube-system rollout status deploy/coredns --timeout=90s >/dev/null

  local lk wid code
  lk="http://localhost:18181"
  kubectl --context "${CTX}" port-forward svc/lakekeeper 18181:8181 >/dev/null 2>&1 &
  local pf=$!
  sleep 3
  wid="$(curl -fsS "${lk}/management/v1/warehouse" | sed -n 's/.*"warehouse-id":"\([^"]*\)".*/\1/p;s/.*"id":"\([^"]*\)".*/\1/p' | head -1)"
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${lk}/management/v1/warehouse/${wid}/storage" \
    -H 'Content-Type: application/json' -d @"${DEMO_DIR}/resources/warehouse-storage.json" || true)"
  kill "${pf}" 2>/dev/null || true
  [[ "${code}" == "200" ]] && ok "warehouse endpoint is http://s2.localhost:9000" \
    || warn "could not update warehouse storage (HTTP ${code}); UI table preview will not work"
}

# ─── apps ─────────────────────────────────────────────────────────────────────

push_stream_up() {
  say "installing ${PUSH_RELEASE} (arcane-push-stream)"
  helm --kube-context "${CTX}" upgrade --install "${PUSH_RELEASE}" "${PUSH_REPO}/.helm" \
    -f "${PUSH_REPO}/.helm/values.yaml" \
    -f "${PUSH_REPO}/.helm/values-dev.yaml" \
    -f "${DEMO_DIR}/values-push-stream-demo.yaml" \
    --wait --timeout 5m
  ok "push-stream rolled out"
}

apply_dataroute() {
  say "applying DataRoute ${PRODUCER}"
  k apply -f "${DEMO_DIR}/dataroute.yaml"

  # push-stream watches the CR and provisions the iceberg table lazily. Surface
  # that here rather than letting the pull-stream init container time out opaquely.
  say "waiting for push-stream to provision ${TARGET_TABLE}"
  local deadline=$(( $(date +%s) + 300 ))
  until k run trino-probe-$$ --rm -i --restart=Never --image=trinodb/trino:474 -- \
          trino --server http://trino:8080 --user probe \
          --execute "DESCRIBE ${TARGET_TABLE}" >/dev/null 2>&1; do
    if (( $(date +%s) >= deadline )); then
      warn "table not visible yet; check: ./demo.sh logs push"
      return 1
    fi
    sleep 10
  done
  ok "iceberg table ${TARGET_TABLE} exists"
}

# Put one event in DynamoDB before the pull-stream ever starts.
#
# PullStreamWatermark.epoch is `Instant.now()`, NOT 1970 (despite the name), and
# PullStreamingSource.getMaxTimestamp falls back to it whenever the DynamoDB query
# returns no items. So against an empty source table the in-memory watermark
# ratchets forward to "now" on every change-capture tick while the stored
# watermark stays at 1970:
#
#   Watermark version changed from 1793718727 to 1793718729   (0 items)
#
# The read query is `#wm > :t` and getLatestWatermarkInRange returns the end of
# the range verbatim, so any event whose timestamp lands in the epoch-second the
# watermark already reached is never read: version equal is not greater, and the
# loop reports "No changes in watermark version" forever. The rows sit in DynamoDB
# and are skipped permanently.
#
# Seeding one row first means getMaxTimestamp always has a real item to return,
# so the watermark is anchored to actual data instead of wall-clock time, and the
# seed row itself is picked up by the first real iteration.
seed_source() {
  say "seeding one event so the source table is non-empty"
  if send_events 1 >/dev/null 2>&1; then
    ok "seed event accepted"
  else
    warn "seed event failed; first batch after startup may be skipped"
  fi
}

# The operator turns every PullStream into a Job. It needs the shared CRDs first,
# plus RBAC on PullStreams/Events that its chart does not grant (applied by the
# kustomize overlay in operator-rbac.yaml, which runs before this).
operator_up() {
  say "installing arcane-crd ${CRD_VERSION} and arcane-operator ${OPERATOR_VERSION}"
  helm --kube-context "${CTX}" upgrade --install arcane-crd \
    oci://ghcr.io/sneaksanddata/helm/arcane-crd --version "${CRD_VERSION}" \
    -n arcane --create-namespace --wait
  helm --kube-context "${CTX}" upgrade --install arcane-operator \
    oci://ghcr.io/sneaksanddata/helm/arcane-operator --version "${OPERATOR_VERSION}" \
    -n arcane -f "${DEMO_DIR}/values-operator-demo.yaml" --wait --timeout 3m
  ok "operator running"
}

pull_stream_up() {
  say "installing ${PULL_RELEASE} (StreamClass + StreamingJobTemplate)"
  helm --kube-context "${CTX}" upgrade --install "${PULL_RELEASE}" "${PULL_REPO}/.helm" \
    -f "${PULL_REPO}/.helm/values.yaml" \
    -f "${PULL_REPO}/.helm/values-dev.yaml" \
    -f "${DEMO_DIR}/values-pull-stream-demo.yaml" \
    --wait --timeout 8m
  ok "pull-stream chart installed"
}

# Provision the flat (no variant) route, seed it, then create the PullStream.
# Seeding first matters for the same watermark reason as seed_source.
# The pull job's staging tables live in `staging`, and the push-stream target tables in
# `arcane_pull_test`. bootstrap.yaml only creates `test` and `arcane_pull_test`, and
# anyone clearing namespaces in the Lakekeeper UI would take them away, which
# makes the pull job die at start-up with "Namespace ... does not exist".
ensure_namespaces() {
  local lk="${1:-http://localhost:8181}" wid ns
  wid="$(curl -fsS "${lk}/management/v1/warehouse" | jq -r '.warehouses[0].id')"
  for ns in staging arcane_pull_test; do
    curl -sS -o /dev/null -X POST "${lk}/catalog/v1/${wid}/namespaces" \
      -H 'Content-Type: application/json' -d "{\"namespace\":[\"${ns}\"],\"properties\":{}}" || true
  done
}

pull_stream_apply() {
  say "applying DataRoute delivery and PullStream delivery"
  ensure_namespaces
  k apply -f "${DEMO_DIR}/dataroute-no-variant.yaml"
  local deadline=$(( $(date +%s) + 300 ))
  until k run trino-probe-$$ --rm -i --restart=Never --image=trinodb/trino:474 -- \
          trino --server http://trino:8080 --user probe \
          --execute "DESCRIBE iceberg.arcane_pull_test.delivery" >/dev/null 2>&1; do
    (( $(date +%s) < deadline )) || { warn "delivery table not visible yet"; return 1; }
    sleep 10
  done
  curl -sS -X POST "http://localhost:8085/api/${API_VERSION}/delivery/data" \
    -H 'Content-Type: application/json' \
    -d '{"event_id":{"string":"seed"},"payload":{"source":"seed","sequence":0,"amount":1.0,"settled":false},"tags":["seed"]}' >/dev/null \
    && ok "seed event accepted" || warn "seed event failed"
  k apply -f "${DEMO_DIR}/pull-stream-no-variant.yaml"
  ok "PullStream delivery created; the operator starts its job"
}

# ─── port-forwards ────────────────────────────────────────────────────────────

stop_forwards() {
  if [[ -f "${PF_PIDFILE}" ]]; then
    while read -r pid _; do
      [[ -n "${pid:-}" ]] && kill "${pid}" 2>/dev/null || true
    done < "${PF_PIDFILE}"
    rm -f "${PF_PIDFILE}"
  fi
}

start_forwards() {
  stop_forwards
  say "starting port-forwards"
  : > "${PF_PIDFILE}"
  local entry host target port label
  for entry in "${PF_SPEC[@]}"; do
    IFS=: read -r host target port label <<< "${entry}"

    if lsof -nP -iTCP:"${host}" -sTCP:LISTEN >/dev/null 2>&1; then
      warn "localhost:${host} already in use, skipping ${label}"
      continue
    fi
    # nohup + disown: a plain background job dies with the script.
    nohup kubectl --context "${CTX}" port-forward "${target}" "${host}:${port}" \
      >"${TMPDIR:-/tmp}/arcane-demo-pf-${host}.log" 2>&1 &
    echo "$! ${host} ${label}" >> "${PF_PIDFILE}"
    disown || true
  done

  sleep 3
  local pid host2 rest
  while read -r pid host2 rest; do
    if kill -0 "${pid}" 2>/dev/null; then
      ok "localhost:${host2} -> ${rest}"
    else
      warn "forward on ${host2} (${rest}) died; see ${TMPDIR:-/tmp}/arcane-demo-pf-${host2}.log"
    fi
  done < "${PF_PIDFILE}"
}

# Back to the starting state of the recording: infrastructure up, nothing deployed.
# Deletes the PullStream and DataRoutes, drops the Iceberg tables (purging their S2
# files), and empties the DynamoDB source table. push-stream is restarted last because it
# remembers which producers it has provisioned and would not re-create a table
# when the DataRoute is applied again.
reset() {
  say "resetting demo to its initial state"
  local lk="http://localhost:8181" ddb="http://localhost:8000" wid ns t key
  k delete pullstream --all --wait=true --timeout=90s >/dev/null
  k delete dataroute --all --wait=true --timeout=90s >/dev/null
  ok "PullStream and DataRoute CRs deleted"

  wid="$(curl -fsS "${lk}/management/v1/warehouse" | jq -r '.warehouses[0].id')"
  for ns in $(curl -fsS "${lk}/catalog/v1/${wid}/namespaces" | jq -r '.namespaces[][0]'); do
    for t in $(curl -fsS "${lk}/catalog/v1/${wid}/namespaces/${ns}/tables" | jq -r '.identifiers[].name'); do
      curl -fsS -o /dev/null -X DELETE "${lk}/catalog/v1/${wid}/namespaces/${ns}/tables/${t}?purgeRequested=true"
    done
  done
  ok "Iceberg tables dropped"

  export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_REGION=us-east-1
  aws dynamodb scan --endpoint-url "${ddb}" --table-name "${DDB_TABLE:-arcane_ingestion}" \
      --expression-attribute-names '{"#p":"producer","#t":"timestampUTC"}' --projection-expression '#p, #t' \
    | jq -c '.Items[]' | while read -r key; do
        aws dynamodb delete-item --endpoint-url "${ddb}" --table-name "${DDB_TABLE:-arcane_ingestion}" --key "${key}"
      done
  ok "DynamoDB table emptied"

  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    aws s3 rm --endpoint-url http://localhost:9000 s3://tmp --recursive --only-show-errors || true
  ok "S2 bucket tmp emptied"

  ensure_namespaces
  k rollout restart deploy/arcane-push-stream >/dev/null
  k rollout status deploy/arcane-push-stream --timeout=120s >/dev/null
  start_forwards
  ok "reset done; apply the DataRoute and PullStream"
}

# ─── demo actions ─────────────────────────────────────────────────────────────

send_events() {
  local count="${1:-1}" url="http://localhost:8085/api/${API_VERSION}/${PRODUCER}/data"
  say "POSTing ${count} event(s) to ${url}"
  local i seq body code failed=0
  for (( i=1; i<=count; i++ )); do
    seq=$(( RANDOM % 100000 ))
    # Shape must satisfy payloadSchema in dataroute.yaml: every declared field present.
    body=$(cat <<JSON
{
  "event_id": "evt-$(date +%s)-${i}",
  "payload": {
    "source": "demo-cli",
    "sequence": ${seq},
    "amount": $(( RANDOM % 500 )).$(( RANDOM % 90 + 10 )),
    "settled": $( (( RANDOM % 2 )) && echo true || echo false ),
    "attributes": { "region": "eu-central-1", "channel": "terminal" }
  },
  "tags": ["demo", "batch-${i}"]
}
JSON
)
    code=$(curl -sS -o /tmp/arcane-demo-resp.txt -w '%{http_code}' \
      -X POST "${url}" -H 'Content-Type: application/json' -d "${body}") || true
    if [[ "${code}" == "202" || "${code}" == "200" ]]; then
      ok "[${i}/${count}] ${code} $(cat /tmp/arcane-demo-resp.txt)"
    else
      warn "[${i}/${count}] HTTP ${code}: $(cat /tmp/arcane-demo-resp.txt)"
      failed=$(( failed + 1 ))
    fi
  done
  printf '\n  %sMERGE lands within ~2s (changeCaptureInterval). Then: ./demo.sh query%s\n' \
    "${c_dim}" "${c_reset}"
  return $(( failed > 0 ? 1 : 0 ))
}

query() {
  say "querying ${TARGET_TABLE} via trino"
  local sql="${1:-}"
  if [[ -z "${sql}" ]]; then
    sql="SELECT source, sequence, amount, settled, attributes, timestampUTC
         FROM ${TARGET_TABLE} ORDER BY timestampUTC DESC LIMIT 20"
  fi
  k run trino-cli-$$ --rm -i --restart=Never --image=trinodb/trino:474 -- \
    trino --server http://trino:8080 --user demo --execute "${sql}"
}

status() {
  printf '%sdemo status (%s)%s\n' "${c_bold}" "${CLUSTER}" "${c_reset}"

  printf '  %-26s' 'kind cluster:'
  kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" && echo "ok" || echo "missing"

  if ! k cluster-info >/dev/null 2>&1; then
    warn "cluster unreachable; run ./demo.sh up"
    return
  fi

  local d
  for d in s2 postgres lakekeeper trino dynamodb-local arcane-push-stream; do
    printf '  %-26s' "${d}:"
    k get deploy "${d}" -o jsonpath='{.status.readyReplicas}/{.spec.replicas} ready' 2>/dev/null \
      || printf 'absent'
    echo
  done

  printf '  %-26s' 'arcane-operator:'
  kubectl --context "${CTX}" -n arcane get deploy arcane-operator \
    -o jsonpath='{.status.readyReplicas}/{.spec.replicas} ready' 2>/dev/null || printf 'absent'
  echo

  printf '  %-26s' 'PullStream delivery:'
  k get pullstream delivery -o jsonpath='{.status.phase}' 2>/dev/null || printf 'absent'
  echo

  printf '  %-26s' 'DataRoute:'
  k get dataroute "${PRODUCER}" -o jsonpath='{.spec.consumerId}' 2>/dev/null || printf 'absent'
  echo

  printf '\n  %sport-forwards%s\n' "${c_bold}" "${c_reset}"
  if [[ -f "${PF_PIDFILE}" ]]; then
    local pid host rest
    while read -r pid host rest; do
      if kill -0 "${pid}" 2>/dev/null; then ok "localhost:${host} -> ${rest}"
      else warn "localhost:${host} -> ${rest} (dead)"; fi
    done < "${PF_PIDFILE}"
  else
    warn "none running; ./demo.sh forward"
  fi
}

logs() {
  case "${1:-}" in
    push)  k logs -f deploy/arcane-push-stream ;;
    pull)  k logs -f -l job-name=delivery --all-containers --tail=200 ;;
    operator) kubectl --context "${CTX}" -n arcane logs -f deploy/arcane-operator ;;
    trino) k logs -f deploy/trino ;;
    *)     die "usage: ./demo.sh logs <push|pull|operator|trino>" ;;
  esac
}

summary() {
  cat <<EOF

$(printf '%s' "${c_bold}")Demo is up.$(printf '%s' "${c_reset}")

  Trino UI          http://localhost:8080        (any username, no password)
  Lakekeeper UI     http://localhost:8181        warehouse "demo" > arcane_pull_test > events
  S2 console        http://localhost:9001        minioadmin / minioadmin
  push-stream API   http://localhost:8085

Demo flow:

  1. Show the CRD that drives everything
       kubectl get dataroute ${PRODUCER} -o yaml

  2. Send data from the terminal
       ./demo.sh send 5

     or by hand:
       curl -X POST http://localhost:8085/api/${API_VERSION}/${PRODUCER}/data \\
         -H 'Content-Type: application/json' \\
         -d '{"event_id":"e1","payload":{"source":"curl","sequence":1,"amount":9.99,"settled":true,"attributes":{"k":"v"}},"tags":["live"]}'

  3. Query it through Trino (~2s after sending)
       ./demo.sh query

  4. Show the Iceberg table in Lakekeeper UI, and the parquet files in S2

  ./demo.sh status | logs <push|pull|operator|trino> | down

EOF
}

up() {
  require_tools
  require_push_repo
  build_images
  create_cluster
  load_images
  data_plane_up
  push_stream_up
  apply_dataroute || warn "continuing; pull-stream will keep waiting for the table"
  # Forwards come up before the pull-stream so the seed event can be POSTed
  # through localhost:8085. See seed_source for why seeding matters.
  start_forwards
  seed_source
  operator_up
  pull_stream_up
  pull_stream_apply || warn "PullStream not created; see ./demo.sh logs push"
  summary
}

down() {
  stop_forwards
  say "deleting kind cluster ${CLUSTER}"
  kind delete cluster --name "${CLUSTER}" || true
  ok "done"
}

case "${1:-up}" in
  up)      up ;;
  forward) start_forwards ;;
  send)    send_events "${2:-1}" ;;
  query)   query "${2:-}" ;;
  status)  status ;;
  logs)    logs "${2:-}" ;;
  reset)   reset ;;
  down)    down ;;
  *)       sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
esac
