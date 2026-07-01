build:
    mise exec -- sbt assembly

docker-build version="dev":
    docker build \
        -f .container/Dockerfile \
        --build-arg GITHUB_TOKEN="$GITHUB_TOKEN" \
        -t ghcr.io/sneaksanddata/arcane-stream-dynamodb:{{ version }} \
        .

clean:
    mise exec -- sbt clean

check:
    mise exec -- sbt scalafmtCheckAll

it: build
    #!/usr/bin/env bash
    set -euo pipefail

    # cleanup docker regardless of test outcome
    trap 'docker compose down' EXIT

    docker compose up -d
    docker compose wait prepare_buckets s3_reader_prepare lakekeeper_migrate lakekeeper_prepare
    mise exec --env it -- sbt test

stream debug="":
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f dev.env ]]; then
        echo "Missing dev.env, create it locally before running this command (see dev.env.example)." >&2
        exit 1
    fi

    log_level="INFO"
    if [[ "{{ debug }}" == "--debug" ]]; then
        log_level="DEBUG"
    elif [[ -n "{{ debug }}" ]]; then
        echo "Unknown stream option: {{ debug }}. Did you mean '--debug'?" >&2
        exit 1
    fi

    just build
    mise exec --env dev -- env STREAMCONTEXT__BACKFILL=${STREAMCONTEXT__BACKFILL:-false} java -DLOG_LEVEL="${log_level}" -Dlogback.configurationFile=src/main/resources/logback.xml -Dscala.concurrent.context.numThreads=2 -Dscala.concurrent.context.maxThreads=2 -jar target/com.sneaksanddata.arcane.stream-dynamodb.assembly.jar

backfill debug="":
    STREAMCONTEXT__BACKFILL=true just stream "{{ debug }}"

# ─── Kind / e2e ────────────────────────────────────────────────────────────────
#
# All recipes below assume `docker`, `kind`, `kubectl`, `helm`. The cluster name
# is overridable per-invocation:
#   KIND_CLUSTER=arcane-push-stream just kind-up      # share with arcane-ingestion-zio
#   KIND_CLUSTER=my-cluster         just kind-up
# Default matches arcane-ingestion-zio's default so `just kind-up` after
# `(cd ../arcane-ingestion-zio && just up)` lands in the same cluster.

KIND_CLUSTER  := env_var_or_default("KIND_CLUSTER", "arcane-push-stream")
KIND_IMAGE    := "arcane-stream-dynamodb"
KIND_TAG      := "latest"
HELM_RELEASE  := "arcane-stream-pull"

[doc("Build the plugin docker image with a local tag for kind")]
kind-build:
    #!/usr/bin/env bash
    set -euo pipefail
    # Prefer an explicit $GITHUB_TOKEN if exported, otherwise mint a short-lived
    # one from the gh CLI. Mirrors arcane-ingestion-zio's docker-build recipe and
    # avoids the token landing in shell history or persistent env config.
    token="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
    if [[ -z "${token}" ]]; then
        echo "No GITHUB_TOKEN available. Either export GITHUB_TOKEN or run 'gh auth login'." >&2
        exit 1
    fi
    # arcane-framework lives on maven.pkg.github.com, which requires read:packages.
    # gh's default login scopes don't include it; surface a clear remediation up
    # front rather than letting sbt fail with a generic 401 12 seconds in.
    scopes="$(gh api -i user 2>/dev/null | awk -F': ' 'tolower($1)=="x-oauth-scopes" {print $2}' | tr -d '\r')"
    if [[ -n "${scopes}" && ",${scopes//[[:space:]]/}," != *",read:packages,"* ]]; then
        echo "GitHub token is missing the 'read:packages' scope (needed for maven.pkg.github.com)." >&2
        echo "Current scopes: ${scopes}" >&2
        echo "Fix with:  gh auth refresh -s read:packages" >&2
        exit 1
    fi
    docker build \
        -f .container/Dockerfile \
        --build-arg GITHUB_TOKEN="${token}" \
        -t {{ KIND_IMAGE }}:{{ KIND_TAG }} \
        .

[doc("Create the kind cluster if it doesn't exist yet")]
kind-create:
    #!/usr/bin/env bash
    set -euo pipefail
    if kind get clusters | grep -qx "{{ KIND_CLUSTER }}"; then
        echo "kind cluster {{ KIND_CLUSTER }} already exists"
    else
        kind create cluster --name {{ KIND_CLUSTER }}
    fi

[doc("Apply the data-plane manifests (minio, postgres, lakekeeper, trino, dynamodb-local)")]
kind-data-up: kind-create
    kubectl --context kind-{{ KIND_CLUSTER }} apply -k deploy/k8s
    @echo "waiting for data plane to become ready..."
    kubectl --context kind-{{ KIND_CLUSTER }} wait --for=condition=available --timeout=300s \
        deploy/minio deploy/postgres deploy/lakekeeper deploy/trino deploy/dynamodb-local
    kubectl --context kind-{{ KIND_CLUSTER }} wait --for=condition=complete --timeout=300s \
        job/minio-buckets job/lakekeeper-migrate job/bootstrap-lakekeeper

[doc("Load the local plugin image into the kind cluster")]
kind-load: kind-build kind-create
    kind load docker-image {{ KIND_IMAGE }}:{{ KIND_TAG }} --name {{ KIND_CLUSTER }}

[doc("Install / upgrade the helm release into the kind cluster")]
kind-helm: kind-load
    helm --kube-context kind-{{ KIND_CLUSTER }} upgrade --install {{ HELM_RELEASE }} .helm \
        -f .helm/values.yaml -f .helm/values-dev.yaml

[doc("Full bring-up: cluster + data plane + plugin")]
kind-up: kind-data-up kind-helm && kind-info
    @echo "This will take a minute (or two...)"
    @echo "Set kubectl context:"
    @printf "\033[0;38;5;40mkubectl config use-context kind-%s\033[0m\n" "{{ KIND_CLUSTER }}"

[doc("Delete the kind cluster")]
kind-stop:
    -kind delete cluster --name {{ KIND_CLUSTER }}

[doc("Re-create the kind environment from scratch")]
kind-fresh: kind-stop kind-up

[doc("Port-forward MinIO to localhost (api :9000, console :9001). Ctrl-C to stop.")]
kind-port-forward-minio:
    @echo "MinIO API     -> http://localhost:9000"
    @echo "MinIO console -> http://localhost:9001  (minioadmin / minioadmin)"
    kubectl --context kind-{{ KIND_CLUSTER }} port-forward svc/minio 9000:9000 9001:9001

[doc("Show kind / helm status")]
kind-info:
    #!/usr/bin/env bash
    set +e
    echo -e "\033[1;4;97mstatus ({{ KIND_CLUSTER }}):\033[0m"
    echo -n "  kind cluster:         "
    if kind get clusters | grep -qx "{{ KIND_CLUSTER }}"; then echo "👌"; else echo "❌"; fi
    echo -n "  container image:      "
    if docker image inspect {{ KIND_IMAGE }}:{{ KIND_TAG }} >/dev/null 2>&1; then echo "👌"; else echo "❌"; fi
    echo -n "  image loaded to kind: "
    if docker exec {{ KIND_CLUSTER }}-control-plane crictl images -q docker.io/library/{{ KIND_IMAGE }}:{{ KIND_TAG }} 2>/dev/null | grep -q .; then
        echo "👌"
    else
        echo "❌"
    fi
    echo -n "  helm release:         "
    status=$(helm --kube-context kind-{{ KIND_CLUSTER }} status {{ HELM_RELEASE }} 2>/dev/null | awk '/^STATUS:/ {print $2}')
    if [ -n "$status" ]; then echo "👌 ($status)"; else echo "❌"; fi
    echo -n "  dev-runner pod:       "
    phase=$(kubectl --context kind-{{ KIND_CLUSTER }} get pod -l app.kubernetes.io/component=dev-runner -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
    if [ -n "$phase" ]; then echo "👌 ($phase)"; else echo "❌"; fi
