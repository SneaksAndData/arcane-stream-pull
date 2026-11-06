# Local demo: push-stream to Iceberg, queried through Trino

Runs `arcane-push-stream` and `arcane-stream-pull` together in a kind cluster, so
a `curl` POST ends up as a row in an Iceberg table you can query in Trino and
browse in the Lakekeeper UI.

```
curl -> arcane-push-stream -> DynamoDB -> arcane-stream-pull job -> Iceberg (S2) -> Trino
                  |                                  |
                  +-- creates the target table       +-- started by arcane-operator
                      from the DataRoute CR              from a PullStream CR
```

# for the impatient - how to run the demo?
Read this section before running the commands, to take the most out of it and
understand what the demo shows you.
```sh
./demo.sh
./demo.sh reset                      # no CRs, no tables, empty DynamoDB and S2
```

To create the dataroute for push-stream (accept http data), apply the data-route crd:
```sh
kubectl apply -f dataroute-no-variant.yaml    # push-stream creates the Iceberg table
```

To start pulling the data (so it shows up in lakekeeper), apply the pull-stream CRD:
```sh
kubectl apply -f pull-stream-no-variant.yaml  # the operator starts the pull job
```

the `no-variant` suffix is there, because there are demo resources to test with
'VARIANT' type.


Use the numbered helper scripts to:
- send data
- send bad data
- check how you can use SQL to query iceberg table (via Trino)
- check how the data look like inside DynamoDB


The table exists and is empty. Wait until `kubectl get pods` shows the `delivery-*`
pod `Running` and give it about 30 seconds before the first event. An event written in
the same second the job starts its first watermark check is skipped and never
arrives. Then `./1-send-data.sh` (or the `delivery` curl in it) and
`./3-query-data-trino.sh`. `./demo.sh reset` returns to the start.

Staging tables for the pull job go in the `staging` Iceberg namespace (the target
table is in `arcane_pull_test`). The namespace must exist before the pull job starts, or
the job dies at start-up with `Namespace ... does not exist` and the PullStream goes
to `Failed`. `./demo.sh reset` and `up` create it.

---

# Prerequisites

- `docker`, `kind`, `kubectl`, `helm`
- An `arcane-push-stream` checkout next to this repo (or `PUSH_REPO=/path/to/it`)
- A GitHub token with `read:packages` (`GITHUB_TOKEN` or `gh auth login`) for the
  image builds

## Usage

```sh
./demo.sh up          # cluster, data plane, both apps, DataRoute, port-forwards
./demo.sh send 5      # POST 5 events
./demo.sh query       # read them back through Trino
./demo.sh status      # what is and isn't healthy
./demo.sh logs pull   # or: push | operator | trino
./demo.sh down        # delete the cluster, stop forwards
```

`up` takes roughly 5-10 minutes cold, most of it image builds. Both images are
reused if already present; `REBUILD=1 ./demo.sh up` forces a rebuild.

### Ports

| URL | What |
|---|---|
| http://localhost:8085 | push-stream HTTP API (`POST /api/v2/producer1/data`) |
| http://localhost:8080 | Trino UI and JDBC (any username, no password) |
| http://localhost:8181 | Lakekeeper UI — warehouse `demo` > `arcane_pull_test` > `events` |
| http://localhost:9001 | S2 console, `minioadmin` / `minioadmin` |

## The demo

1. Show the single CR that drives everything: `kubectl get dataroute producer1 -o yaml`.
   It defines the HTTP route, the payload schema, and the Iceberg table.
2. `./demo.sh send 3`, or by hand:
   ```sh
   curl -X POST http://localhost:8085/api/v2/producer1/data \
     -H 'Content-Type: application/json' \
     -d '{"event_id":"e1","payload":{"source":"curl","sequence":1,"amount":9.99,"settled":true,"attributes":{"k":"v"}},"tags":["live"]}'
   ```
3. `./demo.sh query` — rows appear within a few seconds.
4. Open the Lakekeeper UI for the table and schema, and the S2 console for the
   parquet files underneath.

## Files

| File | Purpose |
|---|---|
| `demo.sh` | The driver. All subcommands live here. |
| `kustomization.yaml` | Overlay on `../../deploy/k8s` (the shared data plane). |
| `resources/pull-stream-logback.yaml` | Logback config for the pull-stream job. |
| `resources/pull-stream-config.yaml` | Env Secret for the job, named by `streamConfigurationSecretRef`. |
| `resources/coredns-s2-localhost.yaml` | CoreDNS rewrite for `s2.localhost`. |
| `resources/warehouse-storage.json` | Warehouse storage profile with the browser-usable endpoint. |
| `resources/s2-config.json` | S2 config with the anonymous read-only principal for the UI preview. |
| `resources/operator-rbac.yaml` | RBAC the operator needs on PullStreams and Events. |
| `resources/noop-job-template.yaml` | Backfill template that exits immediately. |
| `dataroute-no-variant.yaml` | Flat `delivery` route (no variant column). |
| `pull-stream-no-variant.yaml` | `PullStream` that pulls `delivery` into Iceberg. |
| `values-operator-demo.yaml` | arcane-operator Helm overrides. |
| `dataroute.yaml` | The demo `DataRoute` CR. |
| `values-push-stream-demo.yaml` | Push-stream Helm overrides. |
| `values-pull-stream-demo.yaml` | Pull-stream chart overrides (StreamClass, job template). |

The data plane is **not** redefined here. `deploy/k8s` stays the single source of
truth; this directory is a kustomize overlay on it.

## Things worth knowing

These are the non-obvious reasons the demo is wired the way it is. Each is also
commented at the point it matters.

**The DynamoDB table name must match on both sides.** Push-stream's
`values-dev.yaml` writes to `arcane_push_stream`; pull-stream's reads
`arcane_ingestion`. Out of the box the two never connect, so
`values-push-stream-demo.yaml` repoints the producer at `arcane_ingestion`.

**The source table is seeded with one event before pull-stream starts.**
`PullStreamWatermark.epoch` is `Instant.now()`, not 1970 despite the name, and
`getMaxTimestamp` falls back to it whenever the DynamoDB query returns no items.
Against an empty table the in-memory watermark therefore ratchets forward to
wall-clock time every tick while the stored watermark stays at 1970. Since reads
use `timestampUTC > watermark`, any event landing in the epoch-second the
watermark already reached is never read — version equal is not greater — and the
stream reports `No changes in watermark version` indefinitely while those rows sit
in DynamoDB. Seeding one row first anchors the watermark to real data.

**Lakekeeper is pinned to v0.13.3.** `deploy/k8s` pins v0.11.4, which rejects the
demo's schema outright (`data did not match any variant of untagged enum
SchemaEnum`) because the `attributes` map becomes a `variant` column, forcing
Iceberg format-version 3.

**Trino is pinned to 484.** `deploy/k8s` pins 474, which predates Iceberg VARIANT
read support, so every query fails with `Failed to load table: events`. The
integration tests keep 474 because their fixtures are flat.

**Lakekeeper's `BASE_URI` is removed.** It is pinned to `http://lakekeeper:8181`,
which gets baked into the web UI bundle as the API address. A browser on
`localhost:8181` would try to call that name and fail. Unsetting it makes
Lakekeeper derive the base URI from the request's `Host` header, so both
in-cluster clients and the browser work.

**The pull-stream job mounts a `logback.xml`.** `build.sbt` discards every
`logback.xml` in its assembly merge strategy, including the application's own, so
the jar ships no logging config and logback falls back to root DEBUG. That buries
the staging/merge/watermark lines under AWS SDK HTTP wire dumps. The mounted
config puts the SDK loggers at WARN.

**The warehouse endpoint is `s2.localhost:9000`.** Lakekeeper gives every client
the warehouse's S3 endpoint, and its UI previews tables by reading parquet from S2
directly in the browser (DuckDB-WASM). `bootstrap.yaml` registers `http://s2:9000`,
which a browser cannot resolve. `s2.localhost` resolves to 127.0.0.1 on the host
without a hosts-file edit, and reaches the S2 port-forward on `localhost:9000`. A
CoreDNS rewrite (`resources/coredns-s2-localhost.yaml`) maps the same name to the
`s2` Service in-cluster, and `up` updates the warehouse with
`resources/warehouse-storage.json`. Table locations stay `s3://tmp/...`. Trino
keeps its own `s3.endpoint=http://s2:9000`.

**S2 allows anonymous reads on the `tmp` bucket.** DuckDB-WASM in the Lakekeeper UI
reads S2 from the browser and does not sign its requests, so S2 would answer 403.
`resources/s2-config.json` (mounted via a generated ConfigMap and the `s2` patch in
`kustomization.yaml`) adds S2's anonymous principal with `s3:GetObject` and
`s3:ListBucket` on `tmp` only. Writes still need the `minioadmin` credentials. S2
reads its policies at startup, and its data volume is an `emptyDir`, so restarting
the pod wipes the warehouse files.

**The operator runs pull streams, there is no dev-runner.** `arcane-crd` and
`arcane-operator` are installed from the `ghcr.io/sneaksanddata/helm` OCI charts.
The `arcane-stream-pull` chart renders the `StreamClass` and
`StreamingJobTemplate`; applying a `PullStream` makes the operator create a Job
from that template, with env from the Secret in `streamConfigurationSecretRef`.

**The operator needs extra RBAC.** Its chart does not grant access to the
`PullStream` resources it watches or to Events, so `operator-rbac.yaml` binds the
plugin chart's viewer/editor ClusterRoles and an events role to its service
account.

**Backfill is pointed at a no-op template.** The operator always runs a backfill
Job before starting the streaming Job for a new stream. The pull plugin stubs
backfill out and fails with `UnsupportedOperationException`, which would leave the
stream stuck in `Backfilling`. `backfillJobTemplateRef` therefore names a template
that exits immediately (`noop-job-template.yaml`).

**Only the `delivery` route is pulled.** `up` creates a `PullStream` for the flat
`delivery` route. The variant `producer1` route still ingests into DynamoDB, but
no `PullStream` pulls it into Iceberg; copy `pull-stream-no-variant.yaml` and
change `pullIndexValue` and `targetTableFullName` to add one.

