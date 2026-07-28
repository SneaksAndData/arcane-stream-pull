## Pull Stream Plugin for Arcane
This repository contains implementation of a *Queue*-Iceberg streaming plugin for Arcane.

Use this app to livestream DynamoDB exports to an Iceberg table, backed by Trino. Messages are produced by [arcane-push-stream](https://github.com/SneaksAndData/arcane-push-stream) as a streaming source and [Lakekeeper](https://github.com/lakekeeper/lakekeeper) as a data catalog.

### Local kind / e2e setup with arcane-push-stream

1. Clone this repo and arcane-push-stream.
2. Run:
```bash
just kind-up
```
3. and start arcane-push-stream also in the same kind cluster (same ns as well)
4. port forward push-stream if you want to produce messages (and optionally minio srv to
   open the UI)
4. start emitting messages with `uv run produce.py`

### Quickstart

### Development setup

#### Getting access to GitHub Packages registry
In order to build, test and run the project, `GITHUB_TOKEN` environment variable needs to be set.
It is used to authenticate against GitHub Maven package registry, specifially for JAR dependencies under
https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala.

Create [new](https://github.com/settings/personal-access-tokens/new) personal access token PAT (Personal Access Token).
For example, fine-grained token with "Public repositories" access and without explicit permissions.

Export `GITHUB_TOKEN` environment variable before running any `sbt` commands.
For example, put `export GITHUB_TOKEN=github_pat_xxx` line in your `.zshrc`/`.bashrc` file.

### Arcane operator and streams on Kind
Local K8S cluster (i.e. [Kind](https://github.com/kubernetes-sigs/kind)) can be used to verify that Arcane operator and
its dependencies coming from Helm charts are correctly setup.

Furthermore, Arcane is lightweight enough so that actual streams can be deployed on the local K8S cluster to, for example,
try out or test features in a dev setup.

#### Setting up Kind
Kind itself should be already installed if you ran `mise install`. Next steps:
1. Create Kind cluster: `kind create cluster --name arcane-dynamodb-dev`
2. Create namespace: `kubectl create namespace arcane --context kind-arcane-dynamodb-dev`
3. Install required [CRDs](github.com/SneaksAndData/arcane-crd):
```sh
helm install arcane-crd oci://ghcr.io/sneaksanddata/helm/arcane-crd \
  --version vX.Y.Z \
  --namespace arcane \
  --kube-context kind-arcane-dynamodb-dev
  ```
4. Install Arcane [operator](github.com/SneaksAndData/arcane-operator):
```sh
helm install arcane oci://ghcr.io/sneaksanddata/helm/arcane-operator \
  --version vX.Y.Z \
  --namespace arcane \
  --kube-context kind-arcane-dynamodb-dev
  ```
5. Build a Docker image for this project: `mise docker-build kind-dev`
6. Load the Docker image to Kind cluster:
```sh
kind load docker-image \
    ghcr.io/sneaksanddata/arcane-stream-dynamodb:kind-dev \
    --name arcane-dynamodb-dev
```
7. Install chart from this project:
```sh
helm upgrade --install arcane-dynamodb ./.helm \
    --kube-context kind-arcane-dynamodb-dev \
    --namespace arcane \
    --set image.repository=ghcr.io/sneaksanddata/arcane-stream-dynamodb \
    --set image.tag=kind-dev \
    --set image.pullPolicy=IfNotPresent
```


### Development
Project uses `Scala 3.8.3` and tested on JDK 25.

Mermaid chart of dependency graph:
[Link](https://mermaid.live/edit/#pako:eNqVVktv4zYQ_isGTy2QNfSwXj70EHsTtIgXxtrFAoUvtETLRCjSoKggbpD_3iElReNY67Y5GDPMfPP8htQbyVXByJyUmp6Ok_vtTj55v-zI7znbM11-lYab84pKWjI9rbl8fqJnpnfk18mXL79Nnnww3eRHVtEVBw-GK7nWKmd1rfRUdKbgMvipS0NLLstPXgERDogt3QsGbk9M38wFUDNArRshNkYzWq1FA84XShr2aj7SQdatGNki2jxGk4-HVDbhghoqVPlDc8P0J58RGCcXGVi3rLinJj8-0NwofR6BpABZslxQzYoVM5rn9YhV5qwOtBHmG63YI5MMGq7Gcujq8r1_6-Fl8wekN4gBiL4d8zelTpsj1YWtJ38-8L7GJXQEPL_w4qohfuBaa60eLb--s1qJFzDLVXVSNStQaN9O3AX46Sx6p9baTvqPYp-voDi2YfqF52whOJNm1NzO-IEzUTxwAYMD91tNZX1QumLjAezU-0IXkK5g4-zGmGSY0dAjaljf8DFMins7TpLWMPvELPsLddxsP-AiJAeWEa5jjpI3iwns0Je8tnP6D9Z20j-gVF1R_Xy7Sxhmh76lkJBZUQ57KqnM2e1Is65h_4OFaCecC8uGtar5a4t94gcYbtXTCO9-vwWWDY9C7aloN3RLy6to9tbqzFFEJ6dYdhl0B5YxbpV53ubyvZGgjWTiBz3E4tOBaS3uXilTG1iy01X9LSJGit_nhhfdQ3lauYvnO4CHlRQpGZLxuY8u2MDdXTCgpSqnfxb1utkLXh8vW9fZhl6_qfXHqo50I7TsPH1sgzVSjc7Z5ZMQBpc740xGmTLMLkSzc3LoOHrUqimPp8bAnkKL7xsurpkWthvtWGqRlqrdg9Ft0q2bKsAtc29gP4IMEThDjUbnoeV0cZa0Ust96_76Xg9CNOMQgV20CCtY7oeP3hYE9mcoo9lwjpMLkUuUAz73Y0RMp6RITgZChi4GKiod_udkD8khln2kZEjuVsMPMblRmk7OkNxFi4ckYnIHn1C8IHOjG3ZHKnsRWpW87eRksiMGPpLYjsxBLNq93ZGdfAfYicq_lKp6pOMZmR-oqEFrTgVcqktO4etqMGESuLdQjTRk7iex80Hmb-SVzKNgmtm_NEq8OEqy6I6c4dRPp2kUp94smsVhEifvd-RvF9SbpsnMApJ45gVhEiR90K8Fh7eoj0kbozZnmbf6-z_wbyZq)
