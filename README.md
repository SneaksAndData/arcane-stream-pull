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
