## Pull Stream Plugin for Arcane
This repository contains implementation of a *Queue*-Iceberg streaming plugin for Arcane.

Use this app to livestream DynamoDB exports to an Iceberg table, backed by [arcane-push-stream](https://github.com/SneaksAndData/arcane-push-stream) as a streaming source and [Lakekeeper](https://github.com/lakekeeper/lakekeeper) as a data catalog.

### Quickstart

This source continuously ingests files with `multiline-JSON` content (DynamoDB export format) into a target Iceberg table. In order to configure the stream, you must provide the following:
- *Desired* AVRO schema for the source. Note that this schema should conform with JSON created *after* JSON pointers and array explode have been applied. All fields in the schema must be defined as `nullable`. You can use this [handy tool](https://jonathanfiss.github.io/convert-json-to-avro-schema/) to generate the schema.
- Source S3 path
- JSON pointer expression, if desired data is a subset of a source json. For example, given
```json
{
  "colA": "a",
  "colB": {
    "colC": "c",
    "propA": 1,
    "propB": "ABC"
  }
}
```
and `jsonPointerExpression` set to `/colB`, source will be transformed to:
```json
{
  "colC": "c",
  "propA": 1,
  "propB": "ABC"
}
```
- JSON pointers for array explode, if any. For example, given
```json
{
  "colA": "a",
  "colB": [{
    "colC": "c1",
    "propA": 1,
    "propB": "ABC1"
  },{
    "colC": "c2",
    "propA": 2,
    "propB": "ABC2"
  }]
}
```
and `jsonArrayPointers` set to `"/colB": {}`, source will be transformed to:
```json
{"colC": "c1", "propA": 1, "propB": "ABC1"}
{"colC": "c2", "propA": 2, "propB": "ABC2"}
```
emitting 2 rows from 1 source file entry.

### Development setup

#### Tooling
Install the following tools:
- `mise` - for managing tooling versions, environment variables: https://github.com/jdx/mise
- `just` - for orchestrating tasks: https://github.com/casey/just
- Docker/Docker compose - for integration testing: https://www.docker.com/products/docker-desktop/

Once the above are installed, run `mise install`.
It will install other necessary tools (e.g. JDK and SBT) at recommended versions for this project only.

#### Getting access to GitHub Packages registry
In order to build, test and run the project, `GITHUB_TOKEN` environment variable needs to be set.
It is used to authenticate against GitHub Maven package registry, specifially for JAR dependencies under
https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala.

Create [new](https://github.com/settings/personal-access-tokens/new) personal access token PAT (Personal Access Token).
For example, fine-grained token with "Public repositories" access and without explicit permissions.

Export `GITHUB_TOKEN` environment variable before running any `sbt` commands.
For example, put `export GITHUB_TOKEN=github_pat_xxx` line in your `.zshrc`/`.bashrc` file.

#### Common tasks
- Building the project (fat JAR): `just build`
- Building Docker image: `just docker-build [tag]`
- Running integration tests: `just it`
- Running streaming application locally:
  - via `just stream [--debug]` or `just backfill [--debug]` (backfill mode). **Note**: `dev.env` is required, see `dev.env.example` for an example application configuration.
- Cleaning build artifacts: `just clean`
- Code style check: `just check`

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

#### Running streams in Kind
To be added...

### Development
Project uses `Scala 3.8.3` and tested on JDK 25.
