<!-- TOC -->

* [Summary](#summary)
* [Installation](#installation)
* [Requirements](#requirements)
* [Usage](#usage)
    * [`isolationctl deploy`](#isolationctl-deploy)
        * [`isolationctl deploy --registry`](#isolationctl-deploy---registry)
        * [`isolationctl deploy --local-regisry`](#isolationctl-deploy---local-regisry)
        * [`isolationctl deploy --astro`](#isolationctl-deploy---astro)
    * [`isolationctl environment`](#isolationctl-environment)
        * [`isolationctl environment add`](#isolationctl-environment-add)
        * [`isolationctl environment get`](#isolationctl-environment-get)
        * [`isolationctl environment remove`](#isolationctl-environment-remove)
    * [`isolationctl init`](#isolationctl-init)
        * [`isolationctl init --astro`](#isolationctl-init---astro)
        * [`isolationctl init --git`](#isolationctl-init---git)
        * [`isolationctl init --cicd`](#isolationctl-init---cicd)
        * [`isolationctl init --example/--no-example`](#isolationctl-init---example--no-example)
        * [`isolationctl init --local`](#isolationctl-init---local)
        * [`isolationctl init --local-registry`](#isolationctl-init---local-registry)
        * [`isolationctl init --dependency`](#isolationctl-init---dependency)

<!-- TOC -->

# Summary

The `isolationctl` CLI is provided to ease creating and distributing environments

# Installation

```shell
pip install apache-airflow-providers-isolation[cli]
```

# Requirements

- To utilize container-based environments (currently the only option), you must have write access to an
  existing [Container Registry](https://sysdig.com/learn-cloud-native/container-security/what-is-a-container-registry/).
    - [See here for common container registries](https://sysdig.com/learn-cloud-native/container-security/what-is-a-container-registry/#container-registries-compared)
    - you can create a local registry with the following, however this will store container images **ONLY USABLE ON YOUR
      WORKSTATION**:
        ```shell
        isolationctl init --local-registry --no-example
        ```
    - **[Astronomer Users](https://astronomer.io)** - You _cannot utilize the Container Registry utilized
      for `astro deploy`_, you will need
      to provide an alternative container registry and an authentication
      means. [Read more here.](https://docs.astronomer.io/astro/kubernetespodoperator#run-images-from-a-private-registry)

# Usage

```text
$ isolatonctl --help
Usage: isolationctl [OPTIONS] COMMAND [ARGS]...

  ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
  ██▄██░▄▄█▀▄▄▀█░██░▄▄▀█▄░▄██▄██▀▄▄▀█░▄▄▀█▀▄▀█▄░▄█░█
  ██░▄█▄▄▀█░██░█░██░▀▀░██░███░▄█░██░█░██░█░█▀██░██░█
  █▄▄▄█▄▄▄██▄▄██▄▄█▄██▄██▄██▄▄▄██▄▄██▄██▄██▄███▄██▄▄
  ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

Options:
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Commands:
  deploy       Build and deploy environments (alias: d).
  environment  Manage environments (alias: env, e).
  init         Initialize environments (alias: i).

  Check out https://github.com/astronomer/apache-airflow-providers-isolation
  for more details
```

## `isolationctl deploy`

Build and deploy environments (alias: d).

Build both the parent project and either a specific or all child ENVIRONMENTs

### `isolationctl deploy --registry`

The registry to push the image to such as specifies the registry to push child images to such
as `docker.io/path/to/my/airflow` or  `aaa.dkr.ecr.us-east-2.amazonaws.com/bbb`

### `isolationctl deploy --local-regisry`

**Requirement**: a local docker container registry must be running at `localhost:5000`

Pushes to a docker container registry running locally, such as the one created
with [`isolationctl init --local-registry`](#isolationctl-init---local-registry)

### `isolationctl deploy --astro`

**Requirement**: `astro` must be installed and accessible on your path.

Additionally pushes the Airflow project with `astro deploy`

## `isolationctl environment`

Manage environments (alias: env, e). Environments are contained in folders inside an Airflow project.

An environment inherits from a parent image (the root `Dockerfile`).
Both images should contain all DAGs and other supporting source code.
The child image may contain different dependencies or system packages from the parent.

### `isolationctl environment add`

Add an ENVIRONMENT initialized from a template

An environment inherits from a parent image (the root `Dockerfile`).
Both images should contain all DAGs and other supporting source code.
The child image may contain different dependencies or system packages from the parent.

An environment contains:

- A `Dockerfile` with, at a minimum,
    ```Dockerfile
    ARG BASE_IMAGE
    FROM ${BASE_IMAGE}
    ```
- A `requirements.txt` for Python dependencies with, at a minimum,
    ```requirements.txt
    apache-airflow-providers-isolation[kubernetes]
    ```
- An optional `packages.txt` for System Packages

### `isolationctl environment get`

Get environments or specific ENVIRONMENT

### `isolationctl environment remove`

Removes an ENVIRONMENT (which is the same as `rm -rf environments/environment`)

## `isolationctl init`

Initialize environments (alias: i).

Initialize environments for usage with this CLI. Intended to be used with an existing Airflow project.

### `isolationctl init --astro`

**Requirement**: `astro` must be installed and accessible on your path.

Initializes an Astro Airflow project. It runs `astro dev init` directly.

### `isolationctl init --git`

**Requirement**: `git` must be installed and accessible on your path.

Initialize the new project with `git`. It runs `git init` directly.

### `isolationctl init --cicd`

**NOT YET IMPLEMENTED**

Adds a CICD template. The template is an extension of the Astro CICD Templates, and
you can [read more here](https://docs.astronomer.io/astro/ci-cd-templates/template-overview) to finish setting them up.

### `isolationctl init --example/--no-example`

Create an example environment.
It directly invokes [`isolationctl environment add example` (docs)](#isolationctl-environment-add)

### `isolationctl init --local`

Adds entries to `.env`

1. if `--local-registry` and `--astro` are also set, adds
    ```dotenv
    AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE='localhost:5000/my/airflow/project_12345'
    ```
2. if `kubectl` is found, writes `AIRFLOW_CONN_KUBERNETES_DEFAULT` with the contents of `kubectl config view`
   serialized and quoted

### `isolationctl init --local-registry`

Runs a docker registry container via docker at localhost:5000

### `isolationctl init --dependency`

Adds the provider dependency (e.g. `apache-airflow-providers-isolaton[kubernetes]`) to a `requirements.txt`
