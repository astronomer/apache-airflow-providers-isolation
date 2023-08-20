# `isolationctl`
A companion CLI is provided to ease creating and distributing environments

<!-- TOC -->
* [`isolationctl`](#isolationctl)
  * [Install](#install)
  * [Requirements](#requirements)
  * [Usage](#usage)
    * [`deploy`](#deploy)
    * [`environment`](#environment)
      * [`add`](#add)
      * [`remove`](#remove)
    * [`init`](#init)
      * [`--astro`](#--astro)
      * [`--git`](#--git)
      * [`--cicd=...`](#--cicd)
      * [`--example/--no-example`](#--example--no-example)
<!-- TOC -->

## Install
```shell
pip install apache-airflow-providers-isolation[cli]
```

## Requirements
- To utilize container-based environments (currently the only option), you must have write access to an existing [Container Registry](https://sysdig.com/learn-cloud-native/container-security/what-is-a-container-registry/).
    - [See here for common container registries](https://sysdig.com/learn-cloud-native/container-security/what-is-a-container-registry/#container-registries-compared)
    - you can create a local registry with the following, however this will store container images **ONLY USABLE ON YOUR WORKSTATION**:
        ```shell
        docker run -d -p 5000:5000 --restart=always --name registry registry:2
        ```
    - **Astronomer Customers** - You _cannot utilize the Container Registry utilized for `astro deploy`_, you will need to provide an alternative container registry


## Usage
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
### `deploy`
???

### `environment`
Environments are contained in folders inside of an Airflow project. The expected setup is as follows:
```shell
$ tree
.
├── Dockerfile
├── dags
│  └── my_dag.py
├── environments
│  ├── env_one
│  │   ├── Dockerfile
│  │   ├── packages.txt
│  │   └── requirements.txt
│  └── env_two
│      ├── Dockerfile
│      ├── packages.txt
│      └── requirements.txt
├── packages.txt
└── requirements.txt
```

#### `add`
`isolationctl environment add` creates an environment, which inherits from a parent image (the root `Dockerfile`).

An environment contains:
- A `Dockerfile` with, at a minimum
    ```Dockerfile
    ARG BASE_IMAGE
    FROM ${BASE_IMAGE}
    ```
- An optional `requirements.txt` for Python dependencies
- An optional `packages.txt` for System Packages

#### `get`
`isolationctl environment get` Get environments.

#### `remove`
`isolationctl environment remove` removes an environment, which is the same as `rm -rf environments/environment`

### `init`
`isolationctl environment init` initializes environments, which creates the folder structure and a few optional extras

#### `--astro`
**Requirement**: `astro` must be installed and accessible on your path.

`isolationctl init --astro` initializes an Astro Airflow project. It runs `astro dev init` directly.

#### `--git`
**Requirement**: `git` must be installed and accessible on your path.

You can initialize a Git repo with `isolationctl init --git`. It runs `git init` directly.

#### `--cicd=...`
`isolationctl init --cicd="GitHub"` adds a CICD template. The template is an extension of the Astro CICD Templates, and you can [read more here](https://docs.astronomer.io/astro/ci-cd-templates/template-overview) to finish setting them up.

#### `--example/--no-example`
`isolationctl init --example` creates an example environment. It directly invokes [`isolationctl environment add example` (docs)](#environment)
