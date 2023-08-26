<!--suppress HtmlDeprecatedAttribute -->
<p align="center" >
  <a href="https://www.astronomer.io/">
    <img src="https://raw.githubusercontent.com/astronomer/telescope/main/astro.png"
    alt="Astronomer Isolation Provider Logo"
    style="display:block; margin-left: auto; margin-right: auto;" />
  </a>
</p>
<h1 align="center" style="text-align: center;">
   Isolation Provider
</h1>
<h3 align="center" style="text-align: center;">
  Runtime Operator Isolation in Airflow.

Created with ❤️ by the CSE Team @ Astronomer
</h3>

<!-- TOC -->

* [Summary](#summary)
* [Quickstart](#quickstart)
    * [Pre-requisites:](#pre-requisites)
    * [Steps](#steps)
* [Installation](#installation)
    * [CLI](#cli)
* [Usage](#usage)
* [How](#how)
    * [What it is](#what-it-is)
    * [What it isn't](#what-it-isnt)
* [Requirements](#requirements)
    * [CLI](#cli-1)
    * [Host Airflow](#host-airflow)
    * [Target Isolated Environment](#target-isolated-environment)

<!-- TOC -->

# Summary

The Isolation Provider provides the `IsolatedOperator` and the `isolationctl` CLI.
It provides the capacity to run any Airflow Operator in an isolated fashion.

Why use `IsolatedOperator`?

- Run a different version of an underlying library,
  between separate teams on the same Airflow instance,
  even between separate tasks in the same DAG
- Run entirely separate versions of Python for a task
- Keep "heavy" dependencies separate from Airflow
- Run an Airflow task in a completely separate environment - or on a server all the way across the world.
- Run a task "safely" - separate from the Airflow instance
- Run a task with dependencies that conflict more easily
- Do all of the above while having unmodified access to (almost) all of 'normal' Airflow -
  operators, XCOMs, logs, deferring, callbacks.

What does the `isolationctl` provide?

- the `isolationctl` gives you an easy way to manage "environments" for your `IsolatedOperator`

When shouldn't you use `IsolatedOperator`?

- If you _can_ use un-isolated Airflow Operators, you still _should_ use un-isolated Airflow Operators.
- 'Talking back' to Airflow is no longer possible in an `IsolatedOperator`.
  You cannot do `Variable.set` within an `IsolatedOperator(operator=PythonOperator)`, nor can you query the Airflow
  Metadata Database.

# Quickstart

This quickstart utilizes a local Kubernetes cluster, and a local image registry to host isolated environments.

## Pre-requisites:

- [x] [Docker](https://www.docker.com/get-started/) is installed
- [x] A local Kubernetes (
  e.g. [Docker Desktop](https://docs.docker.com/desktop/kubernetes/#turn-on-kubernetes), [Minikube](https://minikube.sigs.k8s.io/docs/),
  etc) is running
- [x] [`astro` CLI](https://docs.astronomer.io/astro/cli/overview) is installed

## Steps

1. Download and install the `isolationctl` CLI via

    ```shell
    pip install apache-airflow-providers-isolation[cli]
    ```

2. Set up the project:

    ```shell
    isolationctl init --example --local --local-registry --astro --git --dependency
    ```

    - `--example` adds `environments/example`
    - `--local` uses `kube config view` to add a `KUBERNETES_DEFAULT` Airflow Connection in `.env`
    - `--local-registry` runs a docker image registry at `localhost:5000`
    - `--astro` runs `astro dev init`
    - `--git` runs `git init`
    - `--dependency` adds `apache-airflow-providers-isolation[kubernetes]` to `requirements.txt`

3. Add an 'older' version of Pandas to `environments/example/requirements.txt`

    ```shell
    echo "\npandas==1.4.2" >> environments/example/requirements.txt
    ```

4. Add the http provider to `environments/example/requirements.txt`

    ```shell
    pushd environments/example/
    astro registry provider add http
    popd
    ```

5. Build the `example` environment and deploy it to the local registry

    ```shell
    isolationctl deploy --local-registry
    ```

6. Add the example DAG, which invokes Pandas with two separate versions in two tasks in the same DAG, and a few other
   tasks

    ```shell
    wget --output-document dags/isolation_provider_example_dag.py https://raw.githubusercontent.com/astronomer/apache-airflow-providers-isolation/main/isolation/example_dags/isolation_provider_example_dag.py
    ```

7. Add the Kubernetes Provider to the Astro project (_not required_ - it is a transitive dependency - but always good to
   be explicit)

    ```shell
    astro registry provider add kubernetes
    ```

8. Start the Airflow Project

    ```shell
    astro dev start --no-browser
    ```

9. Run the example DAG

    ```shell
    astro run isolation_provider_example_dag
    ```

10. 🎉🎉🎉

# Installation

```shell
pip install apache-airflow-providers-isolation[kubernetes]
```

## CLI

To install the [isolationctl CLI](./CLI.md)

```shell
pip install apache-airflow-providers-isolation[cli]
```

# Usage

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from isolation.operators.isolation import IsolatedOperator

with DAG(
    "isolation_provider_example_dag",
    schedule=None,
    start_date=datetime(1970, 1, 1),
):
    IsolatedOperator(
        task_id="echo_with_bash_operator",
        operator=BashOperator,
        environment="example",
        bash_command="echo {{ params.hi }}",
    )
```

# How

![A diagram documenting the flow from IsolatedOperator to KubernetesPodOperator to a pod to PostIsolationHook to OtherOperator](https://lucid.app/publicSegments/view/1695b264-e325-4c13-9c08-6b7b92da867d/image.png)

## What it is

The `IsolatedOperator` is a wrapper on top of underlying operator,
such as `KubernetesPodOperator` or potentially other operators,
to run some `OtherOperator` in an isolated environment without any need to know or understand how the operator
underlying the `IsolatedOperator` works - beyond what is absolutely required to run.

It also has the [isolationctl CLI](./CLI.md) which aids in the creation and deployment of isolated environments to run
in.

The steps that the `IsolatedOperator` takes, in more detail:

1. Airflow initializes an underlying base operator (such as `KubernetesPodOperator`)
   based on the `IsolatedOperator.__init__`
    - Some amount of initial state (e.g. the Airflow, Connections, Variables)
      will be provided to the Isolated environment via special ENV vars
2. The underlying operator (such as `KubernetesPodOperator`) executes as normal
3. The isolated environment runs within the underlying operator,
   and is bootstrapped via the `PostIsolationHook`.

- State is re-establish via passed in ENV vars
- In an otherwise un-initialized Airflow, via an internal hok, the `OtherOperator` is executed
- This isolated environment's Airflow _has no knowledge_ of the parent Airflow that launched it,
  it should have no access to the parents' Metadata Database,
  and _it should not be able to communicate back to the parent Airflow_.

Most Airflow functionality should work out-of-the-box, simply due to the reliance on the underlying operators to do most
of the "heavy lifting" - e.g. XCOMs and Logs

## What it isn't

- If you _can_ use un-isolated Airflow Operators, you still _should_ use un-isolated Airflow Operators.
  This won't make an operator that _would run normally_ any easier to run.
- Anything that requires _communicating back_ to the parent Airflow at runtime is unsupported or impossible -
  e.g. `Variable.set(...)`
- Anything that requires querying or modifying the parent Airflow's state at runtime is unsupported or impossible.
  Examples include `@provide_session`, or `TaskInstance.query(...)`, or
  `ExternalTaskSensor`, or `TriggerDagRunOperator`
- It's possible other less-traditional parts of Airflow may not yet be supported,
  due to development effort - e.g. `@task` annotations or Airflow 2.7 Setup and Teardowns
- It is possible that things like `on_failure_callback`s or lineage data may not work - depending on how exactly they
  are invoked -
  but if these things work with via the underlying operator and are set on the underlying operator,
  then they should work with this.

# Requirements

## CLI

- Python ">=3.8,<3.12"
- You must have write access to a Container Registry, read more at [CLI Requirements](./CLI.md#requirements)

## Host Airflow

- Python ">=3.8,<3.12"
- Airflow >=2.3
- Must have access to create containers in the target Kubernetes Environment

## Target Isolated Environment

- Python ">=3.8,<3.12"
- Airflow >=2.3
    - Note: Airflow 2.2 doesn't have `test_mode` for executing tasks - which is currently utilized to bypass setup of
      the
      isolated environment's Airflow
- Must be a Kubernetes environment
