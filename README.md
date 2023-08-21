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
* [tl;dr](#tldr)
  * [Local Requirements:](#local-requirements-)
  * [`isolationctl`:](#isolationctl-)
  * [Add DAG](#add-dag)
  * [`astro`](#astro)
* [What](#what)
* [Why](#why)
* [Installation](#installation)
* [Usage](#usage)
* [How](#how)
  * [What it is](#what-it-is)
  * [What it isn't](#what-it-isnt)
* [Requirements](#requirements)
  * [Technical Requirements - Host Airflow](#technical-requirements---host-airflow)
  * [Technical Requirements - Target Isolated Environment](#technical-requirements---target-isolated-environment)
<!-- TOC -->

# tl;dr
Check it out locally
## Local Requirements:
   - [x] [Docker](https://www.docker.com/get-started/) is installed
   - [x] A local Kubernetes (e.g. [Docker Desktop](https://docs.docker.com/desktop/kubernetes/#turn-on-kubernetes), [Minikube](https://minikube.sigs.k8s.io/docs/), etc) is running
   - [x] [`astro` CLI](https://docs.astronomer.io/astro/cli/overview) is installed
## `isolationctl`:
   ```shell
   pip install apache-airflow-providers-isolation[cli]
   isolationctl init --example --local --git --astro
   isolationctl deploy --local
   ```

## Add DAG
   ```python
   from airflow import DAG
   from datetime import datetime
   from airflow.providers.isolation.operators import IsolatedOperator

   with DAG("isolated_example", schedule=None, datetime=datetime(1970, 1, 1)):
       IsolatedOperator()
   ```
## `astro`
   ```shell
   astro registry provider add kubernetes
   astro dev start
   astro run isolated_example
   ```

# What
This provider contains the `IsolatedOperator`, which is a mechanism to run other Airflow Operators in an isolated fashion.

# Why
Operator Isolation provides a few benefits:
- In one Airflow project, run separate versions of python or system dependencies in different DAGs or even different Tasks within the same DAG
-

It also has a few drawbacks:
- Talking back to Airflow is no longer supported, e.g. you cannot do `Variable.set` within an Isolated `PythonOperator`

# Installation
1. Generally
    ```shell
    pip install apache-airflow-providers-isolation
    ```
2. For an Astro Project, add to your `requirements.txt`
    ```requirements.txt
    apache-airflow-providers-isolation
    ```
3. To install the [CLI](./CLI.md)
    ```shell
    pip install apache-airflow-providers-isolation[cli]
    ```

# Usage

# How
pic

## What it is
The IsolatedOperator is, in short, a wrapper on top of `KubernetesPodOperator` (and potentially other operators) to run some `OtherOperator` without any need to know or understand how the operator underlying the `IsolatedOperator` works, beyond what is absolutely required to run.

It also has a companion CLI which aids in the creation of isolated environments to run in.

Steps, in more detail:
1. At runtime, Airflow initializes an underlying existing operator and other logic contained within the `IsolatedOperator.__init__`
   - Some amount of initial state (e.g.  the Airflow, Connections, Variables) will be provided to the Isolated environment via special ENV vars
2. Inside the Isolated environment, we utilize the `PostIsolationHook` to re-establish the state that was passed in and execute the `OtherOperator` in an otherwise un-initialized Airflow. This isolated Airflow _has no knowledge_ of the parent Airflow that launched it, should have no access to the parents' Metadata Database, and _should not be able to communicate back_.

Most Airflow functionality should work out-of-the-box, simply due to the reliance on the underlying operators to do most of the "heavy lifting" - e.g. XCOMs and Logs

## What it isn't
- Anything that you can and should just do normally in Airflow, you should do normally in Airflow - this won't make an operator that _would run normally_ any easier to run
- Anything that requires _communicating back_ to the parent Airflow at runtime will not be supported, intentionally - e.g. `Variable.set(...)`
- Anything that requires querying Airflow state at runtime will not be supported, intentionally - e.g. `@provide_session`, or `TaskInstance.query(...)`, or
`ExternalTaskSensor`, or `TriggerDagRunOperator`
- It's possible other less-traditional parts of Airflow may not be supported, just due to development effort - e.g. `@task` annotations or Airflow 2.7 Setup and Teardowns
- It is possible that things like `on_failure_callback`s or lineage data may not work - depending on how exactly they are invoked - but if these things work with via the underlying operator and are set on the underlying operator, then they should work with this.


# Requirements
## Technical Requirements - Host Airflow
- Due to the Isolated requirement, only Airflow 2.3 and above is supported

## Technical Requirements - Target Isolated Environment
- Airflow 2.2 doesn't have `test_mode` for executing tasks, so currently only Airflow 2.3 and above is supported
