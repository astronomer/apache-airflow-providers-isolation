<!-- TOC -->

* [General Usage](#general-usage)
    * [Environments and Images](#environments-and-images)
* [Advanced Usage](#advanced-usage)
    * [Referencing Airflow Connections and Variables:](#referencing-airflow-connections-and-variables)
    * [Referencing Airflow Context and Templates](#referencing-airflow-context-and-templates)
    * [Referencing Functions and Callables as parameters](#referencing-functions-and-callables-as-parameters)
    * [Referencing the Operator as a qualified name](#referencing-the-operator-as-a-qualified-name)

<!-- TOC -->

# General Usage

The `IsolatedOperator` can take any Airflow Operator and takes the parameters of that `Operator` as its own. For
instance, `bash_command` is a parameter of `BashOperator` in the following example.

```python
IsolatedOperator(task_id="...", operator=BashOperator, bash_command="echo hi", ...)
```

## Environments and Images

Using the `isolationctl` CLI - you can build and deploy separate environments. Read more [here](./CLI.md). You will need
to run `isolationctl deploy ...` every time you make changes IF your `IsolatedOperator` references anything outside
the `IsolatedOperator`, in your Airflow project.

If the Environment Variable `AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE` is set

```dotenv
AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE='docker.io/my/airflow/project_12345'
```

You can omit the `image` key and set `environment`:

```python
IsolatedOperator(task_id="...", environment="example", ...)
```

You can also directly specify (or override) `image`

```python
IsolatedOperator(task_id="...", image="docker.io/my/airflow/project_12345/example", ...)
```

Or combine `image` and `environment`

```python
IsolatedOperator(
    task_id="...",
    image="docker.io/my/airflow/project_12345/",
    environment="example",
    ...,
)
```

# Advanced Usage

## Configuring the IsolatedOperator

For an `IsolatedOperator` running with a `KubernetesPodOperator` (default), you can pass any operators
for `KubernetesPodOperator` via `isolated_operator_kwargs`

```python
IsolatedOperator(task_id="...", isolated_operator_kwargs={"image_pull_policy": "Never"})
```

## Referencing Airflow Connections and Variables:

References to Variables, Connections should work as expected with a typical operator. The `IsolatedOperator` will find
any referenced connections and provide them directly to the

```python
IsolatedOperator(
    task_id="...",
    operator=SimpleHttpOperator,
    endpoint="repos/apache/airflow",
    method="GET",
    http_conn_id="GITHUB",
)
```

Variables also function as expected:

```python
IsolatedOperator(
    task_id="...", operator=BashOperator, bash_command="echo {{ var.foo.value }}"
)
```

## Referencing Airflow Context and Templates

References to Airflow Context or templates should work as expected with a typical operator. Not all keys in the Airflow
Context may deserialize correctly, and may not operate as expected.

```python
with DAG(..., params={"xyz": "foo"}):
    IsolatedOperator(
        task_id="...",
        operator=BashOperator,
        bash_command="echo {{ params.xyz }}",
    )
```

## Referencing Functions and Callables as parameters

Airflow obfuscates module imports during DAG Serialization. This can interfere with directly serializing callables in
some circumstances.

If you have a DAG `dags/my_dag.py` like:

```python
def my_fn():
    ...


IsolatedOperator(task_id="...", operator=PythonOperator, python_callable=my_fn)
```

You may encounter an error like:

```text
ModuleNotFoundError: No module named 'unusual_prefix_cc53bfb719e11e2b18b1d66382f5047c2461068c_my_dag'
```

The literal value `unusual_prefix_` is a symptom of Airflow's Serialization.

To fix this you can directly provide the qualified name of your function as a string, with a
special `_*_callable_qualname` parameter. This should be referenced from an entry in `PYTHONPATH`, which is likely the
repository root. If your file is `dags/my_dag.py`, and the function is in that file and named `my_fn`, the qualified
name should be `dags.my_dag.my_fn`.

```python
def my_fn():
    ...


IsolatedOperator(
    task_id="...",
    operator=PythonOperator,
    _python_callable_qualname="dags.my_dag.my_fn",
)
```

## Referencing the Operator as a qualified name

The `IsolatedOperator` also supports the Operator reference itself as a qualified name

```python
class CustomOperator(BashOperator):
    pass


IsolatedOperator(task_id="...", operator="dags.my_dag.CustomOperator")
```
