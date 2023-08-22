import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from isolation.operators.isolation import IsolatedOperator

os.environ["AIRFLOW_CONN_GITHUB"] = "http://api.github.com/https"


def print_pandas_version(arg, ds, params):
    # Note: this doesn't _need_ to be inside this function,
    # it can be in a separate file or in the top-level
    # as long as the version that Airflow has doesn't have an issue with it
    from pandas import __version__ as pandas_version

    print(f"Pandas Version: {pandas_version} \n" "And printing other stuff for fun: \n" f"{arg=}, {ds=}, {params=}")


with DAG(
    "isolation_provider_example_dag",
    schedule=None,
    start_date=datetime(1970, 1, 1),
    params={"hi": "hello"},
):
    PythonOperator(
        task_id="parent_pandas",
        python_callable=print_pandas_version,
        op_args=["Hello!"],
    )

    IsolatedOperator(
        task_id="isolated_environment_pandas",
        operator=PythonOperator,
        environment="example",
        python_callable=print_pandas_version,
        op_args=["Hello!"],
    )

    IsolatedOperator(
        task_id="echo_with_bash_operator",
        operator=BashOperator,
        environment="example",
        bash_command="echo {{ params.hi }}",
    )

    IsolatedOperator(
        task_id="use_a_connection",
        operator=SimpleHttpOperator,
        environment="example",
        endpoint="repos/apache/airflow",
        method="GET",
        http_conn_id="GITHUB",
        log_response=True,
    )
