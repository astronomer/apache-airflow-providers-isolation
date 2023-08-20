from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from isolation.operators.isolation import IsolatedOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    "example_dag",
    schedule=None,
    start_date=datetime(1970, 1, 1),
    params={"hi": "hello"},
):
    echo_params = IsolatedOperator(
        task_id="echo_params",
        operator=BashOperator,
        image="localhost:5000/dependency-isolation-in-airflow_36d6b4/airflow/data-team",
        bash_command="echo {{ params.hi }}",
    )

    use_a_connection = IsolatedOperator(
        task_id="use_a_connection",
        image="localhost:5000/dependency-isolation-in-airflow_36d6b4/airflow/data-team",
        operator=SimpleHttpOperator,
        endpoint="repos/apache/airflow",
        method="GET",
        http_conn_id="github_api",
        log_response=True,
    )

    # noinspection PyUnresolvedReferences,PyGlobalUndefined
    def run_py27_md5():
        from dags.extras import py27_md5

        global virtualenv_string_args
        return py27_md5.main(virtualenv_string_args[0])

    run_python27 = IsolatedOperator(
        task_id="run_python27",
        image="localhost:5000/dependency-isolation-in-airflow_36d6b4/airflow/py2",
        operator=ExternalPythonOperator,
        python="/usr/bin/python2.7",
        python_callable=run_py27_md5,
        string_args=["{{ ti.dag_id }}"],
        expect_airflow=False,
    )
