import sys
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, FIRST_EXCEPTION, Future
from pathlib import Path
from typing import Optional, Dict, List

import pytest
from docker import DockerClient
from docker.errors import ContainerError

from isolation.hooks.post_isolation import XCOM_FILE
from isolation.operators.isolation_kubernetes import run_in_pod, fn_to_source_code
from isolation.util import get_isolated_operator_env
from tests.conftest import manual_tests


def skip_no_docker(has_docker):
    """Skips this test if we don't have docker"""
    if not has_docker:
        pytest.skip("skipped, no docker")


def run_in_an_airflow_container(
    docker_client: DockerClient,
    project_root: Path,
    dist_file: Path,
    image: str,
    env: Optional[Dict[str, str]] = None,
    extra_volumes: Optional[List[str]] = None,
):
    """Run PostIsolationHook.run_isolated_task in the given image,
    with an environment and extra_volumes
    Writes the docker container logs (after it's finished) to stdout
    returns the logs as an object, to look for strings against
    """
    if env is None:
        env = {}
    if extra_volumes is None:
        extra_volumes = []

    # noinspection SpellCheckingInspection
    logs = docker_client.containers.run(
        image=image,
        stderr=True,
        remove=True,
        command=f"""'pip install ./{dist_file.name} && """ f"""python -c "{fn_to_source_code(run_in_pod)}" '""",
        volumes=[
            f"{dist_file}:/usr/local/airflow/{dist_file.name}",
            f"{project_root}/xcom:{Path(XCOM_FILE).parent}",
            *extra_volumes,
        ],
        entrypoint="/bin/bash -euxc",
        working_dir="/usr/local/airflow",
        environment=env,
    )
    sys.stdout.write(logs.decode())
    return logs


@manual_tests
def test_run_in_pod_docker(dist_file, project_root, docker_client, runtime_image, has_docker):
    """Mega test that runs a test-matrix in a threadpool against
    different versions of Astro Runtime

    runtime_image is a pytest fixture that is a matrix of images RT3 -> RT8

    pytest-xdist was causing a bunch of issues, and we just want to parallelize
    all these docker containers
    Below, we define a test matrix and a threadpool and a testing function
    and iterate them all
    We return on the first error

    1) Sees if the provider is generally installable
    2) Runs a bash operator with echo hi
    3) Runs a bash operator with echo {{params}}
    4) Runs a python operator with a callable (source code in the container)
    5) Runs a python operator with a callable that calls another callable
    with a global import
    6) Runs a python operator with a callable that calls another callable
    with a local import
    7) Runs a python operator with provide_context, params, and op_args in the function

    """
    skip_no_docker(has_docker)

    # TEST IS INSTALLABLE
    with pytest.raises(
        ContainerError,
        match="RuntimeError: __ISOLATED_OPERATOR_OPERATOR_QUALNAME must be set!",
    ):
        logs = run_in_an_airflow_container(docker_client, project_root, dist_file, runtime_image)
        sys.stdout.write(logs.decode())
        assert True, "We get log output when we install the operator " "in the container and launch it"

    # Tuple keys: magic_string, env, extra_volumes, reason
    test_matrix = [
        # TEST BASH OPERATOR
        (
            b"%%%%hi%%%%",
            get_isolated_operator_env(
                "airflow.operators.bash.BashOperator", kwargs={"bash_command": "echo '%%%%hi%%%%'"}
            ),
            [],
            "We ran a bash operator and saw the expected output in the task logs",
        ),
        (
            b"%%%%hi%%%%",
            get_isolated_operator_env(
                "airflow.operators.bash.BashOperator",
                kwargs={"bash_command": "echo '{{ params.foo }}'"},
                context={"params": {"foo": "%%%%hi%%%%"}},
            ),
            [],
            "We ran a bash operator and got our output in task logs via {{ params }}",
        ),
        # TEST PYTHON OPERATOR
        (
            b"%%%%hi-print_magic_string_test_direct%%%%",
            get_isolated_operator_env(
                "airflow.operators.python.PythonOperator",
                kwargs={"_python_callable_qualname": "dags.fn.direct.print_magic_string_test_direct"},
            ),
            [f"{project_root}/tests/resources/fn:/usr/local/airflow/dags/fn"],
            "We ran a python operator and it ran a python callable " "which printed our magic string",
        ),
        (
            b"%%%%hi-print_other_magic_string_outer%%%%",
            get_isolated_operator_env(
                "airflow.operators.python.PythonOperator",
                kwargs={"_python_callable_qualname": "dags.fn.outer.print_other_magic_string_test_outer"},
            ),
            [
                f"{project_root}/tests/resources/fn:/usr/local/airflow/dags/fn",
                f"{project_root}/tests/resources/other_fn:/usr/local/airflow/include/other_fn",
            ],
            "We ran a python operator and it ran a python callable, "
            "which imported a fn defined at top-level, which printed our magic string",
        ),
        (
            b"%%%%hi-print_other_magic_string_inner%%%%",
            get_isolated_operator_env(
                "airflow.operators.python.PythonOperator",
                kwargs={"_python_callable_qualname": "dags.fn.inner.print_other_magic_string_test_inner"},
            ),
            [
                f"{project_root}/tests/resources/fn:/usr/local/airflow/dags/fn",
                f"{project_root}/tests/resources/other_fn:/usr/local/airflow/include/other_fn",
            ],
            "We ran a python operator and it ran a python callable, "
            "which imported a fn defined in the fn, "
            "which printed our magic string",
        ),
        (
            b"%%%%hi- {'foo': 'print_magic_string_test_params%%%%'}",
            get_isolated_operator_env(
                "airflow.operators.python.PythonOperator",
                kwargs={
                    "_python_callable_qualname": "dags.fn.direct.print_params",
                    "op_args": ["%%%%hi-"],
                    "provide_context": True,
                },
                context={"params": {"foo": "print_magic_string_test_params%%%%"}},
            ),
            [f"{project_root}/tests/resources/fn:/usr/local/airflow/dags/fn"],
            "We ran a python operator and it ran a python callable "
            "with provide_context=True and printed an op_arg "
            "and params for our magic string",
        ),
    ]
    with ThreadPoolExecutor() as executor:

        def run_test_in_matrix(matrix_config):
            _magic_string, _env, _volumes, _reason = matrix_config
            _actual = run_in_an_airflow_container(
                docker_client, project_root, dist_file, runtime_image, _env, extra_volumes=_volumes
            )
            assert _magic_string in _actual, _reason

        tests = [executor.submit(run_test_in_matrix, test) for test in test_matrix]
        for test in futures.wait(tests, return_when=FIRST_EXCEPTION)[0]:
            test: Future
            if test.exception():
                raise test.exception()
