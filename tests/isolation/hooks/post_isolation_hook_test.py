import inspect
import sys
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, FIRST_EXCEPTION, Future
from pathlib import Path
from textwrap import dedent
from typing import Callable, Optional, Dict, List, Any

import pytest
from docker import DockerClient
from docker.errors import ContainerError

# noinspection PyProtectedMember
from isolation.hooks.post_isolation import _b64encode_json, XCOM_FILE


def get_env(
    op_qualname: str,
    kwargs: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
):
    """Returns a dict that gets passed to the Docker Container as it's `environment`"""
    return {
        "__ISOLATED_OPERATOR_OPERATOR_QUALNAME": op_qualname,
        "__ISOLATED_OPERATOR_OPERATOR_ARGS": _b64encode_json({"args": [], "kwargs": kwargs}) if kwargs else "",
        "__ISOLATED_OPERATOR_AIRFLOW_CONTEXT": _b64encode_json(context) if context else "",
    }


def skip_no_docker(has_docker):
    """Skips this test if we don't have docker"""
    if not has_docker:
        pytest.skip("skipped, no docker")


# Micro-fn to inject this python into the container
def _run_in_pod():
    from isolation.hooks.post_isolation import PostIsolationHook

    print(PostIsolationHook.run_isolated_task())


def fn_to_source_code(fn: Callable):
    return "".join(dedent(line) for line in inspect.getsourcelines(fn)[0][1:])


def run_in_an_airflow_container(
    docker_client: DockerClient,
    project_root: Path,
    dist_file: Path,
    image: str,
    fn: Callable,
    env: Optional[Dict[str, str]] = None,
    extra_volumes: Optional[List[str]] = None,
):
    """Run PostIsolationHook.run_isolated_task in the given image,
    with a environment and extra_volumes
    Writes the docker container logs (after it's finished) to stdout
    returns the logs as an object, to look for strings against
    """
    if env is None:
        env = {}
    if extra_volumes is None:
        extra_volumes = []

    logs = docker_client.containers.run(
        image=image,
        stderr=True,
        remove=True,
        command=f"""'pip install ./{dist_file.name} && """ f"""python -c "{fn_to_source_code(fn)}" '""",
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


@pytest.mark.slow_integration_test
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

    # JUST INSTALL
    with pytest.raises(
        ContainerError,
        match="RuntimeError: __ISOLATED_OPERATOR_OPERATOR_QUALNAME must be set!",
    ):
        logs = run_in_an_airflow_container(docker_client, project_root, dist_file, runtime_image, _run_in_pod)
        sys.stdout.write(logs.decode())
        assert True, "We get log output when we install the operator " "in the container and launch it"

    # Tuple keys: magic_string, env, extra_volumes, reason
    test_matrix = [
        # BASH OPERATOR
        (
            b"%%%%hi%%%%",
            get_env(
                "airflow.operators.bash.BashOperator",
                kwargs={"bash_command": "echo '%%%%hi%%%%'"},
            ),
            [],
            "We ran a bash operator and saw the expected output in the task logs",
        ),
        (
            b"%%%%hi%%%%",
            get_env(
                "airflow.operators.bash.BashOperator",
                kwargs={"bash_command": "echo '{{ params.foo }}'"},
                context={"params": {"foo": "%%%%hi%%%%"}},
            ),
            [],
            "We ran a bash operator and got our output in task logs via {{ params }}",
        ),
        # PYTHON OPERATOR
        (
            b"%%%%hi-print_magic_string_test_direct%%%%",
            get_env(
                "airflow.operators.python.PythonOperator",
                kwargs={"_python_callable_qualname": "dags.fn.direct.print_magic_string_test_direct"},
            ),
            [f"{project_root}/tests/resources/fn:/usr/local/airflow/dags/fn"],
            "We ran a python operator and it ran a python callable " "which printed our magic string",
        ),
        (
            b"%%%%hi-print_other_magic_string_outer%%%%",
            get_env(
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
            get_env(
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
            get_env(
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

    def run_test_in_matrix(matrix_config):
        _magic_string, _env, _volumes, _reason = matrix_config
        _actual = run_in_an_airflow_container(
            docker_client,
            project_root,
            dist_file,
            runtime_image,
            _run_in_pod,
            _env,
            extra_volumes=_volumes,
        )
        assert _magic_string in _actual, _reason

    with ThreadPoolExecutor() as executor:
        tests = [executor.submit(run_test_in_matrix, test) for test in test_matrix]
        done, _ = futures.wait(tests, return_when=FIRST_EXCEPTION)
        for test in done:
            test: Future
            if test.exception():
                raise test.exception()
