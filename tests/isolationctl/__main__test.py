import re
import shutil
import sys
from typing import Optional, Pattern

import pytest
import requests
from click import BaseCommand
from click.testing import CliRunner, Result

# noinspection PyProtectedMember
from isolationctl import (
    DEFAULT_ENVIRONMENTS_FOLDER,
    EXAMPLE_ENVIRONMENT,
    find_docker_container,
    _add,
    _get,
    REGISTRY_CONTAINER_URI,
)
from isolationctl.__main__ import remove, add, init, deploy, get
from tests.conftest import manual_tests, stop_docker_container


def click_method_test(
    method: BaseCommand,
    args: str,
    stdin: Optional[str] = None,
    expected_exit_code: Optional[int] = None,
    expected_output_pattern: Optional[Pattern[str]] = None,
) -> Result:
    result: Result = CliRunner().invoke(method, args, **({"input": stdin} if stdin else {}))
    if result.exception:
        raise result.exception
        # sys.stderr.write(result.exception)
        # sys.stderr.write(result.exception.args)
        # sys.stderr.write(result.exception.__cause__)
        # sys.stderr.write(result.exception.__traceback__)
    if result.stdout_bytes:
        sys.stdout.write(result.stdout)
    if result.stderr_bytes:
        sys.stdout.write(result.stderr)
    if expected_exit_code:
        assert result.exit_code == expected_exit_code, "command's exit code was as expected"
    if expected_output_pattern:
        assert expected_output_pattern.match(result.output), "command has expected output"
    return result


@manual_tests
def test_deploy(environments, environment, docker_registry, astro):
    test_method = deploy
    test_args = f"-f {environments.name} --yes --local-registry"
    expected_exit_code = 0
    e = environment.name
    es = environments.name
    expected_output_pattern = re.compile(
        rf"""Building parent image - all other images will derive this one\.\.\..*
.*
Got image tag: '[\w\-_]+/airflow'\.\.\.
Found 1 environment to build\.\.\.
Building environment '{es}/{e}' from parent image '[\w\-_]+/airflow'\.\.\..*
Deployed environment: '{es}/{e}', image: '{REGISTRY_CONTAINER_URI}/[\w\-_]+/airflow/example'!\n""",
        re.DOTALL,
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )
    result = [
        repo
        for repo in requests.get("http://localhost:5000/v2/_catalog").json()["repositories"]
        if "apache-airflow-providers-isolation" in repo and "/airflow/example" in repo
    ]
    assert len(result), "We correctly pushed an image to our localhost:5000 registry"


@manual_tests
def test_init_git(environments, dotgit):
    shutil.rmtree(environments, ignore_errors=True)
    dotgit, already_existed = dotgit
    if not already_existed:
        shutil.rmtree(dotgit, ignore_errors=True)
    else:
        pytest.skip(".git already exists - cowardly skipping this test!")
    test_method = init
    test_args = f"-f {environments.name} --yes --no-example --git"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Creating an environments folder in '{environments.name}' in '{environments.parent.absolute()}'\.\.\.
Initializing --git repository\.\.\.
Initialized empty Git repository in {environments.parent.absolute()}/\.git/
Initialized!"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


@manual_tests
def test_init_astro(environments, astro):
    shutil.rmtree(environments, ignore_errors=True)
    es = environments.name
    p = environments.parent.absolute()
    test_method = init
    test_args = f"-f {environments.name} --yes --no-example --astro"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Creating an environments folder in '{es}' in '{p}'\.\.\.
Initializing --astro airflow project\.\.\.
astro found\.\.\.
Initializing Astro Project with astro dev init\.\.\..*
Initializing Astro project
Pulling Airflow development files from Astro Runtime \d\.\d\.\d
{p}.*
You are not in an empty directory.+Astro project in {p}.*
Initialized!.*""",
        re.DOTALL,
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


# noinspection DuplicatedCode
@manual_tests
def test_init_local(environments, dotenv):
    shutil.rmtree(environments, ignore_errors=True)
    test_method = init
    test_args = f"-f {environments.name} --yes --no-example --local"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Creating an environments folder in '{environments.name}' in '{environments.parent.absolute()}'\.\.\.
Initializing --local connection\.\.\.
kubectl found\.\.\..*
\.env file found\.\.\.
Writing KUBERNETES_DEFAULT Airflow Connection for local kubernetes to \.env file\.\.\.
Writing AIRFLOW__ISOLATED_POD_OPERATOR__\* variables for local kubernetes to \.env file\.\.\.
Initialized!\n""",
        re.DOTALL,
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )
    assert dotenv.exists()
    actual_dotenv = dotenv.read_text()
    assert "AIRFLOW_CONN_KUBERNETES_DEFAULT=kubernetes://?extra__kubernetes__namespace=default" in actual_dotenv

    # Do it again
    dotenv.unlink(missing_ok=True)
    expected_output_pattern = re.compile(
        rf"""Environments folder '{environments.name}' already exists, skipping\.\.\.
Initializing --local connection\.\.\.
kubectl found\.\.\..*
\.env file not found - Creating\.\.\.
Writing KUBERNETES_DEFAULT Airflow Connection for local kubernetes to \.env file\.\.\.
Writing AIRFLOW__ISOLATED_POD_OPERATOR__\* variables for local kubernetes to \.env file\.\.\.
Initialized!\n""",
        re.DOTALL,
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )
    assert dotenv.exists()
    actual_dotenv = dotenv.read_text()
    assert "AIRFLOW_CONN_KUBERNETES_DEFAULT=kubernetes://?extra__kubernetes__namespace=default" in actual_dotenv
    assert "AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID='kubernetes_default'" in actual_dotenv


# noinspection DuplicatedCode
@manual_tests
def test_init_local_registry(environments, docker_registry):
    stop_docker_container("registry")
    shutil.rmtree(environments, ignore_errors=True)
    test_method = init
    test_args = f"-f {environments.name} --yes --no-example --local-registry"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Creating an environments folder in '{environments.name}' in '{environments.parent.absolute()}'\.\.\.
Initialized!
Initializing --local-registry docker Image Registry\.\.\.
Creating a local docker Image Registry container on port 5000\.\.\..*
Local docker Image Registry container already created\.\.\. Recreating\.\.\.
""",
        re.DOTALL,
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )

    results = find_docker_container("registry")
    assert len(results), "We have a docker container running"


def test_init_example(environments):
    shutil.rmtree(environments, ignore_errors=True)
    environment = EXAMPLE_ENVIRONMENT
    test_method = init
    test_args = f"-f {environments.name} --yes --example"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Creating an environments folder in '{environments.name}' in '{environments.parent.absolute()}'\.\.\.
Initializing --example environment\.\.\.
Adding environment '{environment}' in '{environments.name}'\.\.\.
Folder '{environments.name}/{environment}/' created!
'{environments.name}/{environment}/Dockerfile' added!
'{environments.name}/{environment}/requirements\.txt' added!
'requirements\.txt' file found, appending provider\.\.\.
'{environments.name}/{environment}/packages\.txt' added!
Environment '{environment}' in '{environments.name}' created!
Adding 'dags/isolation_provider_example_dag.py' to 'dags/'...
Initialized!\n"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


def test_init_exists(environments):
    test_method = init
    test_args = f"-f {environments.name} --yes --no-example"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Environments folder '{environments.name}' already exists, skipping\.\.\.
Initialized!"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


# noinspection DuplicatedCode
def test_init_does_not_exist(environments):
    shutil.rmtree(environments, ignore_errors=True)
    test_method = init
    test_args = f"-f {environments.name} --yes --no-example"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Creating an environments folder in '{environments.name}' in '{environments.parent.absolute()}'\.\.\.
Initialized!"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


def test__add_default_folder(default_environments):
    environment = default_environments / "foo"
    dockerfile = environment / "Dockerfile"
    requirements_txt = environment / "requirements.txt"
    packages_txt = environment / "packages.txt"
    shutil.rmtree(environment, ignore_errors=True)

    _add(environment.name, DEFAULT_ENVIRONMENTS_FOLDER)

    assert environment.exists()
    assert dockerfile.exists()
    assert requirements_txt.exists()
    assert packages_txt.exists()


def test__add_non_default_folder(environments, environment, dockerfile, requirements_txt, packages_txt):
    shutil.rmtree(environment, ignore_errors=True)

    _add(environment.name, "environments_baz")

    assert environment.exists()
    assert dockerfile.exists()
    assert requirements_txt.exists()
    assert packages_txt.exists()


def test__get(environments, environment):
    actual = _get(env=environment.name, env_folder=environments.name)
    expected = [f"{environments.name}/{environment.name}"]
    assert actual == expected

    actual = _get(env=None, env_folder=environments.name)
    expected = [f"{environments.name}/{environment.name}"]
    assert actual == expected


# noinspection DuplicatedCode
def test_get(environments, environment):
    test_method = get
    test_args = f"{environment.name} -f {environments.name}"
    expected_exit_code = 0
    # noinspection RegExpRepeatedSpace
    expected_output_pattern = re.compile(
        rf"""Environments {15}|
---------------------------|
{environments.name}/{environment.name} {3}|
"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


def test_get_all(environments, environment):
    test_method = get
    (environments / "a").mkdir()
    (environments / "b").mkdir()
    test_args = f"-f {environments.name}"
    expected_exit_code = 0
    # noinspection RegExpRepeatedSpace
    expected_output_pattern = re.compile(
        rf"""Environments {15}|
---------------------------|
{environments.name}/{environment.name} {3}|
environments_baz/a {9}|
environments_baz/b {10}|
"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


# noinspection DuplicatedCode
def test_add(environments, environment, dockerfile, requirements_txt, packages_txt):
    shutil.rmtree(environment, ignore_errors=True)
    test_method = add
    test_args = f"{environment.name} -f {environments.name} --yes"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Adding environment '{environment.name}' in '{environments.name}'\.\.\.
Folder '{environments.name}/{environment.name}/' created!
'{environments.name}/{environment.name}/Dockerfile' added!
'{environments.name}/{environment.name}/requirements\.txt' added!
'requirements\.txt' file found, appending provider\.\.\.
'{environments.name}/{environment.name}/packages\.txt' added!
Environment '{environment.name}' in '{environments.name}' created!\n"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


def test_add_already_exists(environments, environment):
    test_method = add
    test_args = f"{environment.name} -f {environments.name} --yes"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Adding environment '{environment.name}' in '{environments.name}'\.\.\.
Environment '{environment.name}' in '{environments.name}' already exists - to recreate, remove first!"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )


# noinspection DuplicatedCode
def test_remove_non_existent(environments, environment):
    """remove has expected exit-code+output with --yes when environment doesn't exist"""
    shutil.rmtree(environment, ignore_errors=True)
    test_method = remove
    test_args = f"{environment.name} -f {environments.name} --yes"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Removing environment '{environment.name}' in '{environments.name}'\.
Environment '{environment.name}' in '{environments.name}' does not exist"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )
    assert not environment.exists()


# noinspection DuplicatedCode
def test_remove_exists(environments, environment):
    """remove has expected exit-code+output with --yes when environment exists"""
    test_method = remove
    test_args = f"{environment.name} -f {environments.name} --yes"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Removing environment '{environment.name}' in '{environments.name}'\.
Environment '{environment.name}' in '{environments.name}' removed!"""
    )
    # noinspection PyTypeChecker
    click_method_test(
        test_method,
        test_args,
        expected_exit_code=expected_exit_code,
        expected_output_pattern=expected_output_pattern,
    )
    assert not environment.exists()


def test_remove_no_confirm(environments, environment):
    """remove has expected exit-code+output when they don't say yes"""
    test_method = remove
    test_args = f"{environment.name} -f {environments.name}"
    test_stdin = "n"
    expected_exit_code = 0
    expected_output_pattern = re.compile(
        rf"""Removing environment '{environment.name}' in '{environments.name}'\. Continue\? \[Y/n]: n
Skipping\.\.\.
"""
    )
    # noinspection PyTypeChecker
    click_method_test(test_method, test_args, test_stdin, expected_exit_code, expected_output_pattern)
    assert environment.exists(), "if we said no, it's still there"
