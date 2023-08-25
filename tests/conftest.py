import os
import shlex
import shutil
import subprocess
import sys
from io import StringIO
from pathlib import Path
from typing import Tuple

import pytest
import sh

import isolation

# noinspection PyProtectedMember
from isolationctl import (
    EXAMPLE_ENVIRONMENT,
    SH_KWARGS,
    create_registry_docker_container,
    _add,
    DEFAULT_ENVIRONMENTS_FOLDER,
)

manual_tests = pytest.mark.skipif(not bool(os.getenv("MANUAL_TESTS")), reason="requires env setup")


@pytest.fixture(scope="module")
def docker_registry():
    create_registry_docker_container()
    yield
    stop_docker_container("registry")


@pytest.fixture(scope="function")
def astro(environments):
    dockerfile = environments.parent / "Dockerfile"
    # noinspection SpellCheckingInspection
    dot_dockerignore = environments.parent / ".dockerignore"
    dot_gitignore = environments.parent / ".gitignore"
    dot_astro = environments.parent / ".astro"
    dot_astro_yaml = environments.parent / ".astro/config.yaml"
    dot_env = environments.parent / ".env"
    dags = environments.parent / "dags"
    tests_dags = environments.parent / "tests" / "dags"
    tests = environments.parent / "tests"
    include = environments.parent / "include"
    plugins = environments.parent / "plugins"
    readme = environments.parent / "README.md"
    requirements_txt = environments.parent / "requirements.txt"
    packages_txt = environments.parent / "packages.txt"
    airflow_settings_yaml = environments.parent / "airflow_settings.yaml"

    folders = (dot_astro, dags, include, plugins, tests_dags, tests)
    files = (
        dockerfile,
        dot_env,
        requirements_txt,
        packages_txt,
        airflow_settings_yaml,
        dot_astro_yaml,
        dot_dockerignore,
        dot_gitignore,
        readme,
    )
    folder_existed = [folder.exists() for folder in folders]
    file_existed = [file.exists() for file in files]

    sh.astro.dev("init", _in="y", _out=sys.stdout, _err=sys.stderr)

    yield files, folders

    for i, folder in enumerate(folders):
        if not folder_existed[i]:
            shutil.rmtree(folder, ignore_errors=True)
        else:
            print(f"Cowardly not removing {folder.absolute()} - it already existed")
    for i, file in enumerate(files):
        if not file_existed[i]:
            file.unlink(missing_ok=True)
        else:
            print(f"Cowardly not removing {file.absolute()} - it already existed")


@pytest.fixture(scope="function")
def dotgit(environments) -> Path:
    dotgit = environments.parent / Path(".git")
    already_existed = dotgit.exists()
    dotgit.mkdir(parents=True, exist_ok=True)
    yield dotgit, already_existed
    if not already_existed:
        shutil.rmtree(dotgit, ignore_errors=True)


@pytest.fixture(scope="function")
def dotenv(environments) -> Path:
    dotenv_file = environments.parent / Path(".env")
    dotenv_file.touch(exist_ok=True)
    yield dotenv_file
    dotenv_file.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def project_version() -> str:
    return isolation.__version__


@pytest.fixture(scope="session")
def docker_client():
    import docker

    return docker.from_env()


@pytest.fixture(scope="session")
def build(project_root, project_version) -> Tuple[Path, Path]:
    subprocess.run(
        "python -m build",
        cwd=project_root,
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=True,
        shell=True,
    )
    return (
        project_root / "dist/",
        project_root / f"dist/apache_airflow_providers_isolation-{project_version}-py3-none-any.whl",
    )


@pytest.fixture(scope="session")
def dist_folder(build):
    return build[0]


@pytest.fixture(scope="session")
def dist_file(build):
    return build[1]


@pytest.fixture(
    params=[
        "quay.io/astronomer/astro-runtime:8.8.0",
        "quay.io/astronomer/astro-runtime:7.6.0",
        "quay.io/astronomer/astro-runtime:6.6.0",
        "quay.io/astronomer/astro-runtime:5.4.0",
    ]
)
def runtime_image(request):
    return request.param


@pytest.fixture(scope="session")
def has_docker():
    from shutil import which

    return which("docker") is not None


@pytest.fixture(scope="function")
def local_registry(environments):
    from click.testing import CliRunner
    from isolationctl.__main__ import init

    # noinspection PyTypeChecker
    CliRunner().invoke(init, f"-f {environments.name} --yes --no-example --local-registry")
    yield
    stop_docker_container("registry")


@pytest.fixture(scope="function")
def environments():
    # Setup - Add if not already added
    _environments = Path("environments_baz")
    _environments.mkdir(parents=True, exist_ok=True)

    yield _environments

    # Teardown - Remove if not already removed
    try:
        shutil.rmtree(_environments)
    except Exception as e:
        sys.stderr.write(str(e))


@pytest.fixture(scope="function")
def default_environments():
    # Setup - Add if not already added
    _environments = Path(DEFAULT_ENVIRONMENTS_FOLDER)
    _environments.mkdir(parents=True, exist_ok=True)

    yield _environments

    # Teardown - Remove if not already removed
    try:
        shutil.rmtree(_environments)
    except Exception as e:
        sys.stderr.write(str(e))


# noinspection DuplicatedCode
@pytest.fixture(scope="function")
def environment(environments):
    # Setup - Add if not already added
    _environment = environments / EXAMPLE_ENVIRONMENT
    _environment.mkdir(parents=True, exist_ok=True)
    _add(_environment.name, environments.name)

    yield _environment

    # Teardown - Remove if not already removed
    try:
        shutil.rmtree(_environment)
    except Exception as e:
        sys.stderr.write(str(e))


# noinspection DuplicatedCode
@pytest.fixture(scope="function")
def default_environment(environments):
    # Setup - Add if not already added
    _environment = environments / EXAMPLE_ENVIRONMENT
    _environment.mkdir(parents=True, exist_ok=True)
    _add(_environment.name, environments.name)

    yield _environment

    # Teardown - Remove if not already removed
    try:
        shutil.rmtree(_environment)
    except Exception as e:
        sys.stderr.write(str(e))


@pytest.fixture(scope="function")
def dockerfile(environment):
    # Setup - Add if not already added
    _dockerfile = environment / "Dockerfile"
    _dockerfile.touch(exist_ok=True)

    yield _dockerfile

    # Teardown - Remove if not already removed
    try:
        _dockerfile.unlink(missing_ok=True)
    except Exception as e:
        sys.stderr.write(str(e))


@pytest.fixture(scope="function")
def requirements_txt(environment):
    # Setup - Add if not already added
    _requirements_txt = environment / "requirements.txt"
    _requirements_txt.touch(exist_ok=True)

    yield _requirements_txt

    # Teardown - Remove if not already removed
    try:
        _requirements_txt.unlink(missing_ok=True)
    except Exception as e:
        sys.stderr.write(str(e))


@pytest.fixture(scope="function")
def packages_txt(environment):
    # Setup - Add if not already added
    _packages_txt = environment / "packages.txt"
    _packages_txt.touch(exist_ok=True)

    yield _packages_txt

    # Teardown - Remove if not already removed
    try:
        _packages_txt.unlink(missing_ok=True)
    except Exception as e:
        sys.stderr.write(str(e))


def stop_docker_container(name: str):
    try:
        s = StringIO()
        # noinspection PyUnresolvedReferences
        sh.docker.container.ls(shlex.split(f"--filter name={name}" + ' --format="{{.ID}}"'), _out=s)
        _id = s.getvalue()
        sys.stdout.write(f"Attempting to stop docker container (id:{_id} name:{name})...")
        # noinspection PyUnresolvedReferences
        sh.docker.stop(name, **SH_KWARGS)
    except Exception as e:
        sys.stderr.write(str(e))
