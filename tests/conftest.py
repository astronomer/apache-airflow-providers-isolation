import subprocess
import sys
from pathlib import Path
from typing import Tuple

import docker
import pytest

import isolation


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def project_version() -> str:
    return isolation.__version__


@pytest.fixture(scope="session")
def docker_client():
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


# def pytest_sessionstart(session):
#     __build(Path(__file__).parent.parent)


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
