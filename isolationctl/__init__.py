import json
import re
import shlex
import sys
import urllib.parse
from io import StringIO
from pathlib import Path
from typing import Literal, List, Optional, Any, Dict
import sh
import yaml
import click

MAIN_ECHO_COLOR = "blue"
SKIP_ECHO_COLOR = "white"
ISOLATION_PROVIDER_PACKAGE = "apache-airflow-providers-isolation"
DOCKERFILE = r"""
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# Install system-level packages
COPY packages.txt .
USER root
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | tr '\r\n' '\n' | sed -e 's/#.*//' | \
        xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*; \
  fi

# Install python packages
COPY requirements.txt .
RUN if grep -Eqx 'apache-airflow\s*[=~>]{1,2}.*' requirements.txt; then \
    echo >&2 "Do not upgrade by specifying 'apache-airflow' in your requirements.txt, change the base image instead!"; \
    exit 1; \
  fi; \
  pip install --no-cache-dir --root-user-action=ignore -r requirements.txt
USER astro
"""
DEFAULT_ENVIRONMENTS_FOLDER = "environments"
EXAMPLE_ENVIRONMENT = "example"
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
DEFAULT_ON_FLAG_KWARGS = dict(type=bool, default=True, show_default=True, show_envvar=True)
DEFAULT_OFF_FLAG_KWARGS = dict(type=bool, default=False, show_default=True, show_envvar=True)
# SH_KWARGS = dict(_in=sys.stdin, _out=sys.stdout, _err=sys.stderr, _tee=True)
SH_KWARGS = dict(_fg=True)
FOLDER_OPTION_ARGS = ["-f", "--folder"]
FOLDER_OPTION_KWARGS = dict(
    type=click.Path(file_okay=False, dir_okay=True, readable=True, writable=True),
    default=DEFAULT_ENVIRONMENTS_FOLDER,
    show_default=True,
    show_envvar=True,
    help="Path containing Environments",
)
ENV_ARG_ARGS = ["env"]
ENV_ARG_KWARGS = dict(type=str, required=True)
YES_OPTION_ARGS = ["--yes", "-y"]
YES_OPTION_KWARGS = dict(
    type=bool, show_default=True, show_envvar=True, is_flag=True, default=False, help="Skip confirmation"
)
EPILOG_KWARGS = dict(
    epilog="Check out https://github.com/astronomer/apache-airflow-providers-isolation for more details"
)
DOCKER_TAG_PATTERN = re.compile(r".*naming to docker\.io/([\w\-/]+)[\s:].*", re.DOTALL)
REGISTRY_CONTAINER_NAME = "registry"
REGISTRY_CONTAINER_IMAGE = "registry:2"
REGISTRY_CONTAINER_URI = "localhost:5000"

# We don't *need* apache-airflow-providers-isolation[kubernetes] in child environments - but - can't hurt to add?
ADD_PROVIDER_TO_ENVIRONMENT = True
AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY = "AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE"
KUBERNETES_CONN_KEY = "AIRFLOW_CONN_KUBERNETES_DEFAULT"


def write_tag_to_dot_env(
    registry: str,
    image: str,
    dot_env: Path,
):
    if not dot_env.exists():
        main_echo(".env file doesn't exist - creating...")
        dot_env.touch(exist_ok=True)

    with dot_env.open(mode="a") as f:
        f.write(f"{AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY}='{registry}/{image}'\n")


def get_provider_package(extras: str = "kubernetes") -> str:
    return f"{ISOLATION_PROVIDER_PACKAGE}[{extras}]"


def add_requirement(requirements_txt: Path, extras: str = "kubernetes") -> None:
    """Add apache-airflow-providers-isolation[kubernetes] to requirements.txt"""
    provider_package = get_provider_package(extras)
    if requirements_txt.exists():
        main_echo(f"'{requirements_txt.name}' file found, appending provider...")
        existing_contents = requirements_txt.read_text()
        prefix = "" if not existing_contents or existing_contents[:-2] == "\n" else "\n"
        with requirements_txt.open("a") as f:
            f.write(f"{prefix}{provider_package}\n")
    elif requirements_txt.exists() and ISOLATION_PROVIDER_PACKAGE in requirements_txt.read_text():
        main_echo(f"'{requirements_txt.name}' already contains {ISOLATION_PROVIDER_PACKAGE}! Skipping...")
    else:
        requirements_txt.touch(exist_ok=True)
        requirements_txt.write_text(f"{provider_package}\n")


def print_table(header: List[str], rows: List[List[str]]):
    longest = max(*[len(row) for row in header], *[len(col) for row in rows for col in row]) + 2
    headers = [col.ljust(longest, " ") for col in header]
    rows = [[col.ljust(longest, " ") for col in row] for row in rows]
    headers = f'\n{" | ".join(headers)}' + " |"
    separator = "-" * (len(headers) - 2) + "|"
    rows = "\n".join([" | ".join([col for col in row]) + " |" for row in rows])
    table_str = "\n".join([headers, separator, rows]) + "\n"
    sys.stdout.write(table_str)
    return table_str


def build_image_via_docker(
    tag: str,
    build_args: Optional[Dict[str, Any]] = None,
    push: bool = False,
    should_log: bool = False,
    directory: str = ".",
) -> str:
    """docker build . --tag=TAG --build-arg="BUILD_ARGS" """
    import docker

    if build_args is None:
        build_args = {}

    client = docker.from_env()

    image, logs = client.images.build(path=directory, tag=tag, buildargs=build_args)
    if should_log:
        for log in logs:
            sys.stdout.write(json.dumps(log))

    if push:
        logs = client.images.push(tag, stream=True, decode=False)
        if should_log:
            for log in logs:
                sys.stdout.write(log.decode())

    return tag


def build_image_via_astro_parse(should_log: bool = False, directory: str = ".") -> str:
    """astro dev parse, which happens to build the image as a side effect"""
    s = StringIO()
    sh.astro.dev.parse(_in=sys.stdin, _out=s, _cwd=directory, _err_to_out=True, _tee=True, _tty_size=(24, 200))
    output = s.getvalue()
    if should_log:
        sys.stdout.write(output)
    return output


def build_image_via_astro_pytest(should_log: bool = False, directory: str = ".") -> str:
    """astro dev pytest, which happens to build the image as a side effect"""
    s = StringIO()
    sh.astro.dev.pytest(_in=sys.stdin, _out=s, _cwd=directory, _err_to_out=True, _tee=True, _tty_size=(24, 500))
    output = s.getvalue()
    if should_log:
        sys.stdout.write(output)
    return output


def build_image(
    method: Literal["docker", "astro-parse", "astro-pytest"] = "astro-parse",
    tag: str = "parent",
    build_args: Optional[Dict[str, Any]] = None,
    push: bool = False,
    get_docker_tag: bool = True,
    should_log: bool = True,
    directory: str = ".",
) -> Optional[str]:
    """Build a docker image via one of multiple methods"""
    docker_tag, output = None, None
    if method == "docker":
        docker_tag = build_image_via_docker(tag, build_args, push, should_log, directory)
    elif method == "astro-parse":
        output = build_image_via_astro_parse(should_log, directory)
    elif method == "astro-pytest":
        output = build_image_via_astro_pytest(should_log, directory)
    else:
        raise RuntimeError("Methods for building the parent image are astro or docker")

    if get_docker_tag:
        if method == "docker":
            main_echo(f"Got image tag: '{docker_tag}'...")
            return docker_tag
        else:
            docker_tag = DOCKER_TAG_PATTERN.match(output)
            if docker_tag:
                docker_tag = docker_tag.group(1)
                main_echo(f"Got image tag: '{docker_tag}'...")
                return docker_tag
            else:
                raise click.ClickException(
                    f"Error! Cannot find docker_tag with regex {DOCKER_TAG_PATTERN}. " f"Unable to proceed... Exiting!"
                )


def main_echo(msg) -> None:
    click.echo(click.style(msg, fg=MAIN_ECHO_COLOR, bold=True))


def skip_echo(msg) -> None:
    click.echo(click.style(msg, fg=SKIP_ECHO_COLOR))


def confirm_or_exit(msg: str, yes: bool) -> None:
    if not yes:
        click.confirm(click.style(msg + " Continue?", fg=MAIN_ECHO_COLOR, bold=True), default=True, abort=True)
    else:
        main_echo(msg)


def confirm_or_skip(msg: str, yes: bool) -> bool:
    if not yes:
        if not click.confirm(click.style(msg + " Continue?", fg=MAIN_ECHO_COLOR, bold=True), default=True):
            skip_echo("Skipping...")
            return True
    else:
        main_echo(msg)
    return False


class AliasedGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        # noinspection DuplicatedCode
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        matches = [x for x in self.list_commands(ctx) if x.startswith(cmd_name)]
        if not matches:
            return None
        elif len(matches) == 1:
            return click.Group.get_command(self, ctx, matches[0])
        ctx.fail(f"Too many matches: {', '.join(sorted(matches))}")

    def resolve_command(self, ctx, args):
        # always return the full command name
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args


def add_folder_if_not_default(
    folder: str, prefix=" in '", suffix="'", default: str = DEFAULT_ENVIRONMENTS_FOLDER
) -> str:
    return f"{prefix}{folder}{suffix}" if folder != default else ""


def extract_kubeconfig_to_str(replace_for_docker: bool = True) -> str:
    from io import StringIO

    s = StringIO()
    # noinspection PyUnresolvedReferences
    sh.kubectl.config.view("--raw", _out=s)
    return urllib.parse.quote(
        yaml.dump(
            yaml.safe_load(
                s.getvalue().replace("127.0.0.1", ("host.docker.internal" if replace_for_docker else "127.0.0.1"))
            )
        )
    )


def find_docker_container(name: str):
    from io import StringIO

    out = StringIO()
    # noinspection PyUnresolvedReferences
    sh.docker.ps(shlex.split(f"--all --filter='name={name}' --format=json"), _out=out)
    result = out.getvalue()
    if result:
        try:
            return json.loads(out.getvalue())
        except Exception as e:
            sys.stderr.write(str(e))
    return None


def create_registry_docker_container():
    # noinspection PyUnresolvedReferences
    try:
        if find_docker_container(REGISTRY_CONTAINER_NAME):
            main_echo("Local docker Image Registry container already created... Recreating...")
            # noinspection PyUnresolvedReferences
            sh.docker.start(REGISTRY_CONTAINER_NAME, **SH_KWARGS)
        else:
            # noinspection PyUnresolvedReferences
            sh.docker.run(
                shlex.split(
                    "-d -p 5000:5000 --restart=always " f"--name {REGISTRY_CONTAINER_NAME} {REGISTRY_CONTAINER_IMAGE}"
                ),
                **SH_KWARGS,
            )
            main_echo(f"Local docker Image Registry container created! Accessible via {REGISTRY_CONTAINER_URI} !")
    except sh.ErrorReturnCode_125:
        main_echo(
            "Container is already running, skipping... "
            "To stop and remove this container, run:\n\tdocker rm -f registry"
        )


def _add(env_name: str, folder_name: str):
    """Add an environment
    - Create the folder
    - Add a Dockerfile
    - Add a requirements.txt
    - Add a packages.txt
    """
    env = Path(env_name)
    folder = Path(folder_name)
    (folder / env).mkdir(parents=True, exist_ok=True)
    main_echo(f"Folder '{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/' created!")

    dockerfile = folder / env / "Dockerfile"
    dockerfile.touch()
    dockerfile.write_text(DOCKERFILE)
    main_echo(f"'{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/Dockerfile' added!")

    requirements_txt = folder / env / "requirements.txt"
    requirements_txt.touch()
    main_echo(f"'{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/requirements.txt' added!")
    if ADD_PROVIDER_TO_ENVIRONMENT:
        add_requirement(requirements_txt)

    packages_txt = folder / env / "packages.txt"
    packages_txt.touch()
    main_echo(f"'{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/packages.txt' added!")


def _get(
    env: Optional[str],
    env_folder: Optional[str] = DEFAULT_ENVIRONMENTS_FOLDER,
    _print: bool = True,
    full_path: bool = False,
) -> List[str]:
    header = ["Environments"]
    if env:
        envs = [
            add_folder_if_not_default(env_folder, prefix="", suffix="/", default="" if full_path else None)
            + folder.name
            for folder in Path(env_folder).iterdir()
            if folder.name == env
        ]
    else:
        envs = [
            add_folder_if_not_default(env_folder, prefix="", suffix="/", default="" if full_path else None)
            + folder.name
            for folder in Path(env_folder).iterdir()
        ]
    if len(envs) and _print:
        print_table(header, [[e] for e in envs])
    return envs
