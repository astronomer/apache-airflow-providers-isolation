import json
import re
import shlex
import sys
import urllib.parse
from io import StringIO
from typing import Literal, List, Optional

import sh
import yaml
import click

MAIN_ECHO_COLOR = "blue"
SKIP_ECHO_COLOR = "grey"

DOCKERFILE = r"""
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# Install system-level packages
COPY packages.txt .
USER root
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | tr '\r\n' '\n' | sed -e 's/#.*//' | \
        xargs apt-get install -y \ --no-install-recommends \
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
YES_OPTION_KWARGS = dict(type=bool, show_envvar=True, is_flag=True, default=False, help="Skip confirmation")
EPILOG_KWARGS = dict(
    epilog="Check out https://github.com/astronomer/apache-airflow-providers-isolation for more details"
)
DOCKER_TAG_PATTERN = re.compile(r".+naming to docker\.io/(?P<tag>[^\s:]+).*", re.DOTALL)
REGISTRY_CONTAINER_NAME = "registry"
REGISTRY_CONTAINER_IMAGE = "registry:2"


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


def build_image_via_docker(tag: str, build_args: Optional[str] = None, push: bool = False) -> str:
    """docker build . --tag=TAG --build-arg="BUILD_ARGS" """
    s = StringIO()
    if not build_args:
        # noinspection PyUnresolvedReferences
        sh.docker.build(shlex.split(f". --tag={tag}"), _in=sys.stdin, _out=s, _err_to_out=True, _tee=True)
    else:
        # noinspection PyUnresolvedReferences
        sh.docker.build(
            ".",
            f"--tag={tag}",
            *([build_args] if build_args else []),
            _in=sys.stdin,
            _out=s,
            _err_to_out=True,
            _tee=True,
        )
    if push:
        # noinspection PyUnresolvedReferences
        sh.docker.push(tag, _in=sys.stdin, _out=s, _err_to_out=True, _tee=True)
    return s.getvalue()


def build_image_via_astro_parse() -> str:
    """astro dev parse, which happens to build the image as a side effect"""
    s = StringIO()
    sh.astro.dev.parse(_in=sys.stdin, _out=s, _err_to_out=True, _tee=True)
    return s.getvalue()


def build_image_via_astro_pytest() -> str:
    """astro dev pytest, which happens to build the image as a side effect"""
    s = StringIO()
    sh.astro.dev.pytest(_in=sys.stdin, _out=s, _err_to_out=True, _tee=True)
    output = s.getvalue()
    sys.stdout.write(output)
    return output


def build_image(
    method: Literal["docker", "astro-parse", "astro-pytest"] = "astro-parse",
    tag: str = "parent",
    build_args: str = None,
    push: bool = False,
    get_docker_tag: bool = True,
) -> Optional[str]:
    if method == "docker":
        output = build_image_via_docker(tag, build_args, push)
    elif method == "astro-parse":
        output = build_image_via_astro_parse()
    elif method == "astro-pytest":
        output = build_image_via_astro_pytest()
    else:
        raise RuntimeError("Methods for building the parent image are astro or docker")
    if get_docker_tag:
        docker_tag = DOCKER_TAG_PATTERN.match(output, re.M)
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
    sh.kubectl.config.view(_out=s)
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
            main_echo("Local docker Image Registry container created! Accessible via localhost:5000 !")
    except sh.ErrorReturnCode_125:
        main_echo(
            "Container is already running, skipping... "
            "To stop and remove this container, run:\n\tdocker rm -f registry"
        )
