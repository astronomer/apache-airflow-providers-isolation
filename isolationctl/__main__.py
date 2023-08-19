import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional, List

import click
import sh

from isolationctl import (
    AliasedGroup,
    main_echo,
    confirm_or_exit,
    confirm_or_skip,
    DOCKERFILE,
    CONTEXT_SETTINGS,
    DEFAULT_ON_FLAG_KWARGS,
    DEFAULT_OFF_FLAG_KWARGS,
    SH_KWARGS,
    FOLDER_OPTION_ARGS,
    FOLDER_OPTION_KWARGS,
    YES_OPTION_ARGS,
    YES_OPTION_KWARGS,
    EPILOG_KWARGS,
    add_folder_if_not_default,
    extract_kubeconfig_to_str,
    build_image,
    ENV_ARG_ARGS,
    ENV_ARG_KWARGS,
    print_table,
    DEFAULT_ENVIRONMENTS_FOLDER,
    create_registry_docker_container,
)

log = logging.getLogger(__name__)
log.setLevel(
    os.getenv("LOG_LEVEL", logging.INFO),
)
log.addHandler(logging.StreamHandler())


@click.group(cls=AliasedGroup, context_settings=CONTEXT_SETTINGS, **EPILOG_KWARGS)
@click.version_option(package_name="apache_airflow_providers_isolation")
def cli():
    """
    ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    ██▄██░▄▄█▀▄▄▀█░██░▄▄▀█▄░▄██▄██▀▄▄▀█░▄▄▀█▀▄▀█▄░▄█░█
    ██░▄█▄▄▀█░██░█░██░▀▀░██░███░▄█░██░█░██░█░█▀██░██░█
    █▄▄▄█▄▄▄██▄▄██▄▄█▄██▄██▄██▄▄▄██▄▄██▄██▄██▄███▄██▄▄
    ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
    """


# noinspection SpellCheckingInspection
@cli.command(**EPILOG_KWARGS)
@click.option(*YES_OPTION_ARGS, **YES_OPTION_KWARGS)
@click.option(
    "-r",
    "--registry",
    default=None,
    show_envvar=True,
    type=str,
    help="The registry to push the image to. e.g. aaa.dkr.ecr.us-east-2.amazonaws.com/bbb",
)
@click.option(
    "--local/--no-local",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Shortcut to utilize a local Docker Registry container running at localhost:5000",
)
@click.option(
    "--astro-deploy/--no-astro-deploy",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Use the Astro CLI to deploy the base project, as well as environments",
)
@click.option(
    "-e", "--environment", "env", default=None, show_envvar=True, type=str, help="What environment to deploy."
)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def deploy(yes: bool, registry: Optional[str], local: str, astro_deploy: bool, env: str, folder: str):
    """Build and deploy environments (alias: d).
    \n
    Build both the parent project and either a specific or all child ENVIRONMENTs
    """
    if local and registry:
        raise click.ClickException("--local and --registry cannot be used simultaneously")

    if local:
        registry = "localhost:5000"

    confirm_or_exit("Building parent image - all other images will derive this one...", yes)
    parent_image = build_image(method="astro-parse")
    environments = _get(env, folder, _print=False, full_path=True)
    if len(environments):
        main_echo(
            f"Found {len(environments)} {'environment' if len(environments) == 1 else 'environments'} to build..."
        )
        for _environment in environments:
            [_, _env] = _environment.split("/")
            child_tag = f"{registry}/{parent_image}/{_env}"
            if not confirm_or_skip(f"Building environment '{_environment}' from parent image '{parent_image}'...", yes):
                build_image(
                    method="docker",
                    tag=child_tag,
                    build_args=f'--build-arg="BASE_IMAGE={parent_image}"',
                    push=True,
                    get_docker_tag=False,
                )
                main_echo(f"Deployed environment: '{_environment}', image: '{child_tag}'!")
    else:
        raise click.ClickException("Found 0 environments to build... Exiting!")

    if astro_deploy:
        sh.astro.deploy(**SH_KWARGS)


@cli.command(**EPILOG_KWARGS)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
@click.option("--example/--no-example", **DEFAULT_ON_FLAG_KWARGS, help="Initialize with an example environment")
@click.option(
    "--local/--no-local",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Additional initialization for a local project to use with "
    "an already initialized local Kubernetes and an Astro CLI project",
)
@click.option(
    "--local-registry/--no-local-registry",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Additional initialization for a local project to use a local Docker Registry",
)
@click.option(
    "--astro/--no-astro",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Additional initialization,  create an Astro CLI project in the current directory with `astro dev init`",
)
@click.option(
    "--git/--no-git",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Additional initialization, create a git repository in the current directory with `git init`",
)
@click.option(
    "--cicd",
    type=click.Choice(
        [
            "Codebuild",
            "DevOps",
            "Bitbucket",
            "CircleCI",
            "DroneCI",
            "Jenkins",
            "GitHub",
            "Gitlab",
            "none",
        ],
        case_sensitive=False,
    ),
    default=None,
    show_default=True,
    show_envvar=True,
    help="Additional initialization, add a CICD template file in the current project",
)
@click.option(*YES_OPTION_ARGS, **YES_OPTION_KWARGS)
@click.pass_context
def init(
    ctx, folder: str, example: bool, local: bool, local_registry: bool, astro: bool, git: bool, cicd: str, yes: bool
):
    """Initialize environments (alias: i).
    \n
    Initialize environments for usage with this CLI. Intended to be used with
    an existing Airflow project."""
    folder_path = Path(folder)
    if folder_path.exists():
        main_echo(f"Environments folder '{folder}' already exists, skipping...")
    else:
        confirm_or_exit(
            f"Creating an environments folder{add_folder_if_not_default(folder)} in '{os.getcwd()}'...", yes
        )
        folder_path.mkdir(parents=True, exist_ok=True)

    if example:
        main_echo("Initializing --example environment...")
        ctx.invoke(add, env="example", yes=yes, folder=folder)

    if astro:
        main_echo("Initializing --astro airflow project...")
        if shutil.which("astro") is not None:
            main_echo("astro found...")
            if not confirm_or_skip("Initializing Astro Project with astro dev init...", yes):
                sh.astro.dev.init(_in="y", _out=sys.stdout, _err=sys.stderr)
        else:
            main_echo("Cannot find astro. Expecting astro to be installed. Skipping...")

    if git:
        main_echo("Initializing --git repository...")
        if shutil.which("git") is not None:
            # noinspection PyUnresolvedReferences
            from sh.contrib import git as _git

            _git.init(_out=sys.stdout)

    if cicd:
        main_echo("Initializing --cicd templates...")
        if not Path(".git").exists():
            main_echo(
                "--cicd doesn't make sense unless this is a git repository. " "Use --git to initialize. Skipping..."
            )
        raise NotImplementedError("--cicd is not yet implemented")

    if local:
        main_echo("Initializing --local connection...")
        if shutil.which("kubectl") is not None:
            main_echo("kubectl found...")
            encoded_kubeconfig = extract_kubeconfig_to_str()
            kubernetes_conn_key = "AIRFLOW_CONN_KUBERNETES"
            kubernetes_conn_value = (
                "kubernetes://?extra__kubernetes__namespace=default"
                f"&extra__kubernetes__kube_config={encoded_kubeconfig}"
            )
            dotenv = Path(".env")
            if not dotenv.exists():
                if not confirm_or_skip(".env file not found - Creating...", yes):
                    dotenv.touch(exist_ok=True)
                    if not confirm_or_skip("Writing local KUBERNETES Airflow Connection to .env file...", yes):
                        dotenv.write_text(f"{kubernetes_conn_key}={kubernetes_conn_value}")
            else:
                main_echo(".env file found...")
                if not confirm_or_skip("Writing local KUBERNETES Airflow Connection to .env file...", yes):
                    dotenv.write_text(f"{kubernetes_conn_key}={kubernetes_conn_value}")
        else:
            main_echo("Cannot find kubectl. " "Expecting Kubernetes to be set up for local execution. Skipping...")

    main_echo("Initialized!")

    if local_registry:
        main_echo("Initializing --local-registry docker Image Registry...")
        if shutil.which("docker") is not None:
            if not confirm_or_skip("Creating a local docker Image Registry container on port 5000...", yes):
                create_registry_docker_container()
        else:
            main_echo("Cannot find Docker! Cannot run local Image Registry! Skipping...")


@cli.group(**EPILOG_KWARGS)
def environment():
    """Manage environments (alias: env, e).
    \n
    An environment consists of a child image that inherits the parent image of an Airflow project.
    Both images should contain all DAGs and other supporting source code.
    The child image may contain different dependencies or system packages from the parent."""


def _add(env_name: str, folder_name: str):
    env = Path(env_name)
    folder = Path(folder_name)
    (folder / env).mkdir(parents=True, exist_ok=True)
    main_echo(f"Folder '{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/' created!")

    dockerfile = folder / env / "Dockerfile"
    dockerfile.touch()
    dockerfile.write_text(DOCKERFILE)
    main_echo(f"- '{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/Dockerfile' added!")

    requirements_txt = folder / env / "requirements.txt"
    requirements_txt.touch()
    main_echo(f"- '{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/requirements.txt' added!")

    packages_txt = folder / env / "packages.txt"
    packages_txt.touch()
    main_echo(f"- '{add_folder_if_not_default(folder_name, prefix='', suffix='/')}{env}/packages.txt' added!")


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
            + folder.stem
            for folder in Path(env_folder).iterdir()
            if folder.stem == env
        ]
    else:
        envs = [
            add_folder_if_not_default(env_folder, prefix="", suffix="/", default="" if full_path else None)
            + folder.stem
            for folder in Path(env_folder).iterdir()
        ]
    if len(envs) and _print:
        print_table(header, [envs])
    return envs


# noinspection PyShadowingBuiltins
@environment.command(**EPILOG_KWARGS)
@click.argument(*ENV_ARG_ARGS, **{**ENV_ARG_KWARGS, **dict(required=False)})
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def get(env: str, folder: str):
    """Get environments or specific ENVIRONMENT"""
    _get(env, folder)


# noinspection DuplicatedCode
@environment.command(**EPILOG_KWARGS)
@click.argument(*ENV_ARG_ARGS, **ENV_ARG_KWARGS)
@click.option(*YES_OPTION_ARGS, **YES_OPTION_KWARGS)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def add(env: str, yes: bool, folder: str):
    """Add a new ENVIRONMENT initialized from a template.
    \n
    An environment consists of a child image that inherits the parent image of an Airflow project.
    Both images should contain all DAGs and other supporting source code.
    The child image may contain different dependencies or system packages from the parent.
    """
    confirm_or_exit(f"Adding environment '{env}'{add_folder_if_not_default(folder)}...", yes)
    _environment = Path(folder) / env
    if _environment.exists():
        main_echo(
            f"Environment '{env}'{add_folder_if_not_default(folder)} already exists " f"- to recreate, remove first!"
        )
    else:
        _add(_environment.stem, folder)
        main_echo(f"Environment '{env}'{add_folder_if_not_default(folder)} created!")


# noinspection DuplicatedCode
@environment.command(**EPILOG_KWARGS)
@click.argument(*ENV_ARG_ARGS, **ENV_ARG_KWARGS)
@click.option(*YES_OPTION_ARGS, **YES_OPTION_KWARGS)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def remove(env: str, yes: bool, folder: str):
    """Remove an ENVIRONMENT."""
    confirm_or_exit(f"Removing environment '{env}'{add_folder_if_not_default(folder)}.", yes)
    _environment = Path(folder) / env
    if not _environment.exists():
        main_echo(f"Environment '{env}'{add_folder_if_not_default(folder)} does not exist!")
    else:
        shutil.rmtree(_environment)
        main_echo(f"Environment '{env}'{add_folder_if_not_default(folder)} removed!")


if __name__ == "__main__":
    cli()
