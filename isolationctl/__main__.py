import logging
import os
import shutil
import sys
import textwrap
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError
from urllib.request import urlretrieve

import click
import sh

from isolationctl import (
    AliasedGroup,
    main_echo,
    confirm_or_exit,
    confirm_or_skip,
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
    create_registry_docker_container,
    ISOLATION_PROVIDER_PACKAGE,
    add_requirement,
    _add,
    _get,
    write_tag_to_dot_env,
    REGISTRY_CONTAINER_URI,
    EXAMPLE_ENVIRONMENT,
    KUBERNETES_CONN_KEY,
    AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY,
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
    required=False,
    show_envvar=True,
    type=str,
    help="The registry to push environment images to "
    "such as `docker.io/path/to/my/airflow` "
    "or `aaa.dkr.ecr.us-east-2.amazonaws.com/bbb`",
)
@click.option(
    "--local-registry/--no-local-registry",
    **DEFAULT_OFF_FLAG_KWARGS,
    help=f"Shortcut to utilize a local Docker Registry container running at {REGISTRY_CONTAINER_URI}",
)
@click.option(
    "--astro/--no-astro",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Use the Astro CLI to deploy the base project, as well as environments",
)
@click.option(
    "-e",
    "--environment",
    "env",
    default=None,
    show_default=True,
    show_envvar=True,
    type=str,
    help="What environment to deploy.",
)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def deploy(
    yes: bool, registry: Optional[str], local_registry: Optional[str], astro: bool, env: Optional[str], folder: str
):
    """Build and deploy environments (alias: d).
    \n
    Build both the parent project and either a specific or all child ENVIRONMENTs
    """
    if local_registry and registry:
        raise click.ClickException("--local-registry and --registry cannot be used simultaneously")

    if local_registry:
        registry = REGISTRY_CONTAINER_URI

    confirm_or_exit("Building parent image - all other images will derive this one...", yes)
    parent_image = build_image(method="astro-parse", get_docker_tag=True, push=False, should_log=True)
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
                    build_args={"BASE_IMAGE": parent_image},
                    push=True,
                    get_docker_tag=False,
                    should_log=True,
                    directory=_environment,
                )
                main_echo(f"Deployed environment: '{_environment}', image: '{child_tag}'!")
    else:
        raise click.ClickException("Found 0 environments to build... Exiting!")

    if astro:
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
    "--dependency/--no-dependency",
    **DEFAULT_OFF_FLAG_KWARGS,
    help="Additional initialization, add `apache-airflow-providers-isolation` to a `requirements.txt` file. "
    "Unnecessary with `--astro`, as it's automatically added.",
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
    ctx,
    folder: str,
    example: bool,
    local: bool,
    local_registry: bool,
    astro: bool,
    git: bool,
    cicd: str,
    yes: bool,
    dependency: bool,
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
        ctx.invoke(add, env=EXAMPLE_ENVIRONMENT, yes=yes, folder=folder)

        if not confirm_or_skip("Adding 'dags/isolation_provider_example_dag.py' to 'dags/'...", yes):
            example_dag = (
                "https://raw.githubusercontent.com/astronomer/apache-airflow-providers-isolation/"
                "main/isolation/example_dags/isolation_provider_example_dag.py"
            )
            try:
                Path("dags").mkdir(exist_ok=True, parents=True)
                urlretrieve(example_dag, "dags/isolation_provider_example_dag.py")
            except HTTPError as e:
                main_echo(f"Error finding example DAG: {example_dag} -- Reason:{e.reason}")

    if astro:
        requirements_txt = Path("requirements.txt")
        main_echo("Initializing --astro airflow project...")
        if shutil.which("astro") is not None:
            main_echo("astro found...")
            if not confirm_or_skip("Initializing Astro Project with astro dev init...", yes):
                sh.astro.dev.init(_in="y", _out=sys.stdout, _err=sys.stderr)
                add_requirement(requirements_txt)
        else:
            main_echo("Cannot find astro. Expecting astro to be installed. Skipping...")

    if dependency and not astro:
        requirements_txt = Path("requirements.txt")
        if not confirm_or_skip(f"Adding '{ISOLATION_PROVIDER_PACKAGE}' to '{requirements_txt.name}'...", yes):
            add_requirement(requirements_txt)

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
        dot_env = Path(".env")
        if astro and local_registry:
            if shutil.which("astro") is not None:
                if not confirm_or_skip(
                    "--astro is also set: Running `astro dev parse` to fetch the initial parent image tag, "
                    "and persisting to key: 'AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE' in the '.env' file...",
                    yes,
                ):
                    tag = build_image("astro-parse", should_log=False)
                    if tag:
                        write_tag_to_dot_env(REGISTRY_CONTAINER_URI, tag, dot_env)
                        main_echo(
                            f"Wrote '{AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY}='{REGISTRY_CONTAINER_URI}/{tag}'"
                            " to  .env..."
                        )
                    else:
                        main_echo("Unable to find tag from `astro dev parse` output. Skipping...")
            else:
                main_echo("Cannot find astro. Expecting astro to be installed. Skipping...")

        if shutil.which("kubectl") is not None:
            main_echo("kubectl found...")
            encoded_kubeconfig = extract_kubeconfig_to_str()
            kubernetes_conn_value = (
                "kubernetes://?extra__kubernetes__namespace=default"
                f"&extra__kubernetes__kube_config={encoded_kubeconfig}"
            )
            if not dot_env.exists():
                if not confirm_or_skip(".env file not found - Creating...", yes):
                    dot_env.touch(exist_ok=True)
            else:
                main_echo(".env file found...")
            if not confirm_or_skip(
                "Writing KUBERNETES_DEFAULT Airflow Connection for local kubernetes to .env file...", yes
            ):
                dot_env.open("a").write(f"{KUBERNETES_CONN_KEY}={kubernetes_conn_value}\n")
            if not confirm_or_skip(
                "Writing AIRFLOW__ISOLATED_POD_OPERATOR__* variables for local kubernetes to .env file...", yes
            ):
                dot_env.open("a").write(
                    textwrap.dedent(
                        """
                        AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID='kubernetes_default'
                        AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_PULL_POLICY="Never"
                        """
                    )
                )
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
    """Manage environments (alias: env, e). Environments are contained in folders inside an Airflow project.
    \n
    An environment inherits from a parent image (the root `Dockerfile`).
    Both images should contain all DAGs and other supporting source code.
    The child image may contain different dependencies or system packages from the parent.

    The expected setup is as follows:
    ```shell
    $ tree
    ├── Dockerfile
    ├── dags
    │  └── my_dag.py
    ├── environments
    │  ├── env_one
    │  │   ├── Dockerfile
    │  │   ├── packages.txt
    │  │   └── requirements.txt
    │  └── env_two
    │      ├── Dockerfile
    │      ├── packages.txt
    │      └── requirements.txt
    ├── packages.txt
    └── requirements.txt
    ```
    """


# noinspection PyShadowingBuiltins
@environment.command(**EPILOG_KWARGS)
@click.argument(*ENV_ARG_ARGS, **{**ENV_ARG_KWARGS, **dict(required=False)})
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def get(env: Optional[str], folder: str):
    """Get environments or specific ENVIRONMENT"""
    _get(env, folder)


# noinspection DuplicatedCode
@environment.command(**EPILOG_KWARGS)
@click.argument(*ENV_ARG_ARGS, **ENV_ARG_KWARGS)
@click.option(*YES_OPTION_ARGS, **YES_OPTION_KWARGS)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def add(env: str, yes: bool, folder: str):
    """Add an ENVIRONMENT initialized from a template
    \n
    An environment inherits from a parent image (the root `Dockerfile`).
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
        _add(_environment.name, folder)
        main_echo(f"Environment '{env}'{add_folder_if_not_default(folder)} created!")


# noinspection DuplicatedCode
@environment.command(**EPILOG_KWARGS)
@click.argument(*ENV_ARG_ARGS, **ENV_ARG_KWARGS)
@click.option(*YES_OPTION_ARGS, **YES_OPTION_KWARGS)
@click.option(*FOLDER_OPTION_ARGS, **FOLDER_OPTION_KWARGS)
def remove(env: str, yes: bool, folder: str):
    """Removes an ENVIRONMENT (which is the same as `rm -rf environments/environment`)"""
    if not confirm_or_skip(f"Removing environment '{env}'{add_folder_if_not_default(folder)}.", yes):
        _environment = Path(folder) / env
        if not _environment.exists():
            main_echo(f"Environment '{env}'{add_folder_if_not_default(folder)} does not exist!")
        else:
            shutil.rmtree(_environment)
            main_echo(f"Environment '{env}'{add_folder_if_not_default(folder)} removed!")


if __name__ == "__main__":
    cli()
