This project welcomes contributions. All Pull Requests should include proper testing, documentation, and follow all
existing checks and practices.

<!-- TOC -->

* [Contributing](#contributing)
    * [Versioning](#versioning)
    * [Development](#development)
        * [Pre-Commit](#pre-commit)
        * [Installing Locally](#installing-locally)
    * [Linting](#linting)
    * [Testing](#testing)
    * [Documentation Standards](#documentation-standards)
        * [Inline Module Documentation](#inline-module-documentation)
    * [Functional Testing Standards](#functional-testing-standards)
    * [Publishing your Provider repository for the Astronomer Registry](#publishing-your-provider-repository-for-the-astronomer-registry)

<!-- TOC -->

# Development Workflow

1. Create a branch off `main`
2. Develop, add tests, ensure all tests are passing
3. Push up to GitHub (running pre-commit)
4. Create a PR, get approval
5. Merge the PR to `main`
6. On `main`: Create a tag
    ```shell
    VERSION="v$(python -c 'import isolation; print(isolation.__version__)')"; git tag -d $VERSION; git tag $VERSION
    ```
7. Do any manual or integration testing
8. Push the tag to GitHub `git push origin --tag`, which will create
   a `Draft` [release](https://github.com/astronomer/apache-airflow-providers-isolation/releases) and upload
   to [test.pypi.org](https://test.pypi.org/project/apache-airflow-providers-isolation/) via CICD
9. Approve the [release](https://github.com/astronomer/apache-airflow-providers-isolation/releases) on GitHub, which
   will upload to [pypi.org](https://pypi.org/project/apache-airflow-providers-isolation/) via CICD

## Versioning

- This project follows [Semantic Versioning](https://semver.org/)

## Development

### Pre-Commit

- This project uses pre-commit
- Install it with

```shell
pre-commit install
```

### Installing Locally

Install just the base project with:

```shell
pip install -e '.'
```

Install with extras such as `dev` or `cli`:

```shell
pip install -e '.[dev,cli]'
```

## Linting

This project
uses [`black` (link)](https://black.readthedocs.io/en/stable/), [`blacken-docs` (link)](https://github.com/adamchainz/blacken-docs),
and [`ruff` (link)](https://beta.ruff.rs/). They run with pre-commit but you can run them directly with `ruff check .`
in the root.

## Testing

This project utilizes [Doctests](https://docs.python.org/3/library/doctest.html) and `pytest`.
With the `dev` extras installed, you can run all tests with `pytest` in the root of the project. It will automatically
pick up it's configuration in `pyproject.toml`

## Documentation Standards

Creating excellent documentation is essential for explaining the purpose of your provider package and how to use it.

### Inline Module Documentation

Every Python module, including all hooks, operators, sensors, and transfers, should be documented inline. These
docstrings should be included at the top of each module file and contain three sections separated by blank lines:

- A one-sentence description explaining what the module does.
- A longer description explaining how the module works. This can include details such as code blocks or blockquotes.
- A declarative definition of parameters that you can pass to the module, templated per the example below.

## CICD

The CICD Actions that occur:

- Whenever a push occurs:
    - pre-commit is run to assert those checks and fixes.
- Whenever a tag is pushed:
    - a GitHub release is created in `DRAFT` mode:
    - Notes are automatically created from PRs
    - The package is built and pushed to Test PyPi
- When a GitHub Release is Published:
    - The package is built and pushed to PyPi

## Test PyPi

The package can be built and manually pushed to Test PyPi.
Note: `twine` must be installed
Further instructions are [here](https://packaging.python.org/en/latest/specifications/pypirc/#the-pypirc-file)
and [here](https://packaging.python.org/en/latest/guides/using-testpypi/)

```shell
python -m build
twine check dist/*
twine upload --repository testpypi dist/*
```

You can then install with

```shell
mkdir test && cd test
python -m venv venv
source venv/bin/activate
pip install -i https://test.pypi.org/simple/ "apache-airflow-providers-isolation[cli]"
```

- or just from local via

```shell
mkdir test && cd test
python -m venv venv
source venv/bin/activate
pip install "apache-airflow-providers-isolation[cli] @ file:///absolute/path/to/apache-airflow-providers-isolation/dist/apache_airflow_providers_isolation-X.Y.Z-py3-none-any.whl"
```

This should be done seldom. It should happen via CICD when the tag is created in preparation to publish a release.
Each version can only be uploaded once to Test PyPi so either delete the existing version (and sometimes that involves
waiting an indeterminate amount of time for it to actually be deleted) or get creative with the versioning (but this
should be reverted prior to pushing to the real PyPi)

## Functional Testing Standards

To build your repo into a python wheel that can be tested, follow the steps below:

1. Clone the provider repo.
2. `cd` into provider directory.
3. Run `python3 -m pip install build`.
4. Run `python3 -m build` to build the wheel.
5. Find the .whl file in `/dist/*.whl`.
6. Download the [Astro CLI](https://github.com/astronomer/astro-cli).
7. Create a new project directory, cd into it, and run `astro dev init` to initialize a new astro project.
8. Ensure the Dockerfile contains an [Astro Runtime image that supports _at
   least_ Airflow 2.3.0](https://docs.astronomer.io/astro/runtime-release-notes). For example:

   ```
   FROM quay.io/astronomer/astro-runtime:8.0.0
   ```

9. Copy the `.whl` file to the top level of your project directory.
10. Install `.whl` in your containerized environment by adding the following to your Dockerfile:

```
RUN pip install --user airflow_provider_<PROVIDER_NAME>-0.0.1-py3-none-any.whl
```

11. Copy your sample DAG to the `dags/` folder of your astro project directory.
12. Run `astro dev start` to build the containers and run Airflow locally (you'll need Docker on your machine).
13. When you're done, run `astro dev stop` to wind down the deployment. Run `astro dev kill` to kill the containers and
    remove the local Docker volume. You can also use `astro dev kill` to stop the environment before rebuilding with a
    new `.whl` file.

> Note: If you are having trouble accessing the Airflow webserver locally, there could be a bug in your wheel setup. To
> debug, run `docker ps`, grab the container ID of the scheduler, and run `docker logs <scheduler-container-id>` to
> inspect the logs.

## Publishing your Provider repository for the Astronomer Registry

If you have never submitted your Provider repository for publication to the Astronomer
Registry, [create a new release/tag for your repository](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)
on the `main` branch. Ultimately, the backend of the Astronomer Registry will check for new tags for a Provider
repository to trigger adding the new version of the Provider on the Registry.

> **NOTE:** Tags for the repository must follow typical [semantic versioning](https://semver.org/).

Now that you've created a release/tag, head over to the [Astronomer Registry](https://registry.astronomer.io)
and [fill out the form](https://registry.astronomer.io/publish) with your shiny new Provider repo details!

If your Provider is currently on the Astronomer Registry, simply create a new release/tag will trigger an update to the
Registry and the new version will be published.
