# Contributing

## Pre-commit
- This project uses pre-commit
- Install it with
```shell
pre-commit install
```

## Installing Locally
- Just the base project
```shell
pip install -e '.'
```
- With extras such as `dev`
```shell
pip install -e '.[dev]'
```

## Testing
This project utilizes [Doctests](https://docs.python.org/3/library/doctest.html) and `pytest`
- run all tests with `pytest`

[//]: # (### TESTING NOTE)
[//]: # (This project utilizes `pytest-xdist` which interferes with test output and debuggers. Please disable it if you need either of these functionalities.)
[//]: # (Read more here: https://pytest-xdist.readthedocs.io/en/stable/known-limitations.html#output-stdout-and-stderr-from-workers)


## Documentation Standards

Creating excellent documentation is essential for explaining the purpose of your provider package and how to use it.

### Inline Module Documentation

Every Python module, including all hooks, operators, sensors, and transfers, should be documented inline via [sphinx-templated docstrings](https://pythonhosted.org/an_example_pypi_project/sphinx.html). These docstrings should be included at the top of each module file and contain three sections separated by blank lines:
- A one-sentence description explaining what the module does.
- A longer description explaining how the module works. This can include details such as code blocks or blockquotes. For more information Sphinx markdown directives, read the [Sphinx documentation](https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#directive-code-block).
- A declarative definition of parameters that you can pass to the module, templated per the example below.

### README

The README for your provider package should give users an overview of what your provider package does. Specifically, it should include:

- High-level documentation about the provider's service.
- Steps for building a connection to the service from Airflow.
- What modules exist within the package.
- An exact set of dependencies and versions that your provider has been tested with.
- Guidance for contributing to the provider package.

## Functional Testing Standards

To build your repo into a python wheel that can be tested, follow the steps below:

1. Clone the provider repo.
2. `cd` into provider directory.
3. Run `python3 -m pip install build`.
4. Run `python3 -m build` to build the wheel.
5. Find the .whl file in `/dist/*.whl`.
6. Download the [Astro CLI](https://github.com/astronomer/astro-cli).
7. Create a new project directory, cd into it, and run `astro dev init` to initialize a new astro project.
8. Ensure the Dockerfile contains an [Astro Runtime image that supports _at least_ Airflow 2.3.0](https://docs.astronomer.io/astro/runtime-release-notes). For example:

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
13. When you're done, run `astro dev stop` to wind down the deployment. Run `astro dev kill` to kill the containers and remove the local Docker volume. You can also use `astro dev kill` to stop the environment before rebuilding with a new `.whl` file.

> Note: If you are having trouble accessing the Airflow webserver locally, there could be a bug in your wheel setup. To debug, run `docker ps`, grab the container ID of the scheduler, and run `docker logs <scheduler-container-id>` to inspect the logs.

## Publishing your Provider repository for the Astronomer Registry

If you have never submitted your Provider repository for publication to the Astronomer Registry, [create a new release/tag for your repository](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) on the `main` branch. Ultimately, the backend of the Astronomer Registry will check for new tags for a Provider repository to trigger adding the new version of the Provider on the Registry.

> **NOTE:** Tags for the repository must follow typical [semantic versioning](https://semver.org/).

Now that you've created a release/tag, head over to the [Astronomer Registry](https://registry.astronomer.io) and [fill out the form](https://registry.astronomer.io/publish) with your shiny new Provider repo details!

If your Provider is currently on the Astronomer Registry, simply create a new release/tag will trigger an update to the Registry and the new version will be published.
