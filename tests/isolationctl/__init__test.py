from isolationctl import (
    extract_kubeconfig_to_str,
    print_table,
    get_provider_package,
    ISOLATION_PROVIDER_PACKAGE,
    add_requirement,
    DOCKER_TAG_PATTERN,
    write_tag_to_dot_env,
    REGISTRY_CONTAINER_URI,
)


def test_docker_tag_pattern_lots_of_output():
    test_input = """
 => [6/3] COPY --chown=astro:0 . .                                                                        8.0s
 => [7/3] COPY apache_airflow_providers_isolation-1.0.0-py3-none-any.whl .                                0.1s
 => [8/3] RUN pip install "./apache_airflow_providers_isolation-1.0.0-py3-none-any.whl[kubernetes]"       8.4s
 => exporting to image                                                                                   11.3s
 => => exporting layers                                                                                  11.3s
 => => writing image sha256:75fe6f0a1870fc15fcfe4179d062769ccacb68f00dd169b7a607b221b41635ee              0.0s
 => => naming to docker.io/isolationv-1_0f009a/airflow:latest                                             0.0s
============================= test session starts ==============================
platform linux -- Python 3.11.4, pytest-7.4.0, pluggy-1.2.0
rootdir: /usr/local/airflow
plugins: anyio-3.7.1
collected 2 items

.astro/test_dag_integrity_default.py .F                                  [100%]
"""
    actual = DOCKER_TAG_PATTERN.match(test_input)
    expected = "isolationv-1_0f009a/airflow"
    assert actual is not None
    assert actual.group(1) == expected


def test_docker_tag_pattern():
    test_input = """
 => => naming to docker.io/isolationv-1_0f009a/airflow:latest         0.0s
"""
    actual = DOCKER_TAG_PATTERN.match(test_input)
    expected = "isolationv-1_0f009a/airflow"
    assert actual is not None
    assert actual.group(1) == expected


def test_extract_kubeconfig_to_str():
    actual = extract_kubeconfig_to_str()
    assert "apiVersion" in actual


def test_get_provider_package():
    actual = get_provider_package(extras="foo,bar")
    expected = f"{ISOLATION_PROVIDER_PACKAGE}[foo,bar]"
    assert actual == expected


def write_tag_to_dot_env_not_exists(dotenv):
    dotenv.unlink(missing_ok=True)
    actual = write_tag_to_dot_env(REGISTRY_CONTAINER_URI, "foo/bar/airflow", dotenv)
    expected = ""
    assert actual == expected


def write_tag_to_dot_env_empty(dotenv):
    dotenv.unlink(missing_ok=True)
    dotenv.touch()
    actual = write_tag_to_dot_env(REGISTRY_CONTAINER_URI, "foo/bar/airflow", dotenv)
    expected = ""
    assert actual == expected


def write_tag_to_dot_env_existing_contents(dotenv):
    test_contents = "ABC=XYZ"
    dotenv.write_text(test_contents)
    actual = write_tag_to_dot_env(REGISTRY_CONTAINER_URI, "foo/bar/airflow", dotenv)
    expected = ""
    assert actual == expected


def test_add_requirement_not_exists(requirements_txt):
    requirements_txt.unlink(missing_ok=True)
    add_requirement(requirements_txt)
    actual = requirements_txt.read_text()
    expected = get_provider_package()
    assert actual == expected


def test_add_requirement_empty(requirements_txt):
    requirements_txt.unlink(missing_ok=True)
    requirements_txt.touch()
    add_requirement(requirements_txt)
    actual = requirements_txt.read_text()
    expected = f"\n{get_provider_package()}"
    assert actual == expected


def test_add_requirement_existing_contents(requirements_txt):
    test_contents = "some_stuff=1.0.0"
    requirements_txt.write_text(test_contents)
    add_requirement(requirements_txt)
    actual = requirements_txt.read_text()
    expected = f"""{test_contents}
{get_provider_package()}"""
    assert actual == expected


def test_print_table_one_col():
    actual = print_table(["aaa"], [["bbbbb"], ["b"]])
    expected = """
aaa     |
--------|
bbbbb   |
b       |
"""
    assert actual == expected


def test_print_table_many_col():
    actual = print_table(["aaa", "bb"], [["bbbbb", "cccc"], ["b", "ccc"]])
    expected = """
aaa     | bb      |
------------------|
bbbbb   | cccc    |
b       | ccc     |
"""
    assert actual == expected
