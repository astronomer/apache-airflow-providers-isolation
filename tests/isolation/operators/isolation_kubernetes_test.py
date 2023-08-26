import shutil

from isolation.operators.isolation_kubernetes import (
    IsolatedKubernetesPodOperator,
    _set_simple_templates_via_env,
)


def test_isolated_operator():
    from airflow.operators.bash import BashOperator

    expected_image = "foo"
    # noinspection SpellCheckingInspection
    expected_cmds = ["/bin/bash", "-euxc"]
    expected_args = [
        'python -c "from isolation.hooks.post_isolation import PostIsolationHook'
        '\n\nPostIsolationHook.run_isolated_task()\n"',
    ]
    # noinspection SpellCheckingInspection
    expected_env = [
        {
            "name": "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG",
            "value": "false",
            "value_from": None,
        },
        {
            "name": "AIRFLOW__LOGGING__LOG_FORMAT",
            "value": "%(levelname)s - %(message)s",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
            "value": "airflow.operators.bash.BashOperator",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_ARGS",
            "value": "eyJhcmdzIjogW10sICJrd2FyZ3MiOiB7ImJhc2hfY29t"
            "bWFuZCI6ICJlY2hvIHt7IHBhcmFtcy5oaSB9fSIsICJw"
            "YXJhbXMiOiAieydoaSc6ICdoZWxsbyd9IiwgImRlZmF1"
            "bHRfYXJncyI6IHt9fX0=",
            "value_from": None,
        },
    ]
    expected_node_selector = "bar"
    actual = IsolatedKubernetesPodOperator(
        task_id="bar",
        image=expected_image,
        operator=BashOperator,
        bash_command="echo {{ params.hi }}",
        params={"hi": "hello"},
        isolated_operator_kwargs={"node_selector": expected_node_selector},
    )

    assert actual.cmds == expected_cmds
    assert actual.image == expected_image
    assert actual.arguments == expected_args
    for i in range(len(actual.env_vars)):
        assert actual.env_vars[i].name == expected_env[i]["name"]
        assert actual.env_vars[i].value == expected_env[i]["value"]
    assert actual.node_selector == expected_node_selector


def test_set_simple_templates_via_env_nothing():
    actual = _set_simple_templates_via_env((), {}, {})
    expected = {"env_vars": {}}
    assert actual == expected, "We give nothing, we get nothing"


def test_set_simple_templates_via_env_vars_args_templ(mocker, project_root):
    if (project_root / ".astro").exists():
        # the monkeypatch messes with these tests, and some other test isn't cleaning up
        shutil.rmtree(".astro")
    mocker.patch.dict("os.environ", {"AIRFLOW_VAR_FOO": "bar"})
    actual = _set_simple_templates_via_env(("{{ var.value.foo }}",), {}, {})
    expected = {"env_vars": {"AIRFLOW_VAR_FOO": "bar"}}
    assert actual == expected, "We have a var referred to via the args, we push that into the env"


def test_set_simple_templates_via_env_vars_kwargs_templ(mocker, project_root):
    if (project_root / ".astro").exists():
        # the monkeypatch messes with these tests, and some other test isn't cleaning up
        shutil.rmtree(".astro")
    mocker.patch.dict("os.environ", {"AIRFLOW_VAR_FOO": "bar"})
    actual = _set_simple_templates_via_env((), {"param": "{{ var.value.foo }}"}, {})
    expected = {"env_vars": {"AIRFLOW_VAR_FOO": "bar"}}
    assert actual == expected, "We have a var referred to via the kwargs, we push that into the env"


def test_set_simple_templates_via_env_conns_args_templ(mocker, project_root):
    if (project_root / ".astro").exists():
        # the monkeypatch messes with these tests, and some other test isn't cleaning up
        shutil.rmtree(".astro")
    mocker.patch.dict(
        "os.environ",
        {"AIRFLOW_CONN_FOO": "postgres://postgres:postgres@postgres:5432/db"},
    )
    actual = _set_simple_templates_via_env(("{{ conn.foo }}",), {}, {})
    expected = {"env_vars": {"AIRFLOW_CONN_FOO": "postgres://postgres:postgres@postgres:5432/db"}}
    assert actual == expected, "we have a connection referred to via the args, we push that into the env"


def test_set_simple_templates_via_env_conns_kwargs(mocker, project_root):
    if (project_root / ".astro").exists():
        # the monkeypatch messes with these tests, and some other test isn't cleaning up
        shutil.rmtree(".astro")
    mocker.patch.dict(
        "os.environ",
        {"AIRFLOW_CONN_FOO": "postgres://postgres:postgres@postgres:5432/db"},
    )
    actual = _set_simple_templates_via_env((), {"xyz_conn_id": "foo"}, {})
    expected = {"env_vars": {"AIRFLOW_CONN_FOO": "postgres://postgres:postgres@postgres:5432/db"}}
    assert actual == expected, "we have a conn_id referred to via the kwargs, we push that into the env"


def test_set_simple_templates_via_env_conns_kwargs_templ(mocker, project_root):
    if (project_root / ".astro").exists():
        # the monkeypatch messes with these tests, and some other test isn't cleaning up
        shutil.rmtree(".astro")
    mocker.patch.dict(
        "os.environ",
        {"AIRFLOW_CONN_FOO": "postgres://postgres:postgres@postgres:5432/db"},
    )
    actual = _set_simple_templates_via_env((), {"param": "{{ conn.foo }}"}, {})
    expected = {"env_vars": {"AIRFLOW_CONN_FOO": "postgres://postgres:postgres@postgres:5432/db"}}
    assert actual == expected, "we have a connection referred to via the kwargs, we push that into the env"
