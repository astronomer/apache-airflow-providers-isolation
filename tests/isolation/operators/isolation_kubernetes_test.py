from isolation.operators.isolation_kubernetes import IsolatedKubernetesPodOperator


def test_isolated_operator():
    from airflow.operators.bash import BashOperator

    expected_image = "foo"
    # noinspection SpellCheckingInspection
    expected_cmds = ["/bin/bash", "-euxc"]
    expected_args = [
        "python",
        "-c",
        "from isolation.hooks.post_isolation import PostIsolationHook\n\nPostIsolationHook.run_isolated_task()\n",
    ]
    # noinspection SpellCheckingInspection
    expected_env = [
        {"name": "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG", "value": "false", "value_from": None},
        {"name": "AIRFLOW__LOGGING__LOG_FORMAT", "value": "%(levelname)s - %(message)s", "value_from": None},
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
        kubernetes_pod_operator_kwargs={"node_selector": expected_node_selector},
    )

    assert actual.cmds == expected_cmds
    assert actual.image == expected_image
    assert actual.arguments == expected_args
    for i in range(len(actual.env_vars)):
        assert actual.env_vars[i].name == expected_env[i]["name"]
        assert actual.env_vars[i].value == expected_env[i]["value"]
    assert actual.node_selector == expected_node_selector
