import os
import time
from pprint import pprint

from kubernetes.client import (
    V1PodSpec,
    V1PodStatus,
    V1EnvVar,
    V1Volume,
    V1HostPathVolumeSource,
    V1Container,
    V1VolumeMount,
)
from pytest_mock import MockerFixture

import isolationctl
from isolation.operators.isolation import IsolatedOperator
from isolation.operators.isolation_kubernetes import IsolatedKubernetesPodOperator
from tests.conftest import manual_tests


@manual_tests
def test_bash_isolated_operator_integration(project_root, build, dist_file, dist_folder, mocker: MockerFixture):
    from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
    from airflow.operators.bash import BashOperator

    # Requires astro
    # tag = build_image("astro-parse", should_log=False)

    # Requires kubectl and KUBECONFIG
    encoded_kubeconfig = isolationctl.extract_kubeconfig_to_str()
    kubernetes_conn_value = (
        "kubernetes://?extra__kubernetes__namespace=default" f"&extra__kubernetes__kube_config={encoded_kubeconfig}"
    )

    mocker.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_KUBERNETES_DEFAULT": f"{isolationctl.KUBERNETES_CONN_KEY}={kubernetes_conn_value}",
            "AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID": "kubernetes_default",
            "OPENLINEAGE_DISABLED": "true",
        },
    )

    actual_op: IsolatedKubernetesPodOperator = IsolatedOperator(
        task_id="test",
        operator=BashOperator,
        bash_command="echo hi",
        image="quay.io/astronomer/astro-runtime:8.8.0",
    )

    actual_op.dry_run()  # just prints
    pod = actual_op.build_pod_request_obj()
    pod_spec: V1PodSpec = pod.spec
    assert len(pod_spec.containers) == 1
    container: V1Container = pod_spec.containers[0]
    assert container.command == [
        "/bin/bash",
        "-euxc",
    ], "the container commands are as expected"
    assert container.args == [
        'python -c "from isolation.hooks.post_isolation import PostIsolationHook'
        '\n\nPostIsolationHook.run_isolated_task()\n"',
    ], "the container args are as expected"
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
            "name": "OPENLINEAGE_DISABLED",
            "value": "true",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
            "value": "airflow.operators.bash.BashOperator",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_ARGS",
            # B64 DECODED: "value": "{"args": [], "kwargs": {"bash_command": "echo hi", "default_args": {}}}",
            "value": "eyJhcmdzIjogW10sICJrd2FyZ3MiOiB7ImJhc2hfY29tbWFuZCI6ICJlY2hvIGhpIiwgImRlZmF1bHRfYXJncyI6IHt9fX0=",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_POST_ISOLATION",
            "value": "True",
            "value_from": None,
        },
    ]
    actual_env = container.env
    for i, env in enumerate(actual_env):
        env: V1EnvVar
        for key, value in env.to_dict().items():
            assert value == expected_env[i][key], "all the env keys are as expected"
    assert container.image == "quay.io/astronomer/astro-runtime:8.8.0", "Image was set via the operator"
    # ImagePullPolicy - not needed for a non-local image

    example_dag = project_root / "isolation" / "example_dags" / "isolation_provider_example_dag.py"
    pod.spec.volumes.extend(
        [
            V1Volume(
                host_path=V1HostPathVolumeSource(path=str(example_dag.parent)),
                name="dags",
            ),
            V1Volume(host_path=V1HostPathVolumeSource(path=str(dist_folder)), name="dist"),
        ]
    )
    container.volume_mounts.extend(
        [
            V1VolumeMount(mount_path="/usr/local/airflow/dags", name="dags"),
            V1VolumeMount(mount_path="/usr/local/airflow/dist", name="dist"),
        ]
    )
    # noinspection PyPep8Naming,SpellCheckingInspection
    PYTHONPATH = r'PYTHONPATH="$PYTHONPATH:/home/astro/.local"'
    container.args = [
        f"""bash -c \
        'pip install \
        "apache-airflow-providers-isolation[kubernetes] @ file:///usr/local/airflow/dist/{dist_file.name}" && \
        {PYTHONPATH} {container.args[0]}'
        """
    ]
    # This gets added in with the .execute() so we are mocking it
    # noinspection SpellCheckingInspection
    container.env.append(
        V1EnvVar(
            # B64 DECODED: Context({"ds": "foo", "params": {"foo": "bar"}})
            name="__ISOLATED_OPERATOR_AIRFLOW_CONTEXT",
            value="eyJkcyI6ICIxOTcwLTAxLTAxVDAwOjAwOjAwIiwgInBhcmFtcyI6IHsiZm9vIjogImJhciJ9fQ==",
        )
    )
    print("Modified:")
    pprint(pod)

    client = KubernetesHook(conn_id="kubernetes_default").core_v1_client
    client.create_namespaced_pod("default", pod)

    while pod_status := client.read_namespaced_pod_status(pod.metadata.name, pod.metadata.namespace):
        status: V1PodStatus = pod_status.status
        print(f"{status.message=} {status.phase=} {status.reason=}")
        if status.phase == "Succeeded" or status.phase == "Failed":
            break
        else:
            print("Not done yet - sleeping for 5s")
            time.sleep(5)

    log = client.read_namespaced_pod_log(pod.metadata.name, pod.metadata.namespace)
    assert "Successfully installed apache-airflow-providers-isolation" in log
    assert (
        """INFO - Running command: ['/bin/bash', '-c', 'echo hi']
INFO - Output:
INFO - hi
INFO - Command exited with return code 0"""
        in log
    )


# noinspection DuplicatedCode
@manual_tests
def test_python_isolated_operator_integration(project_root, build, dist_file, dist_folder, mocker: MockerFixture):
    from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
    from airflow.operators.python import PythonOperator

    # Requires kubectl and KUBECONFIG
    encoded_kubeconfig = isolationctl.extract_kubeconfig_to_str()
    kubernetes_conn_value = (
        "kubernetes://?extra__kubernetes__namespace=default" f"&extra__kubernetes__kube_config={encoded_kubeconfig}"
    )

    mocker.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_KUBERNETES_DEFAULT": f"{isolationctl.KUBERNETES_CONN_KEY}={kubernetes_conn_value}",
            "AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID": "kubernetes_default",
            "OPENLINEAGE_DISABLED": "true",
        },
    )
    actual_op: IsolatedKubernetesPodOperator = IsolatedOperator(
        task_id="test",
        operator=PythonOperator,
        python_callable=pprint,
        op_args=["hello world"],
        image="quay.io/astronomer/astro-runtime:8.8.0",
    )

    actual_op.dry_run()  # just prints
    pod = actual_op.build_pod_request_obj()
    pod_spec: V1PodSpec = pod.spec
    assert len(pod_spec.containers) == 1
    container: V1Container = pod_spec.containers[0]
    assert container.command == [
        "/bin/bash",
        "-euxc",
    ], "the container commands are as expected"
    assert container.args == [
        'python -c "from isolation.hooks.post_isolation import PostIsolationHook'
        '\n\nPostIsolationHook.run_isolated_task()\n"',
    ], "the container args are as expected"
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
            "name": "OPENLINEAGE_DISABLED",
            "value": "true",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
            "value": "airflow.operators.python.PythonOperator",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_ARGS",
            # B64 DECODED: {"args": [], "kwargs": {"op_args": ["hello world"], "default_args": {},
            # "_python_callable_qualname": "pprint.pprint"}}
            "value": "eyJhcmdzIjogW10sICJrd2FyZ3MiOiB7Im9wX2FyZ3MiOiBbImhlbGxvIHdvcmxkIl0sICJkZWZhdWx0X2F"
            "yZ3MiOiB7fSwgIl9weXRob25fY2FsbGFibGVfcXVhbG5hbWUiOiAicHByaW50LnBwcmludCJ9fQ==",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_POST_ISOLATION",
            "value": "True",
            "value_from": None,
        },
    ]
    actual_env = container.env
    for i, env in enumerate(actual_env):
        env: V1EnvVar
        for key, value in env.to_dict().items():
            assert value == expected_env[i][key], "all the env keys are as expected"
    assert container.image == "quay.io/astronomer/astro-runtime:8.8.0", "Image was set via the operator"
    # ImagePullPolicy - not needed for a non-local image

    example_dag = project_root / "isolation" / "example_dags" / "isolation_provider_example_dag.py"
    pod.spec.volumes.extend(
        [
            V1Volume(
                host_path=V1HostPathVolumeSource(path=str(example_dag.parent)),
                name="dags",
            ),
            V1Volume(host_path=V1HostPathVolumeSource(path=str(dist_folder)), name="dist"),
        ]
    )
    container.volume_mounts.extend(
        [
            V1VolumeMount(mount_path="/usr/local/airflow/dags", name="dags"),
            V1VolumeMount(mount_path="/usr/local/airflow/dist", name="dist"),
        ]
    )
    # noinspection PyPep8Naming,SpellCheckingInspection
    PYTHONPATH = r'PYTHONPATH="$PYTHONPATH:/home/astro/.local"'
    container.args = [
        f"""bash -c \
        'pip install \
        "apache-airflow-providers-isolation[kubernetes] @ file:///usr/local/airflow/dist/{dist_file.name}" && \
        {PYTHONPATH} {container.args[0]}'
        """
    ]
    # This gets added in with the .execute() so we are mocking it
    # noinspection SpellCheckingInspection
    container.env.append(
        V1EnvVar(
            # B64 DECODED: Context({"ds": "foo", "params": {"foo": "bar"}})
            name="__ISOLATED_OPERATOR_AIRFLOW_CONTEXT",
            value="eyJkcyI6ICIxOTcwLTAxLTAxVDAwOjAwOjAwIiwgInBhcmFtcyI6IHsiZm9vIjogImJhciJ9fQ==",
        )
    )
    print("Modified:")
    pprint(pod)

    client = KubernetesHook(conn_id="kubernetes_default").core_v1_client
    client.create_namespaced_pod("default", pod)

    while pod_status := client.read_namespaced_pod_status(pod.metadata.name, pod.metadata.namespace):
        status: V1PodStatus = pod_status.status
        print(f"{status.message=} {status.phase=} {status.reason=}")
        if status.phase == "Succeeded" or status.phase == "Failed":
            break
        else:
            print("Not done yet - sleeping for 5s")
            time.sleep(5)

    log = client.read_namespaced_pod_log(pod.metadata.name, pod.metadata.namespace)
    assert "Successfully installed apache-airflow-providers-isolation" in log
    assert (
        "INFO - Created\nWARNING - No module named 'paramiko'\nWARNING - No module named 'airflow.providers.dbt'"
        "\nINFO - Exporting env vars: \n'hello world'\nINFO - Done. Returned value was: None" in log
    )


# noinspection DuplicatedCode
@manual_tests
def test_python_example_dag_isolated_operator_integration(
    project_root, build, dist_file, dist_folder, mocker: MockerFixture, example_dag
):
    from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
    from airflow.operators.python import PythonOperator

    # Requires kubectl and KUBECONFIG
    encoded_kubeconfig = isolationctl.extract_kubeconfig_to_str()
    kubernetes_conn_value = (
        "kubernetes://?extra__kubernetes__namespace=default" f"&extra__kubernetes__kube_config={encoded_kubeconfig}"
    )

    mocker.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_KUBERNETES_DEFAULT": f"{isolationctl.KUBERNETES_CONN_KEY}={kubernetes_conn_value}",
            "AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID": "kubernetes_default",
            "OPENLINEAGE_DISABLED": "true",
        },
    )

    # Setting this to prevent the operators from complaining when we import the DAG
    os.environ["__ISOLATED_OPERATOR_POST_ISOLATION"] = "True"
    # noinspection PyUnresolvedReferences
    from dags.isolation_provider_example_dag import print_pandas_version

    del os.environ["__ISOLATED_OPERATOR_POST_ISOLATION"]

    actual_op: IsolatedKubernetesPodOperator = IsolatedOperator(
        task_id="test",
        operator=PythonOperator,
        python_callable=print_pandas_version,
        op_args=["hello world"],
        image="quay.io/astronomer/astro-runtime:8.8.0",
    )

    actual_op.dry_run()  # just prints
    pod = actual_op.build_pod_request_obj()
    pod_spec: V1PodSpec = pod.spec
    assert len(pod_spec.containers) == 1
    container: V1Container = pod_spec.containers[0]
    assert container.command == [
        "/bin/bash",
        "-euxc",
    ], "the container commands are as expected"
    assert container.args == [
        'python -c "from isolation.hooks.post_isolation import PostIsolationHook'
        '\n\nPostIsolationHook.run_isolated_task()\n"',
    ], "the container args are as expected"
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
            "name": "OPENLINEAGE_DISABLED",
            "value": "true",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
            "value": "airflow.operators.python.PythonOperator",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_OPERATOR_ARGS",
            # B64 DECODED: {"args": [], "kwargs": {"op_args": ["hello world"],
            # "default_args": {}, "_python_callable_qualname":
            # "dags.isolation_provider_example_dag.print_pandas_version"}}
            "value": "eyJhcmdzIjogW10sICJrd2FyZ3MiOiB7Im9wX2FyZ3MiOiBbImhlbGxvIHdvcmxkIl0sICJkZWZhdWx0X2FyZ3MiO"
            "iB7fSwgIl9weXRob25fY2FsbGFibGVfcXVhbG5hbWUiOiAiZGFncy5pc29sYXRpb25fcHJvdmlkZXJfZXhhbXBsZV9"
            "kYWcucHJpbnRfcGFuZGFzX3ZlcnNpb24ifX0=",
            "value_from": None,
        },
        {
            "name": "__ISOLATED_OPERATOR_POST_ISOLATION",
            "value": "True",
            "value_from": None,
        },
    ]
    actual_env = container.env
    for i, env in enumerate(actual_env):
        env: V1EnvVar
        for key, value in env.to_dict().items():
            assert value == expected_env[i][key], "all the env keys are as expected"
    assert container.image == "quay.io/astronomer/astro-runtime:8.8.0", "Image was set via the operator"
    # ImagePullPolicy - not needed for a non-local image

    example_dag = project_root / "isolation" / "example_dags" / "isolation_provider_example_dag.py"
    pod.spec.volumes.extend(
        [
            V1Volume(
                host_path=V1HostPathVolumeSource(path=str(example_dag.parent)),
                name="dags",
            ),
            V1Volume(host_path=V1HostPathVolumeSource(path=str(dist_folder)), name="dist"),
        ]
    )
    container.volume_mounts.extend(
        [
            V1VolumeMount(mount_path="/usr/local/airflow/dags", name="dags"),
            V1VolumeMount(mount_path="/usr/local/airflow/dist", name="dist"),
        ]
    )
    # noinspection PyPep8Naming,SpellCheckingInspection
    PYTHONPATH = r'PYTHONPATH="$PYTHONPATH:/home/astro/.local"'
    container.args = [
        f"""bash -c \
        'pip install \
        "apache-airflow-providers-isolation[kubernetes] @ file:///usr/local/airflow/dist/{dist_file.name}" && \
        {PYTHONPATH} {container.args[0]}'
        """
    ]
    # This gets added in with the .execute() so we are mocking it
    # noinspection SpellCheckingInspection
    container.env.append(
        V1EnvVar(
            # B64 DECODED: Context({"ds": "foo", "params": {"foo": "bar"}})
            name="__ISOLATED_OPERATOR_AIRFLOW_CONTEXT",
            value="eyJkcyI6ICIxOTcwLTAxLTAxVDAwOjAwOjAwIiwgInBhcmFtcyI6IHsiZm9vIjogImJhciJ9fQ==",
        )
    )
    print("Modified:")
    pprint(pod)

    client = KubernetesHook(conn_id="kubernetes_default").core_v1_client
    client.create_namespaced_pod("default", pod)

    while pod_status := client.read_namespaced_pod_status(pod.metadata.name, pod.metadata.namespace):
        status: V1PodStatus = pod_status.status
        print(f"{status.message=} {status.phase=} {status.reason=}")
        if status.phase == "Succeeded" or status.phase == "Failed":
            break
        else:
            print("Not done yet - sleeping for 5s")
            time.sleep(5)

    log = client.read_namespaced_pod_log(pod.metadata.name, pod.metadata.namespace)
    assert "Successfully installed apache-airflow-providers-isolation" in log
    assert (
        "INFO - Created\nWARNING - No module named 'paramiko'\nWARNING - No module named 'airflow.providers.dbt'\n"
        "INFO - Exporting env vars: \nPandas Version: 1.5.3 \nAnd printing other stuff for fun: \n"
        "arg='hello world', ds=DateTime(1970, 1, 1, 0, 0, 0, tzinfo=Timezone('UTC')), params={'foo': 'bar'}\n"
        "INFO - Done. Returned value was: None" in log
    )
