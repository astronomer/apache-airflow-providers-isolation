import inspect
import logging
import os
import shlex
from textwrap import dedent
from typing import Callable, Dict, Any, Tuple, List, Type, Optional

from airflow.exceptions import AirflowConfigException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import Context

from isolation.operators.isolation import IsolatedOperator
from isolation.util import (
    conn_template_pattern,
    conn_property_pattern,
    var_template_patterns,
    b64encode_json,
    export_to_qualname,
)

try:
    from airflow.models import BaseOperator
except AirflowConfigException:
    # Sometimes pytest gets weird here, and we are just using it for typing
    pass


# Micro-fn to inject this python into the container
def run_in_pod():
    from isolation.hooks.post_isolation import PostIsolationHook

    PostIsolationHook.run_isolated_task()


def fn_to_source_code(fn: Callable) -> str:
    return "".join(dedent(line) for line in inspect.getsourcelines(fn)[0][1:])


# noinspection GrazieInspection,SpellCheckingInspection
def _set_isolated_logger_configs(kubernetes_pod_operator_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Set some Airflow Configs related to logging for the isolated Airflow
    >>> _set_isolated_logger_configs({}) # doctest: +ELLIPSIS
    {'env_vars': {'AIRFLOW__LOGGING__COLORED_CONSOLE_LOG': 'false', 'AIRFLOW__LOGGING__LOG_FORMAT': ...
    """
    env_vars = kubernetes_pod_operator_kwargs.get("env_vars", {})
    if "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG" not in env_vars:
        env_vars["AIRFLOW__LOGGING__COLORED_CONSOLE_LOG"] = "false"
    if "AIRFLOW__LOGGING__LOG_FORMAT" not in env_vars:
        env_vars["AIRFLOW__LOGGING__LOG_FORMAT"] = "%(levelname)s - %(message)s"
    kubernetes_pod_operator_kwargs["env_vars"] = env_vars
    return kubernetes_pod_operator_kwargs


def _set_default_namespace_configs(kubernetes_pod_operator_kwargs: Dict[str, Any]) -> None:
    from airflow.configuration import conf

    if "namespace" not in kubernetes_pod_operator_kwargs:
        kubernetes_pod_operator_kwargs["namespace"] = conf.get("kubernetes", "NAMESPACE")


def _remove_un_settable_kubernetes_pod_operator_kwargs(kubernetes_pod_operator_kwargs: Dict[str, Any]) -> None:
    for k in IsolatedKubernetesPodOperator.isolated_operator_args:
        if k in kubernetes_pod_operator_kwargs:
            logging.warning(
                "The following cannot be set in 'kubernetes_pod_operator_kwargs' "
                "and must be set in IsolatedOperator or left unset - "
                f"{IsolatedKubernetesPodOperator.isolated_operator_args} . Found: {k}"
            )
            del kubernetes_pod_operator_kwargs[k]


def _set_simple_templates_via_env(
    args: Tuple[Any, ...], kwargs: Dict[str, Any], kubernetes_pod_operator_kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    # noinspection PyTrailingSemicolon
    """Set {{var}} and {{conn}} templates, and *conn_id. Get the values and set them as env vars for the KPO
    # We give nothing, we get nothing
    >>> _set_simple_templates_via_env((),{},{})
    {'env_vars': {}}

    # We have a var referred to via the args, we push that into the env
    >>> import os; os.environ['AIRFLOW_VAR_FOO'] = 'bar'; _set_simple_templates_via_env(("{{ var.value.foo }}",),{},{})
    {'env_vars': {'AIRFLOW_VAR_FOO': 'bar'}}

    # We have a var referred to via the kwargs, we push that into the env
    >>> _set_simple_templates_via_env((),{"param": "{{ var.value.foo }}"},{})
    {'env_vars': {'AIRFLOW_VAR_FOO': 'bar'}}

    # we have a connection referred to via the args, we push that into the env
    >>> import os; os.environ['AIRFLOW_CONN_FOO'] = 'postgres://postgres:postgres@postgres:5432/db'
    >>> print('||'); _set_simple_templates_via_env(("{{ conn.foo }}",),{},{})  # doctest: +ELLIPSIS
    ||...
    {'env_vars': {'AIRFLOW_CONN_FOO': 'postgres://postgres:postgres@postgres:5432/db'}}

    # we have a connection referred to via the kwargs, we push that into the env
    >>> _set_simple_templates_via_env((),{"param": "{{ conn.foo }}"},{})
    {'env_vars': {'AIRFLOW_CONN_FOO': 'postgres://postgres:postgres@postgres:5432/db'}}

     # we have a conn_id referred to via the kwargs, we push that into the env
    >>> _set_simple_templates_via_env((),{"xyz_conn_id": "foo"},{})
    {'env_vars': {'AIRFLOW_CONN_FOO': 'postgres://postgres:postgres@postgres:5432/db'}}
    """
    from airflow.models import Connection
    from airflow.models import Variable

    env_vars = kubernetes_pod_operator_kwargs.get("env_vars", {})

    def set_conns_from_values(_value):
        if not isinstance(_value, str):
            return
        results = conn_template_pattern.findall(_value)
        if results:
            for conn_id in results:
                # there will, likely, only be one here
                logging.debug(f"Passing connection {conn_id} through to " f"IsolatedOperator via ENV VAR")
                env_vars[f"AIRFLOW_CONN_{conn_id.upper()}"] = Connection.get_connection_from_secrets(
                    conn_id=conn_id
                ).get_uri()

    def set_conns_from_key_and_values(_key, _value):
        if not isinstance(_value, str) or not isinstance(_key, str):
            return
        results = conn_property_pattern.findall(f"{_key}='{_value}'")
        if results:
            for conn_id in results:
                # there will, likely, only be one here
                logging.debug(f"Passing connection {conn_id} through to " f"IsolatedOperator via ENV VAR")
                env_vars[f"AIRFLOW_CONN_{conn_id.upper()}"] = Connection.get_connection_from_secrets(
                    conn_id=conn_id
                ).get_uri()

    def set_vars_from_values(_value):
        if not isinstance(_value, str):
            return
        for results in [pattern.findall(_value) for pattern in var_template_patterns]:
            if results:
                for var_id in results:
                    logging.debug(f"Passing variable {var_id} through to " f"IsolatedOperator via ENV VAR")
                    env_vars[f"AIRFLOW_VAR_{var_id.upper()}"] = Variable.get(var_id)

    # Iterate over *args
    for i, val in enumerate(args):
        logging.debug(f"Checking args[{i}]={val} for vars and conns...")
        if val:
            # look for {{ var.xyz }}
            set_vars_from_values(val)

            # look for {{ conn.xyz }}
            set_conns_from_values(val)

    # Iterate over **kwargs
    for key, val in kwargs.items():
        logging.debug(f"Checking {key}={val} ...")
        if val:
            # look for {{ var.xyz }}
            set_vars_from_values(val)

            # look for {{ conn.xyz }}
            set_conns_from_values(val)

            if key:
                # look for *conn_id="..."
                set_conns_from_key_and_values(key, val)

    kubernetes_pod_operator_kwargs["env_vars"] = env_vars
    return kubernetes_pod_operator_kwargs


def _set_pod_name_configs(task_id: str, kubernetes_pod_operator_kwargs: Dict[str, Any]) -> None:
    """Set the KPO Pod Name to the task_id"""
    if "name" not in kubernetes_pod_operator_kwargs:
        kubernetes_pod_operator_kwargs["name"] = task_id


def _set_airflow_context_via_env(context: Context, env_vars: List["V1EnvVar"]) -> List["V1EnvVar"]:  # noqa: F821
    """Serialize the Airflow Context
    >>> _set_airflow_context_via_env(Context({"ds": "foo", "params": {"foo": "bar"}}), [])
    [{'name': '__ISOLATED_OPERATOR_AIRFLOW_CONTEXT',
     'value': 'InsnZHMnOiAnZm9vJywgJ3BhcmFtcyc6IHsnZm9vJzogJ2Jhcid9fSI=',
     'value_from': None}]
    """
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars

    key = IsolatedOperator.settable_environment_variables[_set_airflow_context_via_env.__name__]
    env_vars.extend(convert_env_vars({key: b64encode_json(context)}))
    return env_vars


def _set_operator_via_env(
    operator: Type["BaseOperator"], kubernetes_pod_operator_kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    """Turn an Operator a direct qualified name reference, so it can get resurrected on the other side
    >>> from airflow.operators.bash import BashOperator; kw = {}; _set_operator_via_env(BashOperator, kw)
    {'env_vars': {'__ISOLATED_OPERATOR_OPERATOR_QUALNAME': 'airflow.operators.bash.BashOperator'}}
    """
    env_vars = kubernetes_pod_operator_kwargs.get("env_vars", {})
    key = IsolatedOperator.settable_environment_variables[_set_operator_via_env.__name__]
    env_vars[key] = export_to_qualname(operator)
    kubernetes_pod_operator_kwargs["env_vars"] = env_vars
    return kubernetes_pod_operator_kwargs


def _set_callable_qualname(d: Dict[str, Any]) -> Dict[str, Any]:
    """Turn xyz_callable=fn into _xyz_callable_qualname=qualname. It gets turned back on the other side
    >>> _set_callable_qualname({}) # empty
    {}
    >>> _set_callable_qualname({"python_callable": _set_callable_qualname})  # happy path
    {'_python_callable_qualname': 'isolation.operators.isolation_kubernetes._set_callable_qualname'}
    >>> _set_callable_qualname({"python_callable": "foo"})  # no change
    {'python_callable': 'foo'}
    """
    keys = list(d.keys())
    for key in keys:
        value = d[key]
        if isinstance(value, Callable):
            new_key = f"_{key}_qualname"
            new_value = export_to_qualname(value, validate=False)
            d[new_key] = new_value
            del d[key]
    return d


def _convert_args(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Pack the args and kwargs as json, convert any Callables to qualified names
    >>> _convert_args((), {})
    {'args': (), 'kwargs': {}}
    """
    kwargs = _set_callable_qualname(kwargs)
    return {"args": args, "kwargs": kwargs}


def _set_operator_args_via_env(
    args: Tuple[Any, ...], kwargs: Dict[str, Any], kubernetes_pod_operator_kwargs: Dict[str, Any]
):
    """Set args and kwargs that were given for the operator to __ISOLATED_OPERATOR_OPERATOR_ARGS
     1) b64 and json encode them
     2) if it is xyz_callable, we rename it to _xyz_callable_qualname and input the qualified name of the fn
    >>> _set_operator_args_via_env((), {}, {})
    {'env_vars': {'__ISOLATED_OPERATOR_OPERATOR_ARGS': 'eyJhcmdzIjogW10sICJrd2FyZ3MiOiB7fX0='}}
    >>> _set_operator_args_via_env(('foo',), {"bar_callable": print}, {"existing_kwargs": []}) # doctest: +ELLIPSIS
    {'existing_kwargs': [], 'env_vars': {'__ISOLATED_OPERATOR_OPERATOR_ARGS': ...
    """
    key = IsolatedOperator.settable_environment_variables[_set_operator_args_via_env.__name__]
    env_vars = kubernetes_pod_operator_kwargs.get("env_vars", {})
    env_vars[key] = b64encode_json(_convert_args(args, kwargs))
    kubernetes_pod_operator_kwargs["env_vars"] = env_vars
    return kubernetes_pod_operator_kwargs


def _set_deferrable(
    kwargs: Dict[str, Any], kubernetes_pod_operator_kwargs: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """If the 'deferrable' arg is set, ensure if gets set on the KPO only
    >>> _set_deferrable({"deferrable": False}, {})
    ({}, {'deferrable': False})
    """
    if "deferrable" in kwargs:
        value = kwargs["deferrable"]
        del kwargs["deferrable"]
        kubernetes_pod_operator_kwargs["deferrable"] = value
    return kwargs, kubernetes_pod_operator_kwargs


# noinspection PyTrailingSemicolon
def _set_kpo_default_args_from_env(kubernetes_pod_operator_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Scan env for any key like AIRFLOW__ISOLATED_POD_OPERATOR__XYZ and default it
    >>> os.environ["AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID"]="FOO"; _set_kpo_default_args_from_env(
    ...     {}
    ... ); del os.environ["AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID"]
    {'kubernetes_conn_id': 'FOO'}
    >>> os.environ["AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE"]="FOO"; _set_kpo_default_args_from_env({})
    {}
    """
    prefix = "AIRFLOW__ISOLATED_POD_OPERATOR__"
    for key, value in os.environ.items():
        if prefix in key:
            key = key.replace(prefix, "").lower()
            # If it's not in the args list, and it isn't already set
            if (
                key not in IsolatedKubernetesPodOperator.isolated_operator_args
                and key not in kubernetes_pod_operator_kwargs
            ):
                kubernetes_pod_operator_kwargs[key] = value
    return kubernetes_pod_operator_kwargs


class IsolatedKubernetesPodOperator(KubernetesPodOperator):
    isolated_operator_args = [
        "task_id",
        "image",
        "cmds",
        "arguments",
        "log_events_on_failure",
    ]

    def __init__(
        self,
        task_id: str,
        operator: Type["BaseOperator"],
        image: Optional[str] = None,
        kubernetes_pod_operator_kwargs: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        """IsolatedKubernetesPodOperator - run an IsolatedOperator powered by the KubernetesPodOperator
        :param task_id: Task id, as normal
        :param operator: Operator to run, e.g. operator=BashOperator
        :param image: image - default to env var AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE
        :param kubernetes_pod_operator_kwargs: kwargs passed directly to the KPO
        :param args: args for the operator
        :param kwargs: kwargs for the operator, e.g. bash_command="echo hi"
        """
        self._args = args
        self._kwargs = kwargs
        self.kubernetes_pod_operator_kwargs = kubernetes_pod_operator_kwargs or {}
        self.operator = operator

        _remove_un_settable_kubernetes_pod_operator_kwargs(self.kubernetes_pod_operator_kwargs)
        _set_isolated_logger_configs(self.kubernetes_pod_operator_kwargs)
        _set_default_namespace_configs(self.kubernetes_pod_operator_kwargs)
        _set_pod_name_configs(task_id, self.kubernetes_pod_operator_kwargs)
        _set_deferrable(self._kwargs, self.kubernetes_pod_operator_kwargs)
        _set_simple_templates_via_env(self._args, self._kwargs, self.kubernetes_pod_operator_kwargs)
        _set_operator_via_env(self.operator, self.kubernetes_pod_operator_kwargs)
        _set_operator_args_via_env(self._args, self._kwargs, self.kubernetes_pod_operator_kwargs)
        _set_kpo_default_args_from_env(self.kubernetes_pod_operator_kwargs)
        if not image and "AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE" in os.environ:
            image = os.getenv("AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE")

        # noinspection SpellCheckingInspection
        super().__init__(
            task_id=task_id,
            image=image,
            cmds=shlex.split("/bin/bash -euxc"),
            arguments=shlex.split(f"""python -c "{fn_to_source_code(run_in_pod)}" """),
            log_events_on_failure=True,
            **self.kubernetes_pod_operator_kwargs,
        )

    def execute(self, context: Context):
        _set_airflow_context_via_env(context, self.env_vars)
        super().execute(context)
