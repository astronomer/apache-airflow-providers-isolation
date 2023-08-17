import inspect
import logging
import re
from typing import Dict, Any, Type

from airflow.exceptions import AirflowConfigException

try:
    from airflow.models import BaseOperator
except AirflowConfigException:
    # Sometimes pytest gets weird here
    pass

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)


def create_isolated_operator_command(task_id, operator, *args, **kwargs):
    """Turn an Operator and it's args and/or kwargs into something that can be run directly in a Python shell
    >>> create_isolated_operator_command(task_id="foo", operator=BaseOperator, bar="foo")
    "from airflow.models.baseoperator import BaseOperator; BaseOperator(task_id='foo', bar='foo').execute({})"
    """
    _args = ", ".join(args)
    _kwargs = ", ".join(f"{k}={repr(v)}" for k, v in kwargs.items())
    if _args and _kwargs:
        _cmd = f"task_id='{task_id}', {_args}, {_kwargs}"
    elif _args or _kwargs:
        _cmd = f"task_id='{task_id}', {_args}{_kwargs}"
    else:
        _cmd = ""
    import_path = f"from {inspect.getmodule(operator).__name__} import {operator.__name__}; "
    operator = f"""{operator.__name__}({_cmd})"""
    execute = ".execute({})"
    return import_path + operator + execute


def set_isolated_logger_configs(kubernetes_pod_operator_kwargs: Dict[str, Any]):
    """Set some Airflow Configs related to logging for the isolated Airflow"""
    kubernetes_pod_operator_kwargs["env_vars"] = dict(
        **kubernetes_pod_operator_kwargs.get("env_vars", {}),
        AIRFLOW__LOGGING__COLORED_CONSOLE_LOG="false",
        AIRFLOW__LOGGING__LOG_FORMAT="%(levelname)s - %(message)s",
    )


def set_default_namespace_configs(kubernetes_pod_operator_kwargs: Dict[str, Any]):
    from airflow.configuration import conf

    if "namespace" not in kubernetes_pod_operator_kwargs:
        kubernetes_pod_operator_kwargs["namespace"] = conf.get("kubernetes", "NAMESPACE")


def remove_unsettable_kubernetes_pod_operator_kwargs(kubernetes_pod_operator_kwargs: Dict[str, Any]):
    for k in IsolatedOperator.isolated_operator_args:
        if k in kubernetes_pod_operator_kwargs:
            logging.warning(
                "The following cannot be set in 'kubernetes_pod_operator_kwargs' "
                "and must be set in IsolatedOperator (task_id, image) or left unset (cmds, arguments)"
            )
            del kubernetes_pod_operator_kwargs[k]


def render_airflow_templates_as_env_vars(operator, args, kwargs, kubernetes_pod_operator_kwargs):
    """call this during init? to get context passed in"""
    raise NotImplementedError()
    # operator.template_fields: Sequence[str] = ()
    # operator.template_ext: Sequence[str] = ()
    # operator.template_fields_renderers: dict[str, str] = {}
    # kubernetes_pod_operator_kwargs['env_vars'] = dict(
    #     **kubernetes_pod_operator_kwargs.get("env_vars", {}),
    #     foo="Bar"
    # )


def render_airflow_templates(operator, args, kwargs, context):
    """Call this during execute to get stuff like ds and ti in?"""
    return NotImplementedError()
    # Notes for Context
    # from airflow.models.abstractoperator import AbstractOperator
    # AbstractOperator.operator_class = operator
    # AbstractOperator.render_template_fields()
    # check abstractOperator - render_template_fields for inspiration


var_template_patterns = [
    re.compile(r"{{\s*var.value.([a-zA-Z-_]+)\s*}}"),  # "{{ var.value.<> }}"
    re.compile(r"{{\s*var.json.([a-zA-Z-_]+)\s*}}"),  # "{{ var.json.<> }}"
]
var_object_pattern = re.compile(r"""Variable.get[(]["']([a-zA-Z-_]+)["'][)]""")  # "Variable.get(<>)"
conn_property_pattern = re.compile(r"""(?=\w*_)conn_id=["']([a-zA-Z-_]+)["']""")  # "conn_id=<>"
conn_template_pattern = re.compile(r"[{]{2}\s*conn[.]([a-zA-Z-_]+)[.]?")  # "{{ conn.<> }}"


def set_simple_templates_as_env_vars(args, kwargs, kubernetes_pod_operator_kwargs):
    """

    # We give nothing, we get nothing
    >>> set_simple_templates_as_env_vars([], {}, {})
    {'env_vars': {}}

    # We have a var referred to via the args, we push that into the env
    >>> import os; os.environ['AIRFLOW_VAR_FOO'] = 'bar'; set_simple_templates_as_env_vars(
    ...     ["{{ var.value.foo }}"], {}, {}
    ... )
    {'env_vars': {'AIRFLOW_VAR_FOO': 'bar'}}

    # We have a var referred to via the kwargs, we push that into the env
    >>> set_simple_templates_as_env_vars([], {"param": "{{ var.value.foo }}"}, {})
    {'env_vars': {'AIRFLOW_VAR_FOO': 'bar'}}

    # we have a conn referred to via the args, we push that into the env
    >>> import os; os.environ['AIRFLOW_CONN_FOO'] = 'postgres://postgres:postgres@postgres:5432/db'
    >>> print('||'); set_simple_templates_as_env_vars(["{{ conn.foo }}"], {}, {})  # doctest: +ELLIPSIS
    ||...
    {'env_vars': {'AIRFLOW_CONN_FOO': 'postgres://postgres:postgres@postgres:5432/db'}}

    # we have a conn referred to via the kwargs, we push that into the env
    >>> set_simple_templates_as_env_vars([], {"param": "{{ conn.foo }}"}, {})
    {'env_vars': {'AIRFLOW_CONN_FOO': 'postgres://postgres:postgres@postgres:5432/db'}}

     # we have a conn_id referred to via the kwargs, we push that into the env
    >>> set_simple_templates_as_env_vars([], {"xyz_conn_id": "foo"}, {})
    {'env_vars': {'AIRFLOW_CONN_FOO': 'postgres://postgres:postgres@postgres:5432/db'}}
    """

    from airflow.models import Connection
    from airflow.models import Variable

    env_vars = kubernetes_pod_operator_kwargs.get("env_vars", {})

    def _set_conns_from_values(_value):
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

    def _set_conns_from_key_and_values(_key, _value):
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

    def _set_vars_from_values(_value):
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
            _set_vars_from_values(val)

            # look for {{ conn.xyz }}
            _set_conns_from_values(val)

    # Iterate over **kwargs
    for key, val in kwargs.items():
        logging.debug(f"Checking {key}={val} ...")
        if val:
            # look for {{ var.xyz }}
            _set_vars_from_values(val)

            # look for {{ conn.xyz }}
            _set_conns_from_values(val)

            if key:
                # look for *conn_id="..."
                _set_conns_from_key_and_values(key, val)

    kubernetes_pod_operator_kwargs["env_vars"] = env_vars
    return kubernetes_pod_operator_kwargs


def set_pod_name_configs(task_id, kubernetes_pod_operator_kwargs):
    if "name" not in kubernetes_pod_operator_kwargs:
        kubernetes_pod_operator_kwargs["name"] = task_id


class IsolatedOperator(KubernetesPodOperator):
    isolated_operator_args = [
        "task_id",
        "image",
        "cmds",
        "arguments",
        "log_events_on_failure",
    ]

    def __init__(
        self,
        image: str,
        operator: Type[BaseOperator],
        task_id: str,
        kubernetes_pod_operator_kwargs: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        self._args = args
        self._kwargs = kwargs
        self.kubernetes_pod_operator_kwargs = kubernetes_pod_operator_kwargs or {}
        self.operator = operator

        remove_unsettable_kubernetes_pod_operator_kwargs(self.kubernetes_pod_operator_kwargs)
        set_isolated_logger_configs(self.kubernetes_pod_operator_kwargs)
        set_default_namespace_configs(self.kubernetes_pod_operator_kwargs)
        set_pod_name_configs(task_id, self.kubernetes_pod_operator_kwargs)
        set_simple_templates_as_env_vars(self._args, self._kwargs, self.kubernetes_pod_operator_kwargs)

        super().__init__(
            task_id=task_id,
            image=image,
            cmds=["python"],
            arguments=[
                "-c",
                create_isolated_operator_command(task_id, self.operator, *self._args, **self._kwargs),
            ],
            log_events_on_failure=True,
            **self.kubernetes_pod_operator_kwargs,
        )
