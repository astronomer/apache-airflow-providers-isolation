import inspect
import logging
import os
import shlex
from textwrap import dedent
from typing import Callable, Dict, Any, Tuple, List, Type, Optional

from airflow.exceptions import AirflowConfigException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import Context

from isolation.operators import AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY
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
def _set_isolated_logger_configs(isolated_operator_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Set some Airflow Configs related to logging for the isolated Airflow
    >>> _set_isolated_logger_configs({}) # doctest: +ELLIPSIS
    {'env_vars': {'AIRFLOW__LOGGING__COLORED_CONSOLE_LOG': 'false', 'AIRFLOW__LOGGING__LOG_FORMAT': ...
    """
    env_vars = isolated_operator_kwargs.get("env_vars", {})
    if "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG" not in env_vars:
        env_vars["AIRFLOW__LOGGING__COLORED_CONSOLE_LOG"] = "false"
    if "AIRFLOW__LOGGING__LOG_FORMAT" not in env_vars:
        env_vars["AIRFLOW__LOGGING__LOG_FORMAT"] = "%(levelname)s - %(message)s"
    isolated_operator_kwargs["env_vars"] = env_vars
    return isolated_operator_kwargs


def _set_default_namespace_configs(isolated_operator_kwargs: Dict[str, Any]) -> None:
    from airflow.configuration import conf

    if "namespace" not in isolated_operator_kwargs:
        isolated_operator_kwargs["namespace"] = conf.get("kubernetes", "NAMESPACE")


def _remove_un_settable_isolated_operator_kwargs(isolated_operator_kwargs: Dict[str, Any]) -> None:
    for k in IsolatedKubernetesPodOperator.isolated_operator_args:
        if k in isolated_operator_kwargs:
            logging.warning(
                "The following cannot be set in 'isolated_operator_kwargs' "
                "and must be set in IsolatedOperator or left unset - "
                f"{IsolatedKubernetesPodOperator.isolated_operator_args} . Found: {k}"
            )
            del isolated_operator_kwargs[k]


def _set_simple_templates_via_env(
    args: Tuple[Any, ...], kwargs: Dict[str, Any], isolated_operator_kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    # noinspection PyTrailingSemicolon
    """Set {{var}} and {{conn}} templates, and *conn_id. Get the values and set them as env vars for the KPO"""
    from airflow.models import Connection
    from airflow.models import Variable

    env_vars = isolated_operator_kwargs.get("env_vars", {})

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

    isolated_operator_kwargs["env_vars"] = env_vars
    return isolated_operator_kwargs


def _set_pod_name_configs(task_id: str, isolated_operator_kwargs: Dict[str, Any]) -> None:
    """Set the KPO Pod Name to the task_id"""
    if "name" not in isolated_operator_kwargs:
        isolated_operator_kwargs["name"] = task_id


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


def _set_operator_via_env(operator: Type["BaseOperator"], isolated_operator_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Turn an Operator a direct qualified name reference, so it can get resurrected on the other side
    >>> from airflow.operators.bash import BashOperator; kw = {}; _set_operator_via_env(BashOperator, kw)
    {'env_vars': {'__ISOLATED_OPERATOR_OPERATOR_QUALNAME': 'airflow.operators.bash.BashOperator'}}
    """
    env_vars = isolated_operator_kwargs.get("env_vars", {})
    key = IsolatedOperator.settable_environment_variables[_set_operator_via_env.__name__]
    env_vars[key] = export_to_qualname(operator)
    isolated_operator_kwargs["env_vars"] = env_vars
    return isolated_operator_kwargs


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


def _set_operator_args_via_env(args: Tuple[Any, ...], kwargs: Dict[str, Any], isolated_operator_kwargs: Dict[str, Any]):
    """Set args and kwargs that were given for the operator to __ISOLATED_OPERATOR_OPERATOR_ARGS
     1) b64 and json encode them
     2) if it is xyz_callable, we rename it to _xyz_callable_qualname and input the qualified name of the fn
    >>> _set_operator_args_via_env((), {}, {})
    {'env_vars': {'__ISOLATED_OPERATOR_OPERATOR_ARGS': 'eyJhcmdzIjogW10sICJrd2FyZ3MiOiB7fX0='}}
    >>> _set_operator_args_via_env(('foo',), {"bar_callable": print}, {"existing_kwargs": []}) # doctest: +ELLIPSIS
    {'existing_kwargs': [], 'env_vars': {'__ISOLATED_OPERATOR_OPERATOR_ARGS': ...
    """
    key = IsolatedOperator.settable_environment_variables[_set_operator_args_via_env.__name__]
    env_vars = isolated_operator_kwargs.get("env_vars", {})
    env_vars[key] = b64encode_json(_convert_args(args, kwargs))
    isolated_operator_kwargs["env_vars"] = env_vars
    return isolated_operator_kwargs


def _set_deferrable(
    kwargs: Dict[str, Any], isolated_operator_kwargs: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """If the 'deferrable' arg is set, ensure if gets set on the KPO only
    >>> _set_deferrable({"deferrable": False}, {})
    ({}, {'deferrable': False})
    """
    if "deferrable" in kwargs:
        value = kwargs["deferrable"]
        del kwargs["deferrable"]
        isolated_operator_kwargs["deferrable"] = value
    return kwargs, isolated_operator_kwargs


# noinspection PyTrailingSemicolon
def _set_kpo_default_args_from_env(isolated_operator_kwargs: Dict[str, Any]) -> Dict[str, Any]:
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
            if key not in IsolatedKubernetesPodOperator.isolated_operator_args and key not in isolated_operator_kwargs:
                isolated_operator_kwargs[key] = value
    return isolated_operator_kwargs


def _derive_image(image: Optional[str], default_image_from_env: Optional[str], environment: Optional[str]) -> str:
    """Take the different sources of "image" and "environment" and derive a final image
    1) return image/env, if given
    2) return image, if env not given
    3) return default_image/env, if given
    4) error otherwise

    >>> _derive_image(None,None,None)  # Give nothing, get an error
    Traceback (most recent call last):
        ...
    RuntimeError: Image must be set via the 'image' argument or the 'AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE' env var
    >>> _derive_image("foo",None,None)  # Give an image, get the image
    'foo'
    >>> _derive_image("foo",None,"bar")
    'foo/bar'
    >>> _derive_image("foo","baz","bar")
    'foo/bar'
    >>> _derive_image(None,"baz","bar")
    'baz/bar'
    >>> _derive_image(None,None,"bar")
    Traceback (most recent call last):
        ...
    RuntimeError: Image must be set via the 'image' argument or the 'AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE' env var
    >>> _derive_image(None,"baz",None)
    Traceback (most recent call last):
        ...
    RuntimeError: The 'AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE' env var must be paired with the 'environment' argument
    """
    # Prefer image to the env var default
    if image:
        # if that's all we have - return it
        if not environment:
            return image

        # If we have image/environment - return it like that
        else:
            return f"{image}/{environment}"
    elif default_image_from_env:
        # if that's all we have - error
        if not environment:
            raise RuntimeError(
                f"The '{AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY}' env var "
                f"must be paired with the 'environment' argument"
            )

        # If we have image/environment - return it like that
        else:
            return f"{default_image_from_env}/{environment}"

    # Both image sources are empty - error
    else:
        raise RuntimeError(
            f"Image must be set via the 'image' argument or the '{AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY}' env var"
        )


# noinspection GrazieInspection,SpellCheckingInspection
def _set_post_isolation_flag_via_env(isolated_operator_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Set the __ISOLATED_OPERATOR_POST_ISOLATION flag via the env, to prevent attempting to (re)create
    the IsolatedOperator on the other side, for example if you are loading a function from the DAG itself"""
    env_vars = isolated_operator_kwargs.get("env_vars", {})
    env_vars["__ISOLATED_OPERATOR_POST_ISOLATION"] = "True"
    isolated_operator_kwargs["env_vars"] = env_vars
    return isolated_operator_kwargs


class IsolatedKubernetesPodOperator(KubernetesPodOperator):
    """IsolatedKubernetesPodOperator - run an IsolatedOperator powered by the KubernetesPodOperator
    :param task_id str: Task id, as normal
    :param operator Type[BaseOperator]: Operator to run, e.g. operator=BashOperator.
        :class:`airflow.models.base_operator.BaseOperator`
    :param image Optional[str]: full name of image to run, without a tag, e.g. docker.io/organization/project/airflow
        default to env var AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE
    :var 'AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE' is retrieved from the env if set, as a default
    :param environment Optional[str]: the name of the environment which gets appended to the image.
        Just the image is used, if not given
    :param isolated_operator_kwargs Optional[Dict[str, Any]]: kwargs passed directly to
        the :class:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
    :param args List[str]: args for the operator
    :param kwargs Dict[str, Any]: kwargs for the operator, e.g. bash_command="echo hi"

    :Example:

    ```
    def print_pandas_version(arg, ds, params):
        from pandas import __version__ as pandas_version

        print(f"Pandas Version: {pandas_version} \n" "And printing other stuff for fun: \n" f"{arg=}, {ds=}, {params=}")

    IsolatedOperator(
        task_id="isolated_environment_pandas",
        operator=PythonOperator,
        image="localhost:5000/my-airflow-project_36d6b4/airflow",
        environment="data-team",
        python_callable=print_pandas_version,
        op_args=["Hello!"]
    )
    ```

    .. seealso:: :class:`isolation.operators.isolation.IsolatedOperator`
    """

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
        environment: Optional[str] = None,
        isolated_operator_kwargs: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        if "__ISOLATED_OPERATOR_POST_ISOLATION" in os.environ and os.getenv("__ISOLATED_OPERATOR_POST_ISOLATION"):
            super().__init__(task_id=task_id)
        else:
            self._args = args
            self._kwargs = kwargs
            self.isolated_operator_kwargs = isolated_operator_kwargs or {}
            self.operator = operator
            self.environment = environment
            self.image = image

            _remove_un_settable_isolated_operator_kwargs(self.isolated_operator_kwargs)
            _set_isolated_logger_configs(self.isolated_operator_kwargs)
            _set_default_namespace_configs(self.isolated_operator_kwargs)
            _set_pod_name_configs(task_id, self.isolated_operator_kwargs)
            _set_deferrable(self._kwargs, self.isolated_operator_kwargs)
            _set_simple_templates_via_env(self._args, self._kwargs, self.isolated_operator_kwargs)
            _set_operator_via_env(self.operator, self.isolated_operator_kwargs)
            _set_operator_args_via_env(self._args, self._kwargs, self.isolated_operator_kwargs)
            _set_kpo_default_args_from_env(self.isolated_operator_kwargs)
            default_image_from_env = os.getenv(AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_KEY)
            image = _derive_image(self.image, default_image_from_env, self.environment)
            _set_post_isolation_flag_via_env(self.isolated_operator_kwargs)

            # noinspection SpellCheckingInspection
            super().__init__(
                task_id=task_id,
                image=image,
                cmds=shlex.split("/bin/bash -euxc"),
                arguments=[f'python -c "{fn_to_source_code(run_in_pod)}"'],
                log_events_on_failure=True,
                **self.isolated_operator_kwargs,
            )

    def execute(self, context: Context):
        _set_airflow_context_via_env(context, self.env_vars)
        super().execute(context)
