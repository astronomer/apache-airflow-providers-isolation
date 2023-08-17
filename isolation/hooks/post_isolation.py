import base64
import binascii
import itertools
import json
import os
from copy import copy
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Type, Tuple, List, Optional
import re

XCOM_FILE = "/airflow/xcom/return.json"


def _b64encode_json(d: Dict[str, Any]) -> str:
    """helper utility to encode a dict to a b64 string
    >>> _b64encode_json({"foo": "bar"})
    'eyJmb28iOiAiYmFyIn0='
    """
    return base64.b64encode(json.dumps(d, default=str).encode()).decode()


def _b64decode_json(s: str) -> Dict[str, Any]:
    """helper utility to decode a b64 to a json dict
    >>> _b64decode_json('eyJmb28iOiAiYmFyIn0=')
    {'foo': 'bar'}
    >>> _b64decode_json("garbage")   # Garbage - not b64
    Traceback (most recent call last):
        ...
    RuntimeError: Input is not encoded correctly as base64! Got: garbage . Cannot proceed!
    >>> _b64decode_json("{{}") # Garbage - not json
    Traceback (most recent call last):
        ...
    RuntimeError: Input is not encoded correctly as json! Got: b'' . Cannot proceed!
    """
    try:
        decoded_j = base64.b64decode(s)
        return json.loads(decoded_j)
    except binascii.Error:
        raise RuntimeError(
            f"Input is not encoded correctly as base64! " f"Got: {s} . Cannot proceed!",
        )
    except json.decoder.JSONDecodeError:
        # noinspection PyUnboundLocalVariable
        raise RuntimeError(
            f"Input is not encoded correctly as json! " f"Got: {decoded_j} from '{s}'. Cannot proceed!",
        )


def _getenv_or_raise(key):
    if key not in os.environ:
        raise RuntimeError(f"{key} must be set!")
    return os.getenv(key)


def _verify_kwargs(kwargs: Dict[str, Any]):
    """Override or set required kwargs
    1) Set/override the task_id
    2) Set/override do_xcom_push to False

    >>> d = {}; _verify_kwargs(d); d
    {'task_id': '_ISOLATED', 'do_xcom_push': False}
    >>> d = {"foo": "bar"}; _verify_kwargs(d); d
    {'foo': 'bar', 'task_id': '_ISOLATED', 'do_xcom_push': False}
    """
    kwargs["task_id"] = "_ISOLATED"
    kwargs["do_xcom_push"] = False


def _get_operator_args_from_env() -> Tuple[List[Any], Dict[str, Any]]:
    """Return *args and **kwargs for the operator,
    encoded as {"args":args,"kwargs":kwargs} as a b64 encoded json

    >>> import datetime, base64, json; _get_operator_args_from_env()   # Unset
    Traceback (most recent call last):
        ...
    RuntimeError: __ISOLATED_OPERATOR_OPERATOR_ARGS must be set!
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_ARGS'] = ""; _get_operator_args_from_env()   # Empty
    ([], {})
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_ARGS'] = _b64encode_json(
    ...     {"args":["foo", 1], "kwargs":{"a": 1, "b": 2, "c": datetime.datetime(1970, 1, 1)}},
    ... ); _get_operator_args_from_env()   # Happy Path
    (['foo', 1], {'a': 1, 'b': 2, 'c': '1970-01-01 00:00:00'})

    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_ARGS'] = _b64encode_json(
    ...     {"args": [], "kwargs": {}}
    ... ); _get_operator_args_from_env()   # Happy & Empty
    ([], {})
    """
    key = PostIsolationHook.required_environment_variables[_get_operator_args_from_env.__name__]
    encoded_args_json = _getenv_or_raise(key)
    if not encoded_args_json:
        return [], {}
    args_json = _b64decode_json(encoded_args_json)
    args, kwargs = args_json["args"], args_json["kwargs"]
    if args is None:
        args = []
    if not isinstance(args, list):
        raise RuntimeError(f"args is not a list! Got: {args} . Cannot proceed")

    if kwargs is None:
        kwargs = {}
    if not isinstance(kwargs, dict):
        raise RuntimeError(f"kwargs is not a list! Got: {kwargs} . Cannot proceed")

    return args, kwargs


# noinspection PyUnresolvedReferences
def _get_context_from_env() -> "Context":  # noqa: F821
    """Retrieve a json-serialized version of a subset
    of the Airflow Context object and recreate

    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = _b64encode_json(
    ...     {}
    ... ); r = _get_context_from_env(); r['params'], type(r['ds']) # Empty
    ({}, <class 'pendulum.datetime.DateTime'>)
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = ''; r = _get_context_from_env(
    ... ); r['params'], type(r['ds']) # Empty
    ({}, <class 'pendulum.datetime.DateTime'>)
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = _b64encode_json(
    ...     {"ds": "foo"}
    ... ); r = _get_context_from_env() # bad de-ser data
    Traceback (most recent call last):
        ...
    pendulum.parsing.exceptions.ParserError: Unable to parse string [foo]
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = _b64encode_json(
    ...     {"params": {"foo": "bar"}}
    ... ); r = _get_context_from_env(); r['params'], type(r['params'])  # good params data
    ({'foo': 'bar'}, <class 'airflow.models.param.ParamsDict'>)
    """
    from airflow.utils.context import Context
    import pendulum
    from airflow.models.param import ParamsDict

    key = PostIsolationHook.required_environment_variables[_get_context_from_env.__name__]
    encoded_context_json = _getenv_or_raise(key)
    context_json = _b64decode_json(encoded_context_json) if encoded_context_json != "" else {}

    required_keys = ["ds", "params"]
    serialization_mappings = {
        # Key:  (default-fn,  mapping-fn)
        "params": (dict, ParamsDict),
        "ds": (pendulum.now, pendulum.parse),
    }
    # we check over both what we got AND the required ones
    for k in set(itertools.chain(context_json.keys(), required_keys)):
        if k in context_json:  # if it's one of the ones we got
            if k in serialization_mappings:  # and if we have a mapping
                context_json[k] = serialization_mappings[k][1](context_json[k])  # map it
            else:  # else we got it, but
                pass  # it's fine as-is
        else:  # if it's required, wasn't given, and needs to be defaulted
            context_json[k] = serialization_mappings[k][0]()  # default it
    return Context(**context_json)


def _import_from_qualname(qualname):
    """Turn a.b.c.d.MyOperator into the actual python version"""
    # split out - a.b.c.d, MyOperator
    [module, name] = qualname.rsplit(".", 1)
    # import a.b.c.d
    imported_module = import_module(module)
    # return a.b.c.d.MyOperator
    return getattr(imported_module, name)


def _get_operator_from_env() -> Type["BaseOperator"]:  # noqa: F821
    """Instantiate the operator via the qualified name saved in __ISOLATED_OPERATOR_OPERATOR_QUALNAME
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_QUALNAME']="datetime.datetime"; _get_operator_from_env()
    <class 'datetime.datetime'>
    """
    # Sanity check - __ISOLATED_OPERATOR_OPERATOR_QUALNAME=a.b.c.d.MyOperator
    key = PostIsolationHook.required_environment_variables[_get_operator_from_env.__name__]
    operator_qualname = _getenv_or_raise(key)
    # split out - a.b.c.d, MyOperator
    return _import_from_qualname(operator_qualname)


def _validate_operator_is_operator(operator) -> None:
    """make sure the "operator" we were given is an Operator, e.g BashOperator
    >>> from airflow.operators.bash import BashOperator; _validate_operator_is_operator(BashOperator)  # happy path
    >>> class MyOperator(BashOperator):
    ...    pass
    >>> _validate_operator_is_operator(MyOperator)  # happy path with a custom operator
    >>> _validate_operator_is_operator(dict) # non-operators don't parse
    Traceback (most recent call last):
    ...
    RuntimeError: <class 'dict'> must be a subclass of <class 'airflow.models.baseoperator.BaseOperator'>
    """
    from airflow.models import BaseOperator

    if not issubclass(operator, BaseOperator):
        raise RuntimeError(f"{operator} must be a subclass of {BaseOperator}")


def _patch_task_dependencies(task) -> iter:
    """We overwrite the method to avoid the DB call"""

    def blackhole():
        return iter([])

    task.iter_mapped_dependants = blackhole


def _patch_post_execute_for_xcom(task):
    """Patch the 'task.post_execute' method - write a xcom then run the original"""
    original_fn = task.post_execute

    def new_fn(context, result):
        if result:
            xcom_path = Path(XCOM_FILE)
            xcom_path.parent.mkdir(parents=True, exist_ok=True)
            with xcom_path.open("w") as f:
                f.write(result)
        return original_fn(context, result)

    task.post_execute = new_fn


def _patch_clear_xcom_data(task_instance):
    """Patch the 'task_instance.clear_xcom_data' method - just respond immediately, skip DB"""

    def blackhole():
        return

    task_instance.clear_xcom_data = blackhole


def _maybe_qualname(key: str) -> Optional[str]:
    """
    >>> _maybe_qualname("_xyz_qualname")
    'xyz'
    >>> _maybe_qualname("_python_callable_qualname")
    'python_callable'
    >>> _maybe_qualname("definitely_not_python_callable")
    >>> _maybe_qualname("bash_command")
    """
    maybe_match = re.match(pattern="[_](.*)(?=_qualname)", string=key)
    return maybe_match.group(1) if maybe_match is not None else None


def _transform_args(args: List[Any], kwargs: Dict[str, Any]):
    """
    Look for anything like '_xyz_qualname' or '_python_callable_qualname' in the kwargs, remap it to a fn
    >>> _transform_args([], {
    ...     "_python_callable_qualname": "tests.resources.fn.direct.print_magic_string_test_direct"
    ... })  # doctest: +ELLIPSIS
    ([], {'python_callable': <function print_magic_string_test_direct at ...
    """
    dict_keys = copy(list(kwargs.keys()))
    for key in dict_keys:
        maybe_qualname_key = _maybe_qualname(key)
        if maybe_qualname_key:
            # save whatever the callable is
            qualname = kwargs[key]

            # delete "_python_callable_qualname"
            del kwargs[key]

            # set "python_callable"
            kwargs[maybe_qualname_key] = _import_from_qualname(qualname)
    return args, kwargs


def get_and_check_airflow_version() -> float:
    from airflow import __version__

    # normalize 2.2.5+astro.6 to 2.2.5
    [version, _] = __version__.rsplit("+", 1)
    # normalize 2.2.5 to 2.2
    [major_minor, _] = version.rsplit(".", 1)
    major, minor = major_minor.split(".")
    af_version = float(major_minor)
    if int(major) != 2:
        raise RuntimeError("PostIsolationHook only works with Airflow 2.x!")
    return af_version


class PostIsolationHook:
    required_environment_variables = {
        _get_operator_from_env.__name__: "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
        _get_operator_args_from_env.__name__: "__ISOLATED_OPERATOR_OPERATOR_ARGS",
        _get_context_from_env.__name__: "__ISOLATED_OPERATOR_AIRFLOW_CONTEXT",
    }

    # noinspection PyMethodMayBeStatic
    @classmethod
    def run_isolated_task(cls):
        """Run an Isolated Airflow Task inside a Container
        1) get operator from ENV VAR - e.g. __ISOLATED_OPERATOR_OPERATOR_QUALNAME=a.b.c.d.MyOperator
        2) get args/kwargs from ENV VAR - e.g. __ISOLATED_OPERATOR_OPERATOR_ARGS=<b64 {"args": [], "kwargs": {}}
        3) get Airflow Context from ENV VAR
        4) Execute the Operator with args/kwargs, hijack task.post_execute to write the XCOM
        """
        from airflow.models.taskinstance import set_current_context, TaskInstance

        required_task_args = {"run_id": "RUNID", "state": "running"}

        get_and_check_airflow_version()

        operator = _get_operator_from_env()
        _validate_operator_is_operator(operator)

        args, kwargs = _get_operator_args_from_env()
        _verify_kwargs(kwargs)
        _transform_args(args, kwargs)
        task = operator(*args, **kwargs)
        _patch_task_dependencies(task)
        _patch_post_execute_for_xcom(task)

        context = _get_context_from_env()
        with set_current_context(context):
            # needed execution_date here????

            ti = TaskInstance(task=task, **required_task_args)
            _patch_clear_xcom_data(ti)

            # noinspection PyProtectedMember
            ti._execute_task_with_callbacks(context, test_mode=True)
