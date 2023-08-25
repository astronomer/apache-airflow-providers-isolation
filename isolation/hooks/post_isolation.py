import itertools
import re
from copy import copy
from pathlib import Path
from typing import Any, Dict, Type, Tuple, List, Optional

from isolation.util import (
    b64decode_json,
    getenv_or_raise,
    get_and_check_airflow_version,
    validate_operator_is_operator,
    import_from_qualname,
)

XCOM_FILE = "/airflow/xcom/return.json"


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


def _get_operator_args_via_env() -> Tuple[List[Any], Dict[str, Any]]:
    # noinspection PyUnresolvedReferences, PyProtectedMember
    """Return *args and **kwargs for the operator,
    encoded as {"args":args,"kwargs":kwargs} as a b64 encoded json

    :raises: RuntimeError if extracted args is not a list
    :raises: RuntimeError if extracted kwargs is not a dict
    >>> from isolation.util import b64encode_json; import datetime, base64, json, os
    >>> _get_operator_args_via_env()  # Unset
    Traceback (most recent call last):
        ...
    RuntimeError: __ISOLATED_OPERATOR_OPERATOR_ARGS must be set!
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_ARGS'] = ""; _get_operator_args_via_env()   # Empty
    ([], {})
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_ARGS'] = b64encode_json(
    ...     {"args":["foo", 1], "kwargs":{"a": 1, "b": 2, "c": datetime.datetime(1970, 1, 1)}},
    ... ); _get_operator_args_via_env()   # Happy Path
    (['foo', 1], {'a': 1, 'b': 2, 'c': '1970-01-01 00:00:00'})
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_ARGS'] = b64encode_json(
    ...     {"args": [], "kwargs": {}}
    ... ); _get_operator_args_via_env()   # Happy & Empty
    ([], {})
    """
    key = PostIsolationHook.required_environment_variables[_get_operator_args_via_env.__name__]
    encoded_args_json = getenv_or_raise(key)
    if not encoded_args_json:
        return [], {}
    args_json = b64decode_json(encoded_args_json)
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


def _get_context_via_env() -> "Context":  # noqa: F821
    # noinspection PyUnresolvedReferences,PyTrailingSemicolon,SpellCheckingInspection
    """Retrieve a json-serialized version of a subset
    of the Airflow Context object and recreate
    >>> from isolation.util import b64encode_json; import os
    >>> a = "eyJkcyI6ICIxOTcwLTAxLTAxVDAwOjAwOjAwIiwgInBhcmFtcyI6IHsiZm9vIjogImJhciJ9fQ=="
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = a
    >>> r = _get_context_via_env(); r['params'], type(r['ds'])
    ({'foo': 'bar'}, <class 'pendulum.datetime.DateTime'>)
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = b64encode_json(
    ...     {}
    ... ); r = _get_context_via_env(); r['params'], type(r['ds']) # Empty
    ({}, <class 'pendulum.datetime.DateTime'>)
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = ''; r = _get_context_via_env(); r['params'], type(r['ds'])
    ({}, <class 'pendulum.datetime.DateTime'>)
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = b64encode_json(
    ...     {"ds": "foo"}
    ... ); r = _get_context_via_env() # bad de-ser data
    Traceback (most recent call last):
        ...
    pendulum.parsing.exceptions.ParserError: Unable to parse string [foo]
    >>> os.environ['__ISOLATED_OPERATOR_AIRFLOW_CONTEXT'] = b64encode_json(
    ...     {"params": {"foo": "bar"}}
    ... ); r = _get_context_via_env(); r['params'], type(r['params'])  # good params data
    ({'foo': 'bar'}, <class 'airflow.models.param.ParamsDict'>)
    """
    from airflow.utils.context import Context
    import pendulum
    from airflow.models.param import ParamsDict

    key = PostIsolationHook.required_environment_variables[_get_context_via_env.__name__]
    encoded_context_json = getenv_or_raise(key)
    context_json = b64decode_json(encoded_context_json) if encoded_context_json != "" else {}
    if not isinstance(context_json, dict):
        raise RuntimeError(f"{key} did not decode to JSON - Value: {encoded_context_json}, Decoded: {context_json}")

    required_keys = ["ds", "params"]
    context_serialization_mappings = {
        # Key:  (default-fn,  mapping-fn)
        "params": (dict, ParamsDict),
        "ds": (pendulum.now, pendulum.parse),
    }
    # we check over both what we got AND the required ones
    for k in set(itertools.chain(context_json.keys(), required_keys)):
        if k in context_json:  # if it's one of the ones we got
            if k in context_serialization_mappings:  # and if we have a mapping
                context_json[k] = context_serialization_mappings[k][1](context_json[k])  # map it
            else:  # else we got it, but
                pass  # it's fine as-is
        else:  # if it's required, wasn't given, and needs to be defaulted
            context_json[k] = context_serialization_mappings[k][0]()  # default it
    return Context(**context_json)


def _get_operator_via_env() -> Type["BaseOperator"]:  # noqa: F821
    """Instantiate the operator via the qualified name saved in __ISOLATED_OPERATOR_OPERATOR_QUALNAME
    >>> import os
    >>> os.environ['__ISOLATED_OPERATOR_OPERATOR_QUALNAME']="datetime.datetime"; _get_operator_via_env()
    <class 'datetime.datetime'>
    """
    # Sanity check - __ISOLATED_OPERATOR_OPERATOR_QUALNAME=a.b.c.d.MyOperator
    key = PostIsolationHook.required_environment_variables[_get_operator_via_env.__name__]
    operator_qualname = getenv_or_raise(key)
    # split out - a.b.c.d, MyOperator
    return import_from_qualname(operator_qualname)


def _patch_task_dependencies(task) -> None:
    """We overwrite the method to avoid the DB call"""

    def blackhole() -> iter:
        return iter([])

    task.iter_mapped_dependants = blackhole


def _patch_post_execute_for_xcom(task) -> None:
    """Patch the 'task.post_execute' method - write a xcom then run the original"""
    original_fn = task.post_execute

    def new_fn(context, result):
        if result:
            xcom_path = Path(XCOM_FILE)
            try:
                xcom_path.parent.mkdir(parents=True, exist_ok=True)
                with xcom_path.open("w") as f:
                    f.write(result)
            except PermissionError as e:
                raise RuntimeError("Unable to write XCOM") from e
        return original_fn(context, result)

    task.post_execute = new_fn


def _patch_clear_xcom_data(task_instance) -> None:
    """Patch the 'task_instance.clear_xcom_data' method - just respond immediately, skip DB"""

    def blackhole() -> None:
        return

    task_instance.clear_xcom_data = blackhole


def _get_callable_qualname(key: str) -> Optional[str]:
    """We should be getting xyz_callable as _xyz_callable_qualname - find and return if so
    >>> _get_callable_qualname("_xyz_qualname")
    'xyz'
    >>> _get_callable_qualname("_python_callable_qualname")
    'python_callable'
    >>> _get_callable_qualname("definitely_not_python_callable")
    >>> _get_callable_qualname("bash_command")
    """
    maybe_match = re.match(pattern="[_](.*)(?=_qualname)", string=key)
    return maybe_match.group(1) if maybe_match is not None else None


def _transform_args(args: List[Any], kwargs: Dict[str, Any]) -> Tuple[List[Any], Dict[str, Any]]:
    """
    Look for anything like '_xyz_qualname' or '_python_callable_qualname' in the kwargs, remap it to a fn
    >>> _transform_args([], {
    ...     "_python_callable_qualname": "tests.resources.fn.direct.print_magic_string_test_direct"
    ... })  # doctest: +ELLIPSIS
    ([], {'python_callable': <function print_magic_string_test_direct at ...
    """
    dict_keys = copy(list(kwargs.keys()))
    for key in dict_keys:
        maybe_qualname_key = _get_callable_qualname(key)
        if maybe_qualname_key:
            # save whatever the callable is
            qualname = kwargs[key]

            # delete "_python_callable_qualname"
            del kwargs[key]

            # set "python_callable"
            kwargs[maybe_qualname_key] = import_from_qualname(qualname)
    return args, kwargs


class PostIsolationHook:
    required_environment_variables = {
        _get_operator_via_env.__name__: "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
        _get_operator_args_via_env.__name__: "__ISOLATED_OPERATOR_OPERATOR_ARGS",
        _get_context_via_env.__name__: "__ISOLATED_OPERATOR_AIRFLOW_CONTEXT",
    }

    # noinspection PyMethodMayBeStatic
    @classmethod
    def run_isolated_task(cls) -> None:
        """Run an Isolated Airflow Task inside a Container
        1) get operator from ENV VAR - e.g. __ISOLATED_OPERATOR_OPERATOR_QUALNAME=a.b.c.d.MyOperator
        2) get args/kwargs from ENV VAR - e.g. __ISOLATED_OPERATOR_OPERATOR_ARGS=<b64 {"args": [], "kwargs": {}}
        3) get Airflow Context from ENV VAR e.g. __ISOLATED_OPERATOR_AIRFLOW_CONTEXT=<b64 {"ds": "1970-01-01", ...}
        4) Execute the Operator with args/kwargs, hijack task.post_execute to write the XCOM
        """
        from airflow.models.taskinstance import set_current_context, TaskInstance

        # Do some sanity checks - check that we are using AF2.x
        # returns the AF major.minor version if we need to check compatability later
        get_and_check_airflow_version()

        operator = _get_operator_via_env()
        validate_operator_is_operator(operator)

        args, kwargs = _get_operator_args_via_env()
        _verify_kwargs(kwargs)
        _transform_args(args, kwargs)
        task = operator(*args, **kwargs)
        _patch_task_dependencies(task)
        _patch_post_execute_for_xcom(task)

        context = _get_context_via_env()
        with set_current_context(context):
            # noinspection SpellCheckingInspection
            required_task_args = {"run_id": "RUNID", "state": "running"}
            ti = TaskInstance(task=task, **required_task_args)
            _patch_clear_xcom_data(ti)
            # noinspection PyProtectedMember
            ti._execute_task_with_callbacks(context, test_mode=True)
