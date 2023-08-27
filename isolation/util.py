import base64
import binascii
import hashlib
import inspect
import json
import os
import re
import sys
from importlib import import_module
from pathlib import Path
from typing import Dict, Any, Optional, Type, Union, Callable


def b64encode_json(d: Dict[str, Any]) -> str:
    """helper utility to encode a dict to a b64 string

    :raises: Exception if input is not JSON serializable
    >>> b64encode_json({"foo": "bar"})
    'eyJmb28iOiAiYmFyIn0='
    """
    try:
        return base64.b64encode(json.dumps(d, default=str).encode()).decode()
    except TypeError as e:
        raise RuntimeError(f"Unable to serialize! Given: {d}") from e


def b64decode_json(s: str) -> Dict[str, Any]:
    """helper utility to decode a b64 to a json dict

    :raises: RuntimeError if input is not b64
    :raises: RuntimeError if input is not valid JSON
    >>> b64decode_json("InsnZHMnOiAnZm9vJywgJ3BhcmFtcyc6IHsnZm9vJzogJ2Jhcid9fSI=")
    "{'ds': 'foo', 'params': {'foo': 'bar'}}"
    >>> b64decode_json('eyJmb28iOiAiYmFyIn0=')
    {'foo': 'bar'}
    >>> b64decode_json("garbage")   # Garbage - not b64
    Traceback (most recent call last):
        ...
    RuntimeError: Input is not encoded correctly as base64! Got: garbage . Cannot proceed!
    >>> b64decode_json("{{}") # Garbage - not json
    Traceback (most recent call last):
        ...
    RuntimeError: Input is not encoded correctly as json! Got: b'' from '{{}'. Cannot proceed!
    """
    try:
        decoded_j = base64.b64decode(s)
        return json.loads(decoded_j)
    except binascii.Error as e:
        raise RuntimeError(
            f"Input is not encoded correctly as base64! " f"Got: {s} . Cannot proceed!",
        ) from e
    except json.decoder.JSONDecodeError as e:
        # noinspection PyUnboundLocalVariable
        raise RuntimeError(
            f"Input is not encoded correctly as json! " f"Got: {decoded_j} from '{s}'. Cannot proceed!",
        ) from e


def getenv_or_raise(key):
    """Get the env key or throw an error
    :raises: RuntimeError if key is not set
    """
    if key not in os.environ:
        raise RuntimeError(f"{key} must be set!")
    return os.getenv(key)


def get_isolated_operator_env(
    op_qualname: str,
    kwargs: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
):
    """Returns a dict that gets passed to the Docker Container as it's `environment`"""
    return {
        "__ISOLATED_OPERATOR_OPERATOR_QUALNAME": op_qualname,
        "__ISOLATED_OPERATOR_OPERATOR_ARGS": b64encode_json({"args": [], "kwargs": kwargs}) if kwargs else "",
        "__ISOLATED_OPERATOR_AIRFLOW_CONTEXT": b64encode_json(context) if context else "",
    }


# noinspection RegExpAnonymousGroup
var_template_patterns = [
    re.compile(r"{{\s*var.value.([a-zA-Z-_]+)\s*}}"),  # "{{ var.value.<> }}"
    re.compile(r"{{\s*var.json.([a-zA-Z-_]+)\s*}}"),  # "{{ var.json.<> }}"
]
# noinspection RegExpAnonymousGroup
var_object_pattern = re.compile(r"""Variable.get[(]["']([a-zA-Z-_]+)["'][)]""")  # "Variable.get(<>)"
# noinspection RegExpAnonymousGroup
conn_property_pattern = re.compile(r"""(?=\w*_)conn_id=["']([a-zA-Z-_]+)["']""")  # "conn_id=<>"
# noinspection RegExpAnonymousGroup
conn_template_pattern = re.compile(r"[{]{2}\s*conn[.]([a-zA-Z-_]+)[.]?")  # "{{ conn.<> }}"
# noinspection RegExpAnonymousGroup
unusual_prefix_pattern = re.compile(r"unusual_prefix_(\w+)_(.+)")


def get_and_check_airflow_version() -> float:
    """Normalize the Airflow Version, Steps:
    1) normalize 2.2.5+astro.6 to 2.2.5
    2) normalize 2.2.5 to 2.2
    4) returns 2.x

    :raises: RuntimeError if the version is not Airflow 2.x
    """
    from airflow import __version__

    [version, _] = __version__.rsplit("+", 1)
    [major_minor, _] = version.rsplit(".", 1)
    major, minor = major_minor.split(".")
    af_version = float(major_minor)
    if int(major) != 2:
        raise RuntimeError("PostIsolationHook only works with Airflow 2.x!")
    return af_version


def validate_operator_is_operator(operator: Type["BaseOperator"]) -> None:  # noqa: F821
    """make sure the "operator" we were given is an Operator, e.g. BashOperator
    :raises: RuntimeError - if operator is not a subclass of BaseOperator

    >>> from airflow.operators.bash import BashOperator; validate_operator_is_operator(BashOperator)  # happy path
    >>> class MyOperator(BashOperator):
    ...    pass
    >>> validate_operator_is_operator(MyOperator)  # happy path with a custom operator
    >>> validate_operator_is_operator(dict) # non-operators don't parse
    Traceback (most recent call last):
    ...
    RuntimeError: <class 'dict'> must be a subclass of <class 'airflow.models.baseoperator.BaseOperator'>
    """
    from airflow.models import BaseOperator

    if not issubclass(operator, BaseOperator):
        raise RuntimeError(f"{operator} must be a subclass of {BaseOperator}")


def fix_unusual_prefix_serialized_dag_issue(qualname, calling_filepath: Optional[str] = None):
    """
    Issue with airflow dag serialization stuff modifying module to 'unusual_prefix_xyzabc_file' if file is dags/file.py
    https://github.com/apache/airflow/blob/88b274c95b212b541ba19918880ae425856212be/airflow/models/dagbag.py#L287
    "unusual_prefix_cc53bfb719e11e2b18b1d66382f5047c2461068c_isolation_provider_example_dag"  # pragma: allowlist secret
                    cc53bfb719e11e2b18b1d66382f5047c2461068c
                    /usr/local/airflow/dags/isolation_provider_example_dag.py
    """
    if "unusual_prefix" in qualname:
        [module, name] = qualname.rsplit(".", 1)
        matches = unusual_prefix_pattern.match(module).groups()
        if calling_filepath and matches:
            _hash, file = matches
            files = list(Path(calling_filepath).rglob("*.[pP][yY]"))
            file_results = [path for path in files if file in path.name]
            if len(file_results):
                hash_results = [
                    result
                    for result in file_results
                    if hashlib.sha1(str(result.resolve()).encode("utf-8")).hexdigest() == _hash
                ]
                if len(hash_results):
                    if len(hash_results) > 1:
                        print(
                            f"Attempting to unwind 'unusual_prefix_*'... found {len(hash_results)} files "
                            f"with SHA1 {_hash} recursively in {calling_filepath} of {len(files)} files... "
                            "Using the first..."
                        )

                    print(f"Attempting to unwind 'unusual_prefix_*'... importing {hash_results[0].parent}...")
                    sys.path.append(str(hash_results[0].parent))
                    mod = import_module(file)
                    print(f"Attempting to unwind 'unusual_prefix_*'... {mod} {mod.__loader__}")
                    obj = getattr(mod, name)
                    print(f"Attempting to unwind 'unusual_prefix_*'... {obj}")
                    return export_to_qualname(obj, validate=False, check_unusual=False)
                print(
                    f"Attempting to unwind 'unusual_prefix_*'... "
                    f"unable to find file with SHA1 {_hash} recursively in {calling_filepath} of {len(files)} files"
                )
            print(
                f"Attempting to unwind 'unusual_prefix_*'... "
                f"unable to find file {file} recursively from {calling_filepath} in {len(files)} files"
            )
        print(
            f"Attempting to unwind 'unusual_prefix_*'... Either unable to extract hash and file from {qualname}, "
            f"or unable to get calling file: {calling_filepath}, skipping..."
        )
    return qualname


def import_from_qualname(qualname) -> Type["BaseOperator"]:  # noqa: F821
    """Turn a.b.c.d.MyOperator into the actual python version
    Steps:
    1) split out - a.b.c.d, MyOperator from a.b.c.d.MyOperator
    2) import a.b.c.d
    3) return a.b.c.d.MyOperator
    """
    qualname = fix_unusual_prefix_serialized_dag_issue(qualname)
    [module, name] = qualname.rsplit(".", 1)
    imported_module = import_module(module)
    return getattr(imported_module, name)


def export_to_qualname(
    thing: Union[Callable, Type["BaseOperator"]],  # noqa: F821
    validate: Optional[bool] = True,
    calling_dag_filename: Optional[str] = None,
    check_unusual: Optional[bool] = True,
) -> str:
    """Turn an Operator into it's qualified name
    e.g. BashOperator -> 'airflow.operators.bash.BashOperator'
    :raises: RuntimeError if Operator does not inherit from BaseOperator

    >>> from airflow.operators.bash import BashOperator; export_to_qualname(thing=BashOperator)
    'airflow.operators.bash.BashOperator'
    """
    if validate:
        validate_operator_is_operator(thing)
    qualname = f"{inspect.getmodule(thing).__name__}.{thing.__name__}"
    if check_unusual:
        qualname = fix_unusual_prefix_serialized_dag_issue(qualname, calling_dag_filename)
    return qualname
