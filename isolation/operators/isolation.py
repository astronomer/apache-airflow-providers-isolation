from abc import abstractmethod
from typing import Literal

from airflow.models import BaseOperator


class IsolatedOperator(BaseOperator):
    def __new__(cls, isolation_type: Literal["kubernetes"] = "kubernetes", *args, **kwargs):
        if isolation_type == "kubernetes":
            from isolation.operators.isolation_kubernetes import IsolatedKubernetesPodOperator

            return IsolatedKubernetesPodOperator(*args, **kwargs)
        else:
            raise NotImplementedError("Valid values for isolation_type are: ['kubernetes']")

    @abstractmethod
    def _set_operator_via_env(self):
        raise NotImplementedError

    @abstractmethod
    def _set_operator_args_via_env(self):
        raise NotImplementedError

    @abstractmethod
    def _set_airflow_context_via_env(self):
        raise NotImplementedError

    settable_environment_variables = {
        _set_operator_via_env.__name__: "__ISOLATED_OPERATOR_OPERATOR_QUALNAME",
        _set_operator_args_via_env.__name__: "__ISOLATED_OPERATOR_OPERATOR_ARGS",
        _set_airflow_context_via_env.__name__: "__ISOLATED_OPERATOR_AIRFLOW_CONTEXT",
    }
