import pytest

from isolation.operators.isolation import IsolatedOperator
from isolation.operators.isolation_kubernetes import IsolatedKubernetesPodOperator


def test_isolated_operator__new__():
    actual = IsolatedOperator(task_id="foo", image="", operator=IsolatedOperator)
    assert isinstance(actual, IsolatedKubernetesPodOperator), "isolation_type defaults to kubernetes"

    actual = IsolatedOperator(task_id="foo", isolation_type="kubernetes", image="", operator=IsolatedOperator)
    assert isinstance(
        actual, IsolatedKubernetesPodOperator
    ), "isolation_type=kubernetes gives us IsolatedKubernetesPodOperator"

    with pytest.raises(NotImplementedError):
        # noinspection PyTypeChecker
        IsolatedOperator(task_id="foo", isolation_type="something_else", image="", operator=IsolatedOperator)
