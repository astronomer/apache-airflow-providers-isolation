import hashlib

from isolation.util import fix_unusual_prefix_serialized_dag_issue


def test_fix_unusual_prefix_serialized_dag_issue(project_root):
    util = project_root / "isolation/util.py"
    _hash = hashlib.sha1(str(util).encode("utf-8")).hexdigest()
    test_bad_qualname = f"unusual_prefix_{_hash}_util.fix_unusual_prefix_serialized_dag_issue"
    actual = fix_unusual_prefix_serialized_dag_issue(test_bad_qualname, str(util.parent))
    expected = "util.fix_unusual_prefix_serialized_dag_issue"
    assert actual == expected
