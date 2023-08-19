from isolationctl import extract_kubeconfig_to_str, print_table


def test_extract_kubeconfig_to_str():
    actual = extract_kubeconfig_to_str()
    assert "apiVersion" in actual


def test_print_table():
    actual = print_table(["aaa", "bb"], [["bbbbb", "cccc"], ["b", "ccc"]])
    expected = """
aaa     | bb      |
------------------|
bbbbb   | cccc    |
b       | ccc     |
"""
    assert actual == expected
