from isolationctl import extract_kubeconfig_to_str, print_table


def test_extract_kubeconfig_to_str():
    actual = extract_kubeconfig_to_str()
    assert "apiVersion" in actual


def test_print_table_one_col():
    actual = print_table(["aaa"], [["bbbbb"], ["b"]])
    expected = """
aaa     |
--------|
bbbbb   |
b       |
"""
    assert actual == expected


def test_print_table_many_col():
    actual = print_table(["aaa", "bb"], [["bbbbb", "cccc"], ["b", "ccc"]])
    expected = """
aaa     | bb      |
------------------|
bbbbb   | cccc    |
b       | ccc     |
"""
    assert actual == expected
