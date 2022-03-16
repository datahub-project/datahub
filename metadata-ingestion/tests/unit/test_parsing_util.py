from datahub.utilities.parsing_util import get_missing_key


def test_get_missing_key():
    assert get_missing_key({}, [""]) == ""
    assert get_missing_key({"a": 1}, ["a"]) is None
    assert get_missing_key({"a": {"b": 1}}, ["a", "b"]) is None
    assert get_missing_key({"a": {"b": 1}}, ["a", "c"]) == "c"
