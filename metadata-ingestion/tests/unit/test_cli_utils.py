from datahub.cli import cli_utils


def test_first_non_null():
    assert cli_utils.first_non_null([]) is None
    assert cli_utils.first_non_null([None]) is None
    assert cli_utils.first_non_null([None, "1"]) == "1"
    assert cli_utils.first_non_null([None, "1", "2"]) == "1"
    assert cli_utils.first_non_null(["3", "1", "2"]) == "3"
    assert cli_utils.first_non_null(["", "1", "2"]) == "1"
    assert cli_utils.first_non_null([" ", "1", "2"]) == "1"
