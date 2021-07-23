from click.testing import Result


def assert_result_ok(result: Result) -> None:
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
