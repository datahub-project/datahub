from tests.test_helpers.click_helpers import run_datahub_cmd


def test_cli_help():
    result = run_datahub_cmd(["--help"])
    assert result.output


def test_cli_version():
    result = run_datahub_cmd(["--debug", "version"])
    assert result.output


def test_check_local_docker():
    # This just verifies that it runs without error.
    # We don't actually know what environment this will be run in, so
    # we can't depend on the output. Eventually, we should mock the docker SDK.
    result = run_datahub_cmd(["check", "local-docker"], check_result=False)
    assert result.output
