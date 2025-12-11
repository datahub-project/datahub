# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
