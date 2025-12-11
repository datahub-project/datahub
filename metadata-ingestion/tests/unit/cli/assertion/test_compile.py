# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import filecmp
import os

from datahub.integrations.assertion.snowflake.compiler import (
    DMF_ASSOCIATIONS_FILE_NAME,
    DMF_DEFINITIONS_FILE_NAME,
)
from tests.test_helpers.click_helpers import run_datahub_cmd


def test_compile_assertion_config_spec_for_snowflake(pytestconfig, tmp_path):
    config_file = (
        pytestconfig.rootpath
        / "tests/unit/api/entities/assertion/test_assertion_config.yml"
    ).resolve()

    golden_file_path = pytestconfig.rootpath / "tests/unit/cli/assertion/"
    run_datahub_cmd(
        [
            "assertions",
            "compile",
            "-f",
            f"{config_file}",
            "-p",
            "snowflake",
            "-x",
            "DMF_SCHEMA=test_db.datahub_dmfs",
            "-o",
            tmp_path,
        ],
    )

    output_file_names = [
        DMF_DEFINITIONS_FILE_NAME,
        DMF_ASSOCIATIONS_FILE_NAME,
    ]

    for file_name in output_file_names:
        assert os.path.exists(tmp_path / file_name)
        assert filecmp.cmp(golden_file_path / file_name, tmp_path / file_name), (
            f"{file_name} is not as expected"
        )
