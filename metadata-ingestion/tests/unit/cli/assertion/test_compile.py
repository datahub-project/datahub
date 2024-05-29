import filecmp
import os

from tests.test_helpers.click_helpers import run_datahub_cmd


def test_compile_assertion_config_spec_for_snowflake(pytestconfig, tmp_path):
    config_file = (
        pytestconfig.rootpath
        / "tests/unit/api/entities/assertion/test_assertion_config.yml"
    ).resolve()

    test_file = pytestconfig.rootpath / "tests/unit/cli/assertion/test_bootstrap.sql"
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

    assert os.path.exists(tmp_path / "bootstrap.sql")
    filecmp.cmp(test_file, tmp_path / "bootstrap.sql")
