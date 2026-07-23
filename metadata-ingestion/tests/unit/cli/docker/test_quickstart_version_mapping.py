from unittest import mock

import pytest
from click import ClickException

from datahub.cli.quickstart_versioning import (
    QuickstartExecutionPlan,
    QuickstartVersionMappingConfig,
)

example_version_mapper = QuickstartVersionMappingConfig.model_validate(
    {
        "quickstart_version_map": {
            "default": {
                "composefile_git_ref": "master",
                "docker_tag": "head",
                "mysql_tag": "8.2",
            },
            "v0.9.6": {
                "composefile_git_ref": "v0.9.6.1",
                "docker_tag": "v0.9.6.1",
                "mysql_tag": "5.7",
            },
            "v2.0.0": {
                "composefile_git_ref": "v2.0.1",
                "docker_tag": "v2.0.0",
                "mysql_tag": "8.2",
            },
            "v1.2.0": {
                "composefile_git_ref": "v1.2.0",
                "docker_tag": "v1.2.0",
                "mysql_tag": "8.2",
            },
            "stable": {
                "composefile_git_ref": "v1.2.1",
                "docker_tag": "v1.2.1",
                "mysql_tag": "8.2",
            },
        },
    }
)


def test_quickstart_version_config():
    execution_plan = example_version_mapper.get_quickstart_execution_plan("v1.2.0")
    expected = QuickstartExecutionPlan(
        docker_tag="v1.2.0",
        composefile_git_ref="v1.2.0",
        mysql_tag="8.2",
    )
    assert execution_plan == expected


def test_quickstart_version_config_default():
    execution_plan = example_version_mapper.get_quickstart_execution_plan("v2.0.0")
    expected = QuickstartExecutionPlan(
        docker_tag="v2.0.0",
        composefile_git_ref="v2.0.1",
        mysql_tag="8.2",
    )
    assert execution_plan == expected


def test_quickstart_version_config_stable():
    execution_plan = example_version_mapper.get_quickstart_execution_plan("stable")
    expected = QuickstartExecutionPlan(
        docker_tag="v1.2.1", composefile_git_ref="v1.2.1", mysql_tag="8.2"
    )
    assert execution_plan == expected


def test_quickstart_no_version_does_not_prompt():
    with mock.patch("datahub.cli.quickstart_versioning.click.confirm") as mock_confirm:
        example_version_mapper.get_quickstart_execution_plan(None)
        example_version_mapper.get_quickstart_execution_plan("default")
    mock_confirm.assert_not_called()


def test_quickstart_forced_stable():
    version_mapper = QuickstartVersionMappingConfig.model_validate(
        {
            "quickstart_version_map": {
                **example_version_mapper.quickstart_version_map,
                "default": {
                    "composefile_git_ref": "v1.2.0",
                    "docker_tag": "head",
                    "mysql_tag": "8.2",
                },
            }
        }
    )
    execution_plan = version_mapper.get_quickstart_execution_plan(None)
    expected = QuickstartExecutionPlan(
        docker_tag="quickstart",
        composefile_git_ref="v1.2.0",
        mysql_tag="8.2",
    )
    assert execution_plan == expected


def test_quickstart_head_maps_to_quickstart_docker_tag():
    version_mapper = QuickstartVersionMappingConfig.model_validate(
        {
            "quickstart_version_map": {
                "head": {
                    "composefile_git_ref": "master",
                    "docker_tag": "quickstart",
                    "mysql_tag": "8.2",
                },
            },
        }
    )
    with mock.patch("datahub.cli.quickstart_versioning.click.confirm") as mock_confirm:
        execution_plan = version_mapper.get_quickstart_execution_plan("head")
    mock_confirm.assert_not_called()
    assert execution_plan == QuickstartExecutionPlan(
        docker_tag="quickstart",
        composefile_git_ref="master",
        mysql_tag="8.2",
    )


def test_quickstart_head_missing_from_map_prompts_master_quickstart():
    version_mapper = QuickstartVersionMappingConfig.model_validate(
        {
            "quickstart_version_map": {
                "default": {
                    "composefile_git_ref": "v1.5.0.6",
                    "docker_tag": "v1.5.0.6",
                    "mysql_tag": "8.2",
                },
            },
        }
    )
    with (
        mock.patch(
            "datahub.cli.quickstart_versioning.sys.stdin.isatty", return_value=True
        ),
        mock.patch(
            "datahub.cli.quickstart_versioning.click.confirm", return_value=True
        ) as mock_confirm,
    ):
        execution_plan = version_mapper.get_quickstart_execution_plan("head")
    mock_confirm.assert_called_once()
    assert execution_plan == QuickstartExecutionPlan(
        docker_tag="quickstart",
        composefile_git_ref="master",
        mysql_tag="8.2",
    )


def test_quickstart_head_missing_from_map_rejected_on_no():
    version_mapper = QuickstartVersionMappingConfig.model_validate(
        {
            "quickstart_version_map": {
                "default": {
                    "composefile_git_ref": "v1.5.0.6",
                    "docker_tag": "v1.5.0.6",
                    "mysql_tag": "8.2",
                },
            },
        }
    )
    with (
        mock.patch(
            "datahub.cli.quickstart_versioning.sys.stdin.isatty", return_value=True
        ),
        mock.patch(
            "datahub.cli.quickstart_versioning.click.confirm", return_value=False
        ),
        pytest.raises(ClickException, match="Aborted"),
    ):
        version_mapper.get_quickstart_execution_plan("head")


def test_quickstart_sha_tag_passthrough():
    version_mapper = QuickstartVersionMappingConfig.model_validate(
        {
            "quickstart_version_map": {
                "default": example_version_mapper.quickstart_version_map["default"]
            }
        }
    )
    with mock.patch("datahub.cli.quickstart_versioning.click.confirm") as mock_confirm:
        execution_plan = version_mapper.get_quickstart_execution_plan("sha-abc1234")
    mock_confirm.assert_not_called()
    assert execution_plan.docker_tag == "sha-abc1234"
    assert execution_plan.composefile_git_ref == "sha-abc1234"


def test_quickstart_semver_not_in_map_does_not_prompt():
    with mock.patch("datahub.cli.quickstart_versioning.click.confirm") as mock_confirm:
        execution_plan = example_version_mapper.get_quickstart_execution_plan("v9.9.9")
    mock_confirm.assert_not_called()
    assert execution_plan == QuickstartExecutionPlan(
        docker_tag="v9.9.9",
        composefile_git_ref="v9.9.9",
        mysql_tag="8.2",
    )


def test_quickstart_unrecognized_version_prompts_default_on_yes():
    with (
        mock.patch(
            "datahub.cli.quickstart_versioning.sys.stdin.isatty", return_value=True
        ),
        mock.patch(
            "datahub.cli.quickstart_versioning.click.confirm", return_value=True
        ) as mock_confirm,
    ):
        execution_plan = example_version_mapper.get_quickstart_execution_plan(
            "NOT A VERSION"
        )
    mock_confirm.assert_called_once()
    assert execution_plan == QuickstartExecutionPlan(
        docker_tag="quickstart",
        composefile_git_ref="master",
        mysql_tag="8.2",
    )


def test_quickstart_unrecognized_version_rejected_on_no():
    with (
        mock.patch(
            "datahub.cli.quickstart_versioning.sys.stdin.isatty", return_value=True
        ),
        mock.patch(
            "datahub.cli.quickstart_versioning.click.confirm", return_value=False
        ),
        pytest.raises(ClickException, match="Aborted"),
    ):
        example_version_mapper.get_quickstart_execution_plan("NOT A VERSION")


def test_quickstart_unrecognized_version_accept_flag_skips_prompt():
    with mock.patch("datahub.cli.quickstart_versioning.click.confirm") as mock_confirm:
        execution_plan = example_version_mapper.get_quickstart_execution_plan(
            "NOT A VERSION",
            accept_version_default=True,
        )
    mock_confirm.assert_not_called()
    assert execution_plan == QuickstartExecutionPlan(
        docker_tag="quickstart",
        composefile_git_ref="master",
        mysql_tag="8.2",
    )


def test_quickstart_unrecognized_version_non_interactive_aborts():
    with (
        mock.patch(
            "datahub.cli.quickstart_versioning.sys.stdin.isatty", return_value=False
        ),
        pytest.raises(ClickException, match="non-interactive"),
    ):
        example_version_mapper.get_quickstart_execution_plan("NOT A VERSION")


def test_quickstart_get_older_version():
    with pytest.raises(ClickException, match="Minimum supported version not met"):
        example_version_mapper.get_quickstart_execution_plan("v0.9.6")


def test_quickstart_version_older_than_v1_2_0_uses_commit_hash():
    """
    Test that versions older than v1.2.0 get their composefile_git_ref set to the specific commit hash
    that contains the profiles based resolved compose file.
    """
    version_mapper = QuickstartVersionMappingConfig.model_validate(
        {
            "quickstart_version_map": {
                "v1.1.0": {
                    "composefile_git_ref": "v1.1.0",
                    "docker_tag": "v1.1.0",
                    "mysql_tag": "8.2",
                },
            },
        }
    )

    execution_plan = version_mapper.get_quickstart_execution_plan("v1.1.0")
    expected = QuickstartExecutionPlan(
        docker_tag="v1.1.0",
        composefile_git_ref="21726bc3341490f4182b904626c793091ac95edd",
        mysql_tag="8.2",
    )
    assert execution_plan == expected
