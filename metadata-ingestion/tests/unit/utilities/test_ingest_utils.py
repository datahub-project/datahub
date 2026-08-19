import pathlib

import pytest

from datahub.utilities.ingest_utils import (
    _make_ingestion_urn,
    deploy_source_vars,
    parse_json_list,
)


def test_make_ingestion_urn():
    name = "test"
    urn = _make_ingestion_urn(name)
    assert (
        urn == "urn:li:dataHubIngestionSource:deploy-2b895b6efaa28b818284e5c696a18799"
    )


def test_deploy_source_vars():
    name = "test"
    config = pathlib.Path(__file__).parent / "sample_demo.dhub.yaml"
    urn = None
    executor_id = "default"
    cli_version = "0.15.0.1"
    schedule = "5 4 * * *"
    time_zone = "UTC"
    extra_pip = '["pandas"]'
    debug = False

    deploy_vars = deploy_source_vars(
        name,
        str(config),
        urn,
        executor_id,
        cli_version,
        schedule,
        time_zone,
        extra_pip,
        debug,
    )
    assert deploy_vars == {
        "urn": "urn:li:dataHubIngestionSource:deploy-2b895b6efaa28b818284e5c696a18799",
        "input": {
            "name": "test",
            "schedule": {
                "interval": "5 4 * * *",
                "timezone": "UTC",
            },
            "type": "demo-data",
            "config": {
                "recipe": '{"source": {"type": "demo-data", "config": {}}}',
                "debugMode": False,
                "executorId": "default",
                "version": "0.15.0.1",
                "extraArgs": [{"key": "extra_pip_requirements", "value": '["pandas"]'}],
            },
        },
    }


def test_deploy_source_vars_with_extra_env():
    name = "test"
    config = pathlib.Path(__file__).parent / "sample_demo.dhub.yaml"
    urn = None
    executor_id = "default"
    cli_version = "0.15.0.1"
    schedule = "5 4 * * *"
    time_zone = "UTC"
    extra_pip = None
    debug = False
    extra_env = "VAR1=value1,VAR2=value2"

    deploy_vars = deploy_source_vars(
        name,
        str(config),
        urn,
        executor_id,
        cli_version,
        schedule,
        time_zone,
        extra_pip,
        debug,
        extra_env,
    )
    assert deploy_vars == {
        "urn": "urn:li:dataHubIngestionSource:deploy-2b895b6efaa28b818284e5c696a18799",
        "input": {
            "name": "test",
            "schedule": {
                "interval": "5 4 * * *",
                "timezone": "UTC",
            },
            "type": "demo-data",
            "config": {
                "recipe": '{"source": {"type": "demo-data", "config": {}}}',
                "debugMode": False,
                "executorId": "default",
                "version": "0.15.0.1",
                "extraArgs": [
                    {
                        "key": "extra_env_vars",
                        "value": '{"VAR1": "value1", "VAR2": "value2"}',
                    },
                ],
            },
        },
    }


def test_deploy_source_vars_from_config_file():
    name = None
    config = pathlib.Path(__file__).parent / "sample_deploy_demo.dhub.yaml"
    urn = None
    executor_id = None
    cli_version = None
    schedule = None
    time_zone = None
    extra_pip = None
    debug = False
    extra_env = None

    deploy_vars = deploy_source_vars(
        name,
        str(config),
        urn,
        executor_id,
        cli_version,
        schedule,
        time_zone,
        extra_pip,
        debug,
        extra_env,
    )
    assert deploy_vars == {
        "urn": "urn:li:dataHubIngestionSource:deploy-9bb513c1b594da861f5a95a670938576",
        "input": {
            "name": "test-deploy",
            "schedule": {
                "interval": "5 0 * * *",
                "timezone": "Europe/London",
            },
            "type": "demo-data",
            "config": {
                "recipe": '{"source": {"type": "demo-data", "config": {}}}',
                "debugMode": False,
                "executorId": "other-default",
                "version": "1.3.1.1",
                "extraArgs": [
                    {"key": "extra_pip_requirements", "value": '["polars"]'},
                    {"key": "extra_env_vars", "value": '{"VAR0": "value0"}'},
                ],
            },
        },
    }


def test_deploy_source_name_precedence():
    name = "test"
    config = pathlib.Path(__file__).parent / "sample_deploy_demo.dhub.yaml"
    urn = None
    executor_id = None
    cli_version = None
    schedule = None
    time_zone = None
    extra_pip = None
    debug = False
    extra_env = None

    deploy_vars = deploy_source_vars(
        name,
        str(config),
        urn,
        executor_id,
        cli_version,
        schedule,
        time_zone,
        extra_pip,
        debug,
        extra_env,
    )
    assert deploy_vars == {
        "urn": "urn:li:dataHubIngestionSource:deploy-2b895b6efaa28b818284e5c696a18799",
        "input": {
            "name": "test",
            "schedule": {
                "interval": "5 0 * * *",
                "timezone": "Europe/London",
            },
            "type": "demo-data",
            "config": {
                "recipe": '{"source": {"type": "demo-data", "config": {}}}',
                "debugMode": False,
                "executorId": "other-default",
                "version": "1.3.1.1",
                "extraArgs": [
                    {"key": "extra_pip_requirements", "value": '["polars"]'},
                    {"key": "extra_env_vars", "value": '{"VAR0": "value0"}'},
                ],
            },
        },
    }


def test_deploy_source_vars_with_datahub_plugins():
    name = "test"
    config = pathlib.Path(__file__).parent / "sample_demo.dhub.yaml"

    deploy_vars = deploy_source_vars(
        name,
        str(config),
        urn=None,
        executor_id="default",
        cli_version="0.15.0.1",
        schedule=None,
        time_zone=None,
        extra_pip=None,
        debug=False,
        extra_env=None,
        datahub_plugins='["github:acme/salesforce-source@v1.0"]',
    )
    assert deploy_vars["input"]["config"]["extraArgs"] == [
        {
            "key": "datahub_plugins",
            "value": '["github:acme/salesforce-source@v1.0"]',
        },
    ]


def test_deploy_source_vars_precedence():
    name = None
    config = pathlib.Path(__file__).parent / "sample_deploy_demo.dhub.yaml"
    urn = None
    executor_id = "default"
    cli_version = "0.15.0.1"
    schedule = "5 4 * * *"
    time_zone = "UTC"
    extra_pip = '["pandas"]'
    debug = False
    extra_env = "VAR1=value1,VAR2=value2"

    deploy_vars = deploy_source_vars(
        name,
        str(config),
        urn,
        executor_id,
        cli_version,
        schedule,
        time_zone,
        extra_pip,
        debug,
        extra_env,
    )
    assert deploy_vars == {
        "urn": "urn:li:dataHubIngestionSource:deploy-9bb513c1b594da861f5a95a670938576",
        "input": {
            "name": "test-deploy",
            "schedule": {
                "interval": "5 4 * * *",
                "timezone": "UTC",
            },
            "type": "demo-data",
            "config": {
                "recipe": '{"source": {"type": "demo-data", "config": {}}}',
                "debugMode": False,
                "executorId": "default",
                "version": "0.15.0.1",
                "extraArgs": [
                    {"key": "extra_pip_requirements", "value": '["pandas"]'},
                    {
                        "key": "extra_env_vars",
                        "value": '{"VAR1": "value1", "VAR2": "value2"}',
                    },
                ],
            },
        },
    }


def test_deploy_source_vars_malformed_extra_env():
    """extra_env entries without '=' raise ValueError."""
    config = pathlib.Path(__file__).parent / "sample_demo.dhub.yaml"

    with pytest.raises(ValueError, match="Invalid extra_env entry"):
        deploy_source_vars(
            "test",
            str(config),
            urn=None,
            executor_id="default",
            cli_version="0.15.0.1",
            schedule=None,
            time_zone=None,
            extra_pip=None,
            debug=False,
            extra_env="GOOD=value,BAD_NO_EQUALS",
        )


def test_deploy_source_vars_malformed_datahub_plugins():
    """datahub_plugins with invalid JSON raises ValueError."""
    config = pathlib.Path(__file__).parent / "sample_demo.dhub.yaml"

    with pytest.raises(ValueError, match="datahub_plugins must be a valid JSON array"):
        deploy_source_vars(
            "test",
            str(config),
            urn=None,
            executor_id="default",
            cli_version="0.15.0.1",
            schedule=None,
            time_zone=None,
            extra_pip=None,
            debug=False,
            datahub_plugins="{not valid json",
        )


def test_parse_json_list_rejects_non_string_elements():
    """parse_json_list should reject arrays with non-string elements."""
    with pytest.raises(ValueError, match=r"value\[1\] must be a string, got int"):
        parse_json_list('["ok", 42]')


def test_parse_json_list_rejects_null_elements():
    with pytest.raises(ValueError, match=r"value\[0\] must be a string, got NoneType"):
        parse_json_list("[null]")


def test_parse_json_list_accepts_all_strings():
    assert parse_json_list('["a", "b", "c"]') == ["a", "b", "c"]


def test_deploy_source_vars_datahub_plugins_not_array():
    """datahub_plugins with non-array JSON raises ValueError."""
    config = pathlib.Path(__file__).parent / "sample_demo.dhub.yaml"

    with pytest.raises(ValueError, match="datahub_plugins must be a JSON array"):
        deploy_source_vars(
            "test",
            str(config),
            urn=None,
            executor_id="default",
            cli_version="0.15.0.1",
            schedule=None,
            time_zone=None,
            extra_pip=None,
            debug=False,
            datahub_plugins='"just-a-string"',
        )
