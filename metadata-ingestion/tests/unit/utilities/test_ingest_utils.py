import pathlib

from datahub.utilities.ingest_utils import (
    _make_ingestion_urn,
    deploy_source_vars,
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
    cli_version = None
    schedule = None
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
            "type": "demo-data",
            "config": {
                "recipe": '{"source": {"type": "demo-data", "config": {}}}',
                "debugMode": False,
                "executorId": "default",
                "version": None,
                "extraArgs": [{"key": "extra_pip_requirements", "value": '["pandas"]'}],
            },
        },
    }
