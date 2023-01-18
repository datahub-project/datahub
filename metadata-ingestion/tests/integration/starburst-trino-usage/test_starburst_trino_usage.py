import json
import pathlib
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.usage.starburst_trino_usage import TrinoUsageConfig
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-08-24 09:00:00"


def test_trino_usage_config():
    config = TrinoUsageConfig.parse_obj(
        dict(
            host_port="xxxxx",
            database="testcatalog",
            username="xxxxx",
            password="xxxxx",
            email_domain="xxxxx",
            audit_catalog="xxxxx",
            audit_schema="xxxxx",
            include_views=True,
            include_tables=True,
        )
    )

    assert config.host_port == "xxxxx"
    assert config.database == "testcatalog"
    assert config.username == "xxxxx"
    assert config.email_domain == "xxxxx"
    assert config.audit_catalog == "xxxxx"
    assert config.audit_schema == "xxxxx"
    assert config.include_views
    assert config.include_tables


@freeze_time(FROZEN_TIME)
def test_trino_usage_source(pytestconfig, tmp_path):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/starburst-trino-usage"
    )

    with patch(
        "datahub.ingestion.source.usage.starburst_trino_usage.TrinoUsageSource._get_trino_history"
    ) as mock_event_history:
        access_events = load_access_events(test_resources_dir)
        mock_event_history.return_value = access_events

        # Run ingestion
        pipeline = Pipeline.create(
            {
                "run_id": "test-trino-usage",
                "source": {
                    "type": "starburst-trino-usage",
                    "config": {
                        "host_port": "xxxxx",
                        "database": "testcatalog",
                        "username": "xxxxx",
                        "password": "xxxxx",
                        "audit_catalog": "test",
                        "audit_schema": "test",
                        "email_domain": "acryl.io",
                        "include_views": True,
                        "include_tables": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": f"{tmp_path}/trino_usages.json"},
                },
            },
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "trino_usages.json",
        golden_path=test_resources_dir / "trino_usages_golden.json",
    )


def load_access_events(test_resources_dir):
    access_events_history_file = test_resources_dir / "usage_events_history.json"
    with access_events_history_file.open() as access_events_json:
        access_events = json.loads(access_events_json.read())
    return access_events
