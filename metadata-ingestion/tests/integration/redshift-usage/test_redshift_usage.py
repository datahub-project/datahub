import json
import pathlib
from typing import Dict, List, cast
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.usage.redshift_usage import (
    RedshiftUsageConfig,
    RedshiftUsageSourceReport,
)
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-08-24 09:00:00"


def test_redshift_usage_config():
    config = RedshiftUsageConfig.parse_obj(
        dict(
            host_port="xxxxx",
            database="xxxxx",
            username="xxxxx",
            password="xxxxx",
            email_domain="xxxxx",
            include_views=True,
            include_tables=True,
        )
    )

    assert config.host_port == "xxxxx"
    assert config.database == "xxxxx"
    assert config.username == "xxxxx"
    assert config.email_domain == "xxxxx"
    assert config.include_views
    assert config.include_tables


def yield_function(li):
    for i in li:
        yield i


@freeze_time(FROZEN_TIME)
def test_redshift_usage_source(pytestconfig, tmp_path):

    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/redshift-usage"
    )

    with patch(
        "datahub.ingestion.source.usage.redshift_usage.Engine.execute"
    ) as mock_engine_execute:
        raw_access_events: List[Dict] = load_access_events(test_resources_dir)
        mock_engine_execute.return_value = raw_access_events

        # Run ingestion
        pipeline = Pipeline.create(
            {
                "run_id": "test-redshift-usage",
                "source": {
                    "type": "redshift-usage",
                    "config": {
                        "host_port": "xxxxx",
                        "database": "xxxxx",
                        "username": "xxxxx",
                        "password": "xxxxx",
                        "email_domain": "acryl.io",
                        "include_views": True,
                        "include_tables": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": f"{tmp_path}/redshift_usages.json"},
                },
            },
        )
        pipeline.run()
        pipeline.raise_from_status()

    # There should be 2 calls (usage aspects -1, operation aspects -1).
    assert mock_engine_execute.call_count == 2
    source_report: RedshiftUsageSourceReport = cast(
        RedshiftUsageSourceReport, pipeline.source.get_report()
    )
    assert source_report.num_usage_workunits_emitted == 3
    assert source_report.num_operational_stats_workunits_emitted == 3
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "redshift_usages.json",
        golden_path=test_resources_dir / "redshift_usages_golden.json",
    )


@freeze_time(FROZEN_TIME)
def test_redshift_usage_filtering(pytestconfig, tmp_path):

    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/redshift-usage"
    )

    with patch(
        "datahub.ingestion.source.usage.redshift_usage.Engine.execute"
    ) as mock_engine_execute:
        access_events = load_access_events(test_resources_dir)
        mock_engine_execute.return_value = access_events

        # Run ingestion
        pipeline = Pipeline.create(
            {
                "run_id": "test-redshift-usage",
                "source": {
                    "type": "redshift-usage",
                    "config": {
                        "host_port": "xxxxx",
                        "database": "xxxxx",
                        "username": "xxxxx",
                        "password": "xxxxx",
                        "email_domain": "acryl.io",
                        "include_views": True,
                        "include_tables": True,
                        "schema_pattern": {"allow": ["public"]},
                        "table_pattern": {"deny": [r"dev\.public\.orders"]},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": f"{tmp_path}/redshift_usages.json"},
                },
            },
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "redshift_usages.json",
        golden_path=test_resources_dir / "redshift_usages_filtered_golden.json",
    )


def load_access_events(test_resources_dir: pathlib.Path) -> List[Dict]:
    access_events_history_file = test_resources_dir / "usage_events_history.json"
    with access_events_history_file.open() as access_events_json:
        access_events = json.loads(access_events_json.read())
    return access_events
