import json
import pathlib
from typing import Dict, List
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.usage.redshift_usage import (
    RedshiftAccessEvent,
    RedshiftUsageConfig,
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
        "datahub.ingestion.source.usage.redshift_usage.RedshiftUsageSource._gen_access_events_from_history_query"
    ) as mock_event_history:
        # The raw_access_events mimics the result of query execution
        raw_access_events: List[Dict] = load_access_events(test_resources_dir)
        # The redshift_access_events is the response of each call to _gen_access_events_from_history_query.
        redshift_access_events: List[RedshiftAccessEvent] = []
        for ae in raw_access_events:
            try:
                redshift_access_events.append(RedshiftAccessEvent(**ae))
            except Exception:
                pass
        # Return an iterator to redshift_access_events for each call to _gen_access_events_from_history_query
        # via the side_effects.
        mock_event_history.side_effect = lambda *args, **kwdargs: iter(
            redshift_access_events
        )

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

    # There should be 3 calls (usage aspects -1, insert operation aspects -1, delete operation aspects - 1).
    assert mock_event_history.call_count == 3
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "redshift_usages.json",
        golden_path=test_resources_dir / "redshift_usages_golden.json",
    )


def load_access_events(test_resources_dir: pathlib.Path) -> List[Dict]:
    access_events_history_file = test_resources_dir / "usage_events_history.json"
    with access_events_history_file.open() as access_events_json:
        access_events = json.loads(access_events_json.read())
    return access_events
