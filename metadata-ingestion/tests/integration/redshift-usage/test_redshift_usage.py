import json
import pathlib
from pathlib import Path
from typing import Dict, List, Union
from unittest.mock import Mock, patch

from freezegun import freeze_time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.redshift.usage import RedshiftUsageExtractor
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-08-24 09:00:00"


def test_redshift_usage_config():
    config = RedshiftConfig.parse_obj(
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


@freeze_time(FROZEN_TIME)
@patch("redshift_connector.Cursor")
@patch("redshift_connector.Connection")
def test_redshift_usage_source(mock_cursor, mock_connection, pytestconfig, tmp_path):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/redshift-usage"
    )
    generate_mcps_path = Path(f"{tmp_path}/redshift_usages.json")
    mock_usage_query_result = open(f"{test_resources_dir}/usage_events_history.json")
    mock_operational_query_result = open(
        f"{test_resources_dir}/operational_events_history.json"
    )
    mock_usage_query_result_dict = json.load(mock_usage_query_result)
    mock_operational_query_result_dict = json.load(mock_operational_query_result)

    mock_cursor.execute.return_value = None
    mock_connection.cursor.return_value = mock_cursor

    mock_cursor_usage_query = Mock()
    mock_cursor_operational_query = Mock()
    mock_cursor_usage_query.execute.return_value = None
    mock_cursor_operational_query.execute.return_value = None
    mock_cursor_usage_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_usage_query_result_dict],
        [],
    ]
    mock_cursor_operational_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_operational_query_result_dict],
        [],
    ]

    mock_cursor_usage_query.description = [
        [key] for key in mock_usage_query_result_dict[0].keys()
    ]
    mock_cursor_operational_query.description = [
        [key] for key in mock_operational_query_result_dict[0].keys()
    ]

    mock_connection.cursor.side_effect = [
        mock_cursor_operational_query,
        mock_cursor_usage_query,
    ]

    config = RedshiftConfig(host_port="test:1234", email_domain="acryl.io")
    source_report = RedshiftReport()
    usage_extractor = RedshiftUsageExtractor(
        config=config,
        connection=mock_connection,
        report=source_report,
    )

    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        "db1": {
            "schema1": [
                RedshiftTable(
                    name="category",
                    schema="schema1",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
            ]
        },
        "dev": {
            "public": [
                RedshiftTable(
                    name="users",
                    schema="public",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
                RedshiftTable(
                    name="orders",
                    schema="public",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
            ]
        },
    }
    mwus = usage_extractor.generate_usage(all_tables=all_tables)
    metadata: List[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = []

    for mwu in mwus:
        metadata.append(mwu.metadata)

    # There should be 2 calls (usage aspects -1, operation aspects -1).
    assert mock_connection.cursor.call_count == 2
    assert source_report.num_usage_workunits_emitted == 3
    assert source_report.num_operational_stats_workunits_emitted == 3

    write_metadata_file(generate_mcps_path, metadata)
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "redshift_usages.json",
        golden_path=test_resources_dir / "redshift_usages_golden.json",
    )


@freeze_time(FROZEN_TIME)
@patch("redshift_connector.Cursor")
@patch("redshift_connector.Connection")
def test_redshift_usage_filtering(mock_cursor, mock_connection, pytestconfig, tmp_path):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/redshift-usage"
    )
    generate_mcps_path = Path(f"{tmp_path}/redshift_usages.json")
    mock_usage_query_result = open(f"{test_resources_dir}/usage_events_history.json")
    mock_operational_query_result = open(
        f"{test_resources_dir}/operational_events_history.json"
    )
    mock_usage_query_result_dict = json.load(mock_usage_query_result)
    mock_operational_query_result_dict = json.load(mock_operational_query_result)

    mock_cursor.execute.return_value = None
    mock_connection.cursor.return_value = mock_cursor

    mock_cursor_usage_query = Mock()
    mock_cursor_operational_query = Mock()
    mock_cursor_usage_query.execute.return_value = None
    mock_cursor_operational_query.execute.return_value = None
    mock_cursor_usage_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_usage_query_result_dict],
        [],
    ]
    mock_cursor_operational_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_operational_query_result_dict],
        [],
    ]

    mock_cursor_usage_query.description = [
        [key] for key in mock_usage_query_result_dict[0].keys()
    ]
    mock_cursor_operational_query.description = [
        [key] for key in mock_operational_query_result_dict[0].keys()
    ]

    mock_connection.cursor.side_effect = [
        mock_cursor_operational_query,
        mock_cursor_usage_query,
    ]

    config = RedshiftConfig(host_port="test:1234", email_domain="acryl.io")
    usage_extractor = RedshiftUsageExtractor(
        config=config,
        connection=mock_connection,
        report=RedshiftReport(),
    )

    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        "dev": {
            "public": [
                RedshiftTable(
                    name="users",
                    schema="public",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
            ]
        },
    }
    mwus = usage_extractor.generate_usage(all_tables=all_tables)
    metadata: List[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = []

    for mwu in mwus:
        metadata.append(mwu.metadata)

    write_metadata_file(generate_mcps_path, metadata)
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
