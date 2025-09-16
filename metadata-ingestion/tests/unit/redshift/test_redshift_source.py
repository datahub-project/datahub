from typing import Iterable
from unittest.mock import MagicMock, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftDataDictionary,
    RedshiftTable,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
)


def redshift_source_setup(custom_props_flag: bool) -> Iterable[MetadataWorkUnit]:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        patch_custom_properties=custom_props_flag,
    )
    source: RedshiftSource = RedshiftSource(config, ctx=PipelineContext(run_id="test"))
    gen = source.gen_dataset_workunits(
        table=RedshiftTable(
            name="category",
            columns=[],
            created=None,
            comment="",
        ),
        database="dev",
        schema="public",
        sub_type="test_sub_type",
        custom_properties={"my_key": "my_value"},
    )
    return gen


def test_gen_dataset_workunits_patch_custom_properties_patch():
    gen = redshift_source_setup(True)
    custom_props_exist = False
    for item in gen:
        mcp = item.metadata
        assert not isinstance(mcp, MetadataChangeEventClass)
        if mcp.aspectName == "datasetProperties":
            assert isinstance(mcp, MetadataChangeProposalClass)
            assert mcp.changeType == "PATCH"
            custom_props_exist = True
        else:
            assert isinstance(mcp, MetadataChangeProposalWrapper)

    assert custom_props_exist


def test_gen_dataset_workunits_patch_custom_properties_upsert():
    gen = redshift_source_setup(False)
    custom_props_exist = False
    for item in gen:
        assert isinstance(item.metadata, MetadataChangeProposalWrapper)
        mcp = item.metadata
        if mcp.aspectName == "datasetProperties":
            assert mcp.changeType == "UPSERT"
            custom_props_exist = True

    assert custom_props_exist


def test_has_shared_tables_in_regular_schemas():
    """Test the shared table detection logic"""
    data_dict = RedshiftDataDictionary(is_serverless=False)

    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Test case 1: Has shared tables
    mock_cursor.fetchone.return_value = (5,)  # 5 shared tables
    with patch.object(RedshiftDataDictionary, "get_query_result") as mock_query:
        mock_query.return_value = mock_cursor
        result = data_dict._has_shared_tables_in_regular_schemas(mock_conn, "test_db")
        assert result is True

        # Verify the correct query was called
        expected_query = """
            SELECT COUNT(*) as shared_count
            FROM svv_table_info 
            WHERE database = 'test_db' 
              AND share_name IS NOT NULL
            """
        mock_query.assert_called_once()
        actual_query = mock_query.call_args[0][1].strip()
        expected_query = expected_query.strip()
        assert actual_query == expected_query

    # Test case 2: No shared tables
    mock_cursor.fetchone.return_value = (0,)  # 0 shared tables
    with patch.object(RedshiftDataDictionary, "get_query_result") as mock_query:
        mock_query.return_value = mock_cursor
        result = data_dict._has_shared_tables_in_regular_schemas(mock_conn, "test_db")
        assert result is False

    # Test case 3: Query fails gracefully
    with patch.object(RedshiftDataDictionary, "get_query_result") as mock_query:
        mock_query.side_effect = Exception("Connection error")
        result = data_dict._has_shared_tables_in_regular_schemas(mock_conn, "test_db")
        assert result is False  # Should return False on error
