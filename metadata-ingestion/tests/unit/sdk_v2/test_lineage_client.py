import pathlib
from typing import Dict, List
from unittest.mock import MagicMock

from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

_GOLDEN_DIR = pathlib.Path(__file__).parent / "lineage_client_golden"
_GOLDEN_DIR.mkdir(exist_ok=True)


def test_add_dataset_transform_lineage_basic() -> None:
    """Test basic case: upstream to downstream with no column lineage or query text."""
    # Create mock client with mock graph
    mock_graph = MagicMock()
    mock_entities = MagicMock()
    mock_client = MagicMock(spec=DataHubClient)
    mock_client._graph = mock_graph
    mock_client.entities = mock_entities

    # Create the lineage client with our mock
    lineage_client = LineageClient(client=mock_client)

    # Define test parameters
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    # Call the method under test
    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
    )

    # Verify the correct calls were made
    mock_entities.update.assert_called_once()
    mock_graph.emit_mcps.assert_not_called()  # No query entity should be emitted


def test_add_dataset_transform_lineage_with_query() -> None:
    """Test with query text included."""
    # Create mock client with mock graph
    mock_graph = MagicMock()
    mock_entities = MagicMock()
    mock_client = MagicMock(spec=DataHubClient)
    mock_client._graph = mock_graph
    mock_client.entities = mock_entities

    # Create the lineage client with our mock
    lineage_client = LineageClient(client=mock_client)

    # Define test parameters
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    query_text = "SELECT * FROM upstream_table"

    # Call the method under test
    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
    )

    # Verify the correct calls were made
    mock_entities.update.assert_called_once()
    mock_graph.emit_mcps.assert_called_once()  # Query entity should be emitted


def test_add_dataset_transform_lineage_with_column_lineage() -> None:
    """Test with column lineage mapping."""
    # Create mock client with mock graph
    mock_graph = MagicMock()
    mock_entities = MagicMock()
    mock_client = MagicMock(spec=DataHubClient)
    mock_client._graph = mock_graph
    mock_client.entities = mock_entities

    # Create the lineage client with our mock
    lineage_client = LineageClient(client=mock_client)

    # Define test parameters
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    column_lineage: Dict[str, List[str]] = {
        "ds_col1": ["us_col1"],  # Simple 1:1 mapping
        "ds_col2": ["us_col2", "us_col3"],  # 2:1 mapping
    }

    # Call the method under test
    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        column_lineage=column_lineage,
    )

    # Verify the correct calls were made
    mock_entities.update.assert_called_once()
    mock_graph.emit_mcps.assert_not_called()  # No query entity should be emitted


def test_add_dataset_transform_lineage_complete() -> None:
    """Test with both query text and column lineage mapping."""
    # Create mock client with mock graph
    mock_graph = MagicMock()
    mock_entities = MagicMock()
    mock_client = MagicMock(spec=DataHubClient)
    mock_client._graph = mock_graph
    mock_client.entities = mock_entities

    # Create the lineage client with our mock
    lineage_client = LineageClient(client=mock_client)

    # Define test parameters
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    query_text = (
        "SELECT us_col1 as ds_col1, us_col2 + us_col3 as ds_col2 FROM upstream_table"
    )
    column_lineage: Dict[str, List[str]] = {
        "ds_col1": ["us_col1"],  # Simple 1:1 mapping
        "ds_col2": ["us_col2", "us_col3"],  # 2:1 mapping
    }

    # Call the method under test
    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
        column_lineage=column_lineage,
    )

    # Verify the correct calls were made
    mock_entities.update.assert_called_once()
    mock_graph.emit_mcps.assert_called_once()  # Query entity should be emitted

    # Since we're using mocks, we can't easily test the internal details here
    # We're just verifying the correct calls are made to update and emit_mcps


# This test needs to run with the --update-golden-files flag to create the golden file first
# We're commenting it out until that's done
# def test_add_dataset_transform_lineage_goldens() -> None:
#     """Test that generates golden files for validation."""
#     from tests.test_helpers import mce_helpers
#
#     def capture_mcps(*args, **kwargs):
#         # This function will be called instead of the real emit_mcps
#         return args[0]  # Return the MCPs that would have been emitted
#
#     # Create mock client
#     mock_graph = MagicMock()
#     mock_entities = MagicMock()
#     mock_client = MagicMock(spec=DataHubClient)
#     mock_client._graph = mock_graph
#     mock_client.entities = mock_entities
#
#     # Make _graph.emit_mcps capture the MCPs
#     mock_graph.emit_mcps.side_effect = capture_mcps
#
#     # Mock the update method to return the DatasetPatchBuilder
#     def capture_patch_builder(patch_builder):
#         return patch_builder
#     mock_entities.update.side_effect = capture_patch_builder
#
#     lineage_client = LineageClient(client=mock_client)
#
#     # Define test parameters for a complete test
#     upstream = DatasetUrn("snowflake", "upstream_table", "PROD")
#     downstream = DatasetUrn("snowflake", "downstream_table", "PROD")
#     query_text = "SELECT us_col1 as ds_col1, us_col2 + us_col3 as ds_col2 FROM upstream_table"
#     column_lineage: Dict[str, List[str]] = {
#         "ds_col1": ["us_col1"],
#         "ds_col2": ["us_col2", "us_col3"],
#     }
#
#     # Call the method under test
#     lineage_client.add_dataset_transform_lineage(
#         upstream=upstream,
#         downstream=downstream,
#         query_text=query_text,
#         column_lineage=column_lineage,
#     )
#
#     # Get the DatasetPatchBuilder and extract the MCPs
#     patch_builder = mock_entities.update.return_value
#
#     # Get the query entity MCPs
#     query_mcps = mock_graph.emit_mcps.return_value
#
#     # Combine both for golden file comparison
#     all_mcps = []
#
#     # Add patch builder MCPs if available
#     if hasattr(patch_builder, "build"):
#         all_mcps.extend(patch_builder.build())
#
#     # Add query MCPs if available
#     if query_mcps:
#         all_mcps.extend(query_mcps)
#
#     # Check using golden files
#     golden_file = _GOLDEN_DIR / "lineage_complete_golden.json"
#     mce_helpers.check_goldens_stream(
#         outputs=all_mcps,
#         golden_path=golden_file,
#         ignore_order=False,
#     )


def test_add_dataset_transform_lineage_empty_column_mapping() -> None:
    """Test with an empty column lineage mapping."""
    # Create mock client with mock graph
    mock_graph = MagicMock()
    mock_entities = MagicMock()
    mock_client = MagicMock(spec=DataHubClient)
    mock_client._graph = mock_graph
    mock_client.entities = mock_entities

    # Create the lineage client with our mock
    lineage_client = LineageClient(client=mock_client)

    # Define test parameters
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    column_lineage: Dict[str, List[str]] = {}  # Empty column lineage

    # Call the method under test
    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        column_lineage=column_lineage,
    )

    # Verify the correct calls were made - should still work with empty lineage
    mock_entities.update.assert_called_once()


def test_add_dataset_transform_lineage_string_urns() -> None:
    """Test with string URNs instead of URN objects."""
    # Create mock client with mock graph
    mock_graph = MagicMock()
    mock_entities = MagicMock()
    mock_client = MagicMock(spec=DataHubClient)
    mock_client._graph = mock_graph
    mock_client.entities = mock_entities

    # Create the lineage client with our mock
    lineage_client = LineageClient(client=mock_client)

    # Define test parameters as strings
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    query_text = "SELECT * FROM upstream_table"

    # Call the method under test with string URNs
    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
    )

    # Verify the method works with string URNs
    mock_entities.update.assert_called_once()
    mock_graph.emit_mcps.assert_called_once()  # Query entity should be emitted
