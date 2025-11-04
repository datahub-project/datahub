from unittest.mock import MagicMock, patch

import datahub.metadata.schema_classes as models
import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, QueryUrn

from datahub_integrations import __version__
from datahub_integrations.gen_ai.description_context import (
    EntityDescriptionResult,
    ExtractedTableInfo,
)
from datahub_integrations.gen_ai.router import suggest_description
from datahub_integrations.telemetry.ai_docs_events import (
    InferDocsApiRequestEvent,
    InferDocsApiResponseEvent,
)


@pytest.fixture
def mock_graph() -> DataHubGraph:
    mock_client = MagicMock()

    # Mock entity response for dataset
    mock_entity = {
        "schemaMetadata": models.SchemaMetadataClass(
            schemaName="test_db.test_schema",
            platform="snowflake",
            platformSchema=models.OtherSchemaClass(rawSchema=""),
            version=1,
            hash="1234567890",
            fields=[
                models.SchemaFieldClass(
                    fieldPath="id",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR",
                    description="Primary key for the table",
                    isPartOfKey=True,
                ),
            ],
        ),
        "datasetProperties": models.DatasetPropertiesClass(
            name="test_table",
            description="A test table",
        ),
    }

    def mock_get_entity_semityped(urn: str) -> dict:
        if "dataset" in urn:
            return mock_entity
        return {}

    mock_client.get_entity_semityped.side_effect = mock_get_entity_semityped
    return mock_client


@pytest.fixture
def mock_extracted_info() -> ExtractedTableInfo:
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
    return ExtractedTableInfo(
        urn=dataset_urn,
        table_name="test_table",
        table_description="A test table",
        column_names={"id": "id"},  # Changed to dict as required by validation
        column_descriptions={"id": "Primary key for the table"},
        column_upstream_lineages={},
        table_tags=[],
        table_glossary_terms=[],
        table_domains_info=[],
        table_owners_info=[],
        table_upstream_lineage_info=[],
        table_downstream_lineage_info=[],
    )


def test_suggest_description_tracks_query_event(mock_graph: DataHubGraph) -> None:
    # Test with a Query URN and no user_urn
    query_urn = "urn:li:query:test-query"
    expected_query_desc = "This is a test query description"

    with (
        patch("datahub_integrations.gen_ai.router.track_saas_event") as mock_track,
        patch(
            "datahub_integrations.gen_ai.router.generate_query_desc",
            return_value=expected_query_desc,
        ),
    ):
        # Call the function without user_urn
        result = suggest_description(mock_graph, query_urn)

        # Verify the result
        assert result.entity_description == expected_query_desc
        assert result.column_descriptions == {}

        # Verify track_saas_event was called with correct parameters for request
        mock_track.assert_called()
        request_call = mock_track.call_args_list[0][0][0]
        assert isinstance(request_call, InferDocsApiRequestEvent)
        assert request_call.entity_urn == query_urn
        assert request_call.entity_type == QueryUrn.ENTITY_TYPE
        assert request_call.user_urn is None

        # Verify response event was also tracked with correct data
        response_call = mock_track.call_args_list[1][0][0]
        assert isinstance(response_call, InferDocsApiResponseEvent)
        assert response_call.entity_urn == query_urn
        assert response_call.entity_type == QueryUrn.ENTITY_TYPE
        assert response_call.user_urn is None
        assert response_call.has_entity_description is True
        assert response_call.entity_description == expected_query_desc


def test_suggest_description_with_custom_user_urn(
    mock_graph: DataHubGraph, mock_extracted_info: ExtractedTableInfo
) -> None:
    # Test with a Dataset URN and custom user URN
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
    custom_user_urn = "urn:li:corpuser:custom-user"

    expected_result = EntityDescriptionResult(
        table_description="This is a test table description",
        column_descriptions={"id": "Primary key for the table"},
        failure_reason=None,
        extracted_entity_info=mock_extracted_info,
    )

    with (
        patch("datahub_integrations.gen_ai.router.track_saas_event") as mock_track,
        patch(
            "datahub_integrations.gen_ai.router.generate_entity_descriptions_for_urn",
            return_value=expected_result,
        ),
    ):
        # Call the function with custom user URN
        result = suggest_description(mock_graph, dataset_urn, user_urn=custom_user_urn)

        # Verify the result
        assert result.entity_description == expected_result.table_description
        assert result.column_descriptions == expected_result.column_descriptions

        # Verify track_saas_event was called with correct parameters for request
        mock_track.assert_called()
        request_call = mock_track.call_args_list[0][0][0]
        assert isinstance(request_call, InferDocsApiRequestEvent)
        assert request_call.entity_urn == dataset_urn
        assert request_call.entity_type == DatasetUrn.ENTITY_TYPE
        assert request_call.user_urn == custom_user_urn

        # Verify response event was also tracked with correct data
        response_call = mock_track.call_args_list[1][0][0]
        assert isinstance(response_call, InferDocsApiResponseEvent)
        assert response_call.entity_urn == dataset_urn
        assert response_call.entity_type == DatasetUrn.ENTITY_TYPE
        assert response_call.user_urn == custom_user_urn
        assert response_call.has_entity_description is True
        assert response_call.has_column_descriptions is True
        assert response_call.entity_description == expected_result.table_description


def test_suggest_description_tracks_dataset_event(
    mock_graph: DataHubGraph, mock_extracted_info: ExtractedTableInfo
) -> None:
    # Test with a Dataset URN
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"

    expected_result = EntityDescriptionResult(
        table_description="This is a test table description",
        column_descriptions={"id": "Primary key for the table"},
        failure_reason=None,
        extracted_entity_info=mock_extracted_info,
    )

    with (
        patch("datahub_integrations.gen_ai.router.track_saas_event") as mock_track,
        patch(
            "datahub_integrations.gen_ai.router.generate_entity_descriptions_for_urn",
            return_value=expected_result,
        ),
    ):
        # Call the function
        result = suggest_description(mock_graph, dataset_urn)

        # Verify the result
        assert result.entity_description == expected_result.table_description
        assert result.column_descriptions == expected_result.column_descriptions

        # Verify track_saas_event was called with correct parameters for request
        mock_track.assert_called()
        request_call = mock_track.call_args_list[0][0][0]
        assert isinstance(request_call, InferDocsApiRequestEvent)
        assert request_call.entity_urn == dataset_urn
        assert request_call.entity_type == DatasetUrn.ENTITY_TYPE
        assert request_call.user_urn is None

        # Verify response event was also tracked with correct data
        response_call = mock_track.call_args_list[1][0][0]
        assert isinstance(response_call, InferDocsApiResponseEvent)
        assert response_call.entity_urn == dataset_urn
        assert response_call.entity_type == DatasetUrn.ENTITY_TYPE
        assert response_call.user_urn is None
        assert response_call.has_entity_description is True
        assert response_call.has_column_descriptions is True
        assert response_call.entity_description == expected_result.table_description


def test_suggest_description_tracks_error_event(mock_graph: DataHubGraph) -> None:
    # Test with an invalid URN
    invalid_urn = "urn:li:invalid:test"
    custom_user_urn = "urn:li:corpuser:custom-user"

    with patch("datahub_integrations.gen_ai.router.track_saas_event") as mock_track:
        # Call the function and expect it to raise an error
        with pytest.raises(ValueError):
            suggest_description(mock_graph, invalid_urn, user_urn=custom_user_urn)

        # Verify only request event was tracked (no response event due to error)
        mock_track.assert_called_once()
        request_call = mock_track.call_args[0][0]
        assert isinstance(request_call, InferDocsApiRequestEvent)
        assert request_call.entity_urn == invalid_urn
        assert request_call.user_urn == custom_user_urn


def test_suggest_description_tracks_failed_generation(
    mock_graph: DataHubGraph, mock_extracted_info: ExtractedTableInfo
) -> None:
    # Test with a Dataset URN that fails to generate description
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
    custom_user_urn = "urn:li:corpuser:custom-user"

    expected_result = EntityDescriptionResult(
        table_description="",
        column_descriptions={},
        failure_reason="Failed to generate description",
        extracted_entity_info=mock_extracted_info,
    )

    with (
        patch("datahub_integrations.gen_ai.router.track_saas_event") as mock_track,
        patch(
            "datahub_integrations.gen_ai.router.generate_entity_descriptions_for_urn",
            return_value=expected_result,
        ),
    ):
        # Call the function
        result = suggest_description(mock_graph, dataset_urn, user_urn=custom_user_urn)

        # Verify the result
        assert result.entity_description == ""
        assert result.column_descriptions == {}

        # Verify track_saas_event was called with correct parameters for request
        mock_track.assert_called()
        request_call = mock_track.call_args_list[0][0][0]
        assert isinstance(request_call, InferDocsApiRequestEvent)
        assert request_call.entity_urn == dataset_urn
        assert request_call.entity_type == DatasetUrn.ENTITY_TYPE
        assert request_call.user_urn == custom_user_urn
        assert request_call.datahub_integrations_version == __version__

        # Verify response event was also tracked with correct data
        response_call = mock_track.call_args_list[1][0][0]
        assert isinstance(response_call, InferDocsApiResponseEvent)
        assert response_call.entity_urn == dataset_urn
        assert response_call.entity_type == DatasetUrn.ENTITY_TYPE
        assert response_call.user_urn == custom_user_urn
        assert response_call.has_entity_description is False
        assert response_call.has_column_descriptions is False
        assert response_call.error_msg == "Failed to generate description"
        assert response_call.datahub_integrations_version == __version__


def test_entity_description_result_column_metrics(
    mock_graph: DataHubGraph,
) -> None:
    """Test that num_columns and num_columns_with_description properties work correctly and are tracked in telemetry."""
    # Create an ExtractedTableInfo with 3 columns: 2 with descriptions, 1 without
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
    extracted_info = ExtractedTableInfo(
        urn=dataset_urn,
        table_name="test_table",
        table_description="A test table",
        column_names={
            "col1": "col1",
            "col2": "col2",
            "col3": "col3",
        },
    )

    # Test the properties directly
    result = EntityDescriptionResult(
        table_description="Test table",
        column_descriptions={
            "col1": "First column with description",
            "col2": "Second column with description",
        },
        extracted_entity_info=extracted_info,
        metadata_extraction_time_ms=123.45,
    )
    assert result.num_columns == 3
    assert (
        result.num_columns_with_description == 2
    )  # Only col1 and col2 have descriptions

    # Test that metrics are tracked in telemetry event
    with (
        patch("datahub_integrations.gen_ai.router.track_saas_event") as mock_track,
        patch(
            "datahub_integrations.gen_ai.router.generate_entity_descriptions_for_urn",
            return_value=result,
        ),
    ):
        suggest_description(mock_graph, dataset_urn)
        response_call = mock_track.call_args_list[1][0][0]
        assert response_call.num_columns == 3
        assert response_call.num_columns_with_description == 2
        assert response_call.metadata_extraction_time_ms == 123.45
