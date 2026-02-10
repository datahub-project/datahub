from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dynamodb.dynamodb import (
    DynamoDBConfig,
    DynamoDBSource,
    PAGE_SIZE,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
)


class TestDynamoDBTagsIngestion:
    """Test suite for DynamoDB tag extraction"""

    # Fixtures
    @pytest.fixture
    def mock_dynamodb_client(self):
        """Fixture for mock DynamoDB client."""
        return MagicMock()

    @pytest.fixture
    def dynamodb_config(self):
        """Fixture for DynamoDB source configuration."""
        return DynamoDBConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-west-2",
            extract_table_tags=True,
        )

    @pytest.fixture
    def mock_context(self):
        """Fixture for mock pipeline context."""
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.pipeline_name = "test_pipeline"
        mock_ctx.run_id = "test_run"
        mock_ctx.graph = None
        return mock_ctx

    @pytest.fixture
    def aws_tags(self):
        """Fixture for AWS DynamoDB tags."""
        return [
            {"Key": "env", "Value": "prod"},
            {"Key": "team", "Value": "data"},
        ]

    @pytest.fixture
    def dataset_info(self):
        """Fixture for dataset URN and table ARN."""
        return {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:dynamodb,us-west-2.test_table,PROD)",
            "table_arn": "arn:aws:dynamodb:us-west-2:123456789012:table/test_table",
        }

    @staticmethod
    def create_dynamodb_source(ctx, config):
        """Helper method to create DynamoDB source instance."""
        return DynamoDBSource(ctx=ctx, config=config, platform="dynamodb")

    @staticmethod
    def get_tag_urns_from_workunits(workunits):
        """Helper method to extract tag URNs from workunits.

        Returns the set of tag URNs from the GlobalTags aspect.
        Returns empty set if no workunits or if tags aspect is not present.
        """
        if len(workunits) == 0:
            return set()

        wu = workunits[0]
        tags_aspect = wu.metadata.aspect

        if isinstance(tags_aspect, GlobalTagsClass):
            return {tag.tag for tag in tags_aspect.tags}

        return set()

    def test_tags_extraction_from_aws(
        self,
        mock_dynamodb_client,
        mock_context,
        dynamodb_config,
        aws_tags,
        dataset_info,
    ):
        """Test that DynamoDB tags are extracted from AWS and emitted directly."""
        source = self.create_dynamodb_source(mock_context, dynamodb_config)

        with patch.object(source, "_get_dynamodb_table_tags", return_value=aws_tags):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        assert tag_urns == {
            "urn:li:tag:env:prod",
            "urn:li:tag:team:data",
        }, "Tag URNs should match expected AWS tags"

    def test_tags_extraction_error_handling(
        self,
        mock_dynamodb_client,
        mock_context,
        dynamodb_config,
        dataset_info,
    ):
        """Test that errors during tag extraction are handled gracefully.

        When AWS tag fetching fails, no workunits are emitted and the error is logged as a warning.
        """
        source = self.create_dynamodb_source(mock_context, dynamodb_config)

        with patch.object(
            source, "_get_dynamodb_table_tags", side_effect=Exception("AWS API error")
        ):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        # Should not emit any workunits when fetch fails
        assert len(workunits) == 0

        # Verify warning was logged
        assert len(source.report.warnings) >= 1
        assert any(
            "Failed to extract tags for table" in str(w) for w in source.report.warnings
        )

    @pytest.mark.parametrize(
        "tags_input,expected_urns",
        [
            # Special characters, empty/None values
            (
                [
                    {"Key": "app-name", "Value": "my-app_v2.0"},
                    {"Key": "owner@domain", "Value": "user+admin@example.com"},
                    {"Key": "complex-tag@env:prod", "Value": "app_v2.0+beta/test-123"},
                    {"Key": "production"},  # No Value key
                    {"Key": "critical", "Value": ""},  # Empty value
                ],
                [
                    "urn:li:tag:app-name:my-app_v2.0",
                    "urn:li:tag:owner@domain:user+admin@example.com",
                    "urn:li:tag:complex-tag@env:prod:app_v2.0+beta/test-123",
                    "urn:li:tag:production",
                    "urn:li:tag:critical",
                ],
            ),
            ([], []),
        ],
    )
    def test_tag_format_variations(
        self,
        mock_dynamodb_client,
        mock_context,
        dynamodb_config,
        dataset_info,
        tags_input,
        expected_urns,
    ):
        """Test tag extraction with various input formats: special chars, empty values, empty list."""
        source = self.create_dynamodb_source(mock_context, dynamodb_config)

        with patch.object(source, "_get_dynamodb_table_tags", return_value=tags_input):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        assert len(tag_urns) == len(expected_urns), (
            f"Expected {len(expected_urns)} tags, got {len(tag_urns)}"
        )
        assert set(tag_urns) == set(expected_urns), (
            "Tag URNs should match expected tags"
        )


class TestDynamoDBSchemaSampling:
    """Test suite for DynamoDB schema sampling configuration"""

    @pytest.fixture
    def mock_context(self):
        """Fixture for mock pipeline context."""
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.pipeline_name = "test_pipeline"
        mock_ctx.run_id = "test_run"
        mock_ctx.graph = None
        return mock_ctx

    def test_default_schema_sampling_size(self, mock_context):
        """Test that the default schema_sampling_size is 100."""
        config = DynamoDBConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-west-2",
        )
        assert config.schema_sampling_size == 100

    def test_custom_schema_sampling_size(self, mock_context):
        """Test that custom schema_sampling_size is properly set."""
        config = DynamoDBConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-west-2",
            schema_sampling_size=500,
        )
        assert config.schema_sampling_size == 500

    def test_schema_sampling_size_used_in_pagination(self, mock_context):
        """Test that schema_sampling_size is used in pagination config."""
        config = DynamoDBConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-west-2",
            schema_sampling_size=250,
        )
        source = DynamoDBSource(ctx=mock_context, config=config, platform="dynamodb")

        # Mock the dynamodb client and paginator
        mock_dynamodb_client = MagicMock()
        mock_paginator = MagicMock()
        mock_dynamodb_client.get_paginator.return_value = mock_paginator

        # Mock the paginate response
        mock_page = {"Items": [{"field1": {"S": "value1"}}]}
        mock_paginator.paginate.return_value = [mock_page]

        # Mock include_table_item_to_schema to do nothing
        with patch.object(source, "include_table_item_to_schema"):
            with patch.object(source, "construct_schema_from_items"):
                source._get_sample_values_for_table(
                    mock_dynamodb_client, "us-west-2", "test_table"
                )

        # Verify paginate was called with the correct MaxItems
        mock_paginator.paginate.assert_called_once()
        call_args = mock_paginator.paginate.call_args
        pagination_config = call_args[1]["PaginationConfig"]

        assert pagination_config["MaxItems"] == 250
        assert pagination_config["PageSize"] == PAGE_SIZE
