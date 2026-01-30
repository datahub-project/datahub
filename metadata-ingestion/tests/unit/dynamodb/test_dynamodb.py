from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dynamodb.dynamodb import (
    DynamoDBConfig,
    DynamoDBSource,
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
        """Helper method to extract tag URNs from workunits."""
        if len(workunits) == 0:
            return set()
        assert len(workunits) == 1
        wu = workunits[0]
        assert wu.metadata.aspect is not None
        tags_aspect = wu.metadata.aspect
        assert isinstance(tags_aspect, GlobalTagsClass)
        return {tag.tag for tag in tags_aspect.tags}

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
        }

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
        assert len(tag_urns) == len(expected_urns)
        assert set(tag_urns) == set(expected_urns)
