from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dynamodb.dynamodb import (
    DynamoDBConfig,
    DynamoDBSource,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)


class TestDynamoDBTagsIngestion:
    """Test suite for DynamoDB tag extraction and merging."""

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
        """Fixture for mock pipeline context with graph."""
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.pipeline_name = "test_pipeline"
        mock_ctx.run_id = "test_run"
        mock_ctx.graph = MagicMock()
        return mock_ctx

    @pytest.fixture
    def mock_context_without_graph(self):
        """Fixture for mock pipeline context without graph."""
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.pipeline_name = "test_pipeline"
        mock_ctx.run_id = "test_run"
        mock_ctx.graph = None
        return mock_ctx

    @pytest.fixture
    def existing_tags(self):
        """Fixture for existing DataHub tags."""
        return GlobalTagsClass(
            tags=[
                TagAssociationClass(tag="urn:li:tag:manual_tag"),
                TagAssociationClass(tag="urn:li:tag:ui_added_tag"),
            ]
        )

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

    def test_tags_merge_with_existing(
        self,
        mock_dynamodb_client,
        mock_context,
        dynamodb_config,
        existing_tags,
        aws_tags,
        dataset_info,
    ):
        """Test that DynamoDB tags are merged with existing DataHub tags when graph is available."""
        mock_context.graph.get_aspect.return_value = existing_tags

        source = self.create_dynamodb_source(mock_context, dynamodb_config)

        with patch.object(source, "_get_dynamodb_table_tags", return_value=aws_tags):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        mock_context.graph.get_aspect.assert_called_once_with(
            entity_urn=dataset_info["dataset_urn"], aspect_type=GlobalTagsClass
        )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        assert tag_urns == {
            "urn:li:tag:env:prod",
            "urn:li:tag:team:data",
            "urn:li:tag:manual_tag",
            "urn:li:tag:ui_added_tag",
        }

    def test_tags_without_graph(
        self,
        mock_dynamodb_client,
        mock_context_without_graph,
        dynamodb_config,
        aws_tags,
        dataset_info,
    ):
        """Test that DynamoDB tags work when graph is not available (no merging)."""
        source = self.create_dynamodb_source(
            mock_context_without_graph, dynamodb_config
        )

        with patch.object(source, "_get_dynamodb_table_tags", return_value=aws_tags):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        assert tag_urns == {"urn:li:tag:env:prod", "urn:li:tag:team:data"}

    def test_tags_merge_error_handling(
        self,
        mock_dynamodb_client,
        mock_context,
        dynamodb_config,
        dataset_info,
    ):
        """Test that errors during tag merging are handled gracefully."""
        mock_context.graph.get_aspect.side_effect = Exception("Graph API error")

        source = self.create_dynamodb_source(mock_context, dynamodb_config)

        error_case_tags = [{"Key": "env", "Value": "prod"}]

        with patch.object(
            source, "_get_dynamodb_table_tags", return_value=error_case_tags
        ):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        assert tag_urns == {"urn:li:tag:env:prod"}

        assert len(source.report.warnings) > 0
        assert any(
            "Failed to merge existing tags" in str(w) for w in source.report.warnings
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
        mock_context_without_graph,
        dynamodb_config,
        dataset_info,
        tags_input,
        expected_urns,
    ):
        """Test tag extraction with various input formats: special chars, empty values, empty list."""
        source = self.create_dynamodb_source(
            mock_context_without_graph, dynamodb_config
        )

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

    def test_tags_with_missing_key_field(
        self,
        mock_dynamodb_client,
        mock_context_without_graph,
        dynamodb_config,
        dataset_info,
    ):
        """Test that tags missing the Key field are skipped with warning."""
        source = self.create_dynamodb_source(
            mock_context_without_graph, dynamodb_config
        )

        malformed_tags = [
            {"Value": "orphaned-value"},  # Missing Key
            {},  # Empty dict
        ]

        with patch.object(
            source, "_get_dynamodb_table_tags", return_value=malformed_tags
        ):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        assert len(tag_urns) == 0

        # Verify warning was logged
        assert len(source.report.warnings) >= 1
        assert any(
            "Skipping tag entry without 'Key' field" in str(w)
            for w in source.report.warnings
        )

    def test_tags_deduplication(
        self,
        mock_dynamodb_client,
        mock_context_without_graph,
        dynamodb_config,
        existing_tags,
        dataset_info,
    ):
        """Test that duplicate tags from AWS and existing tags are deduplicated."""
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.pipeline_name = "test_pipeline"
        mock_ctx.run_id = "test_run"
        mock_ctx.graph = MagicMock()

        aws_tags = [
            {"Key": "env", "Value": "prod"},
            {"Key": "team", "Value": "data"},
        ]

        existing_duplicate = GlobalTagsClass(
            tags=[
                TagAssociationClass(tag="urn:li:tag:env:prod"),  # Duplicate of AWS tag
                TagAssociationClass(tag="urn:li:tag:manual_tag"),
            ]
        )
        mock_ctx.graph.get_aspect.return_value = existing_duplicate

        source = self.create_dynamodb_source(mock_ctx, dynamodb_config)

        with patch.object(source, "_get_dynamodb_table_tags", return_value=aws_tags):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=mock_dynamodb_client,
                    table_arn=dataset_info["table_arn"],
                    dataset_urn=dataset_info["dataset_urn"],
                )
            )

        tag_urns = self.get_tag_urns_from_workunits(workunits)
        # Should have no duplicates (3 unique tags, not 4)
        assert len(tag_urns) == 3
        assert "urn:li:tag:env:prod" in tag_urns
        assert "urn:li:tag:team:data" in tag_urns
        assert "urn:li:tag:manual_tag" in tag_urns
