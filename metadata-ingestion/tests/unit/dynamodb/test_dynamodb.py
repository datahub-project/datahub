import json
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dynamodb.dynamodb import (
    DynamoDBConfig,
    DynamoDBSource,
)


class TestDynamoDBTagsIngestion:
    """Test suite for DynamoDB tag extraction using PATCH operations."""

    @pytest.fixture
    def mock_context(self):
        """Fixture for mock pipeline context."""
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.pipeline_name = "test_pipeline"
        mock_ctx.run_id = "test_run"
        mock_ctx.graph = None
        return mock_ctx

    @pytest.fixture
    def config(self):
        """Fixture for DynamoDB source configuration."""
        return DynamoDBConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-west-2",
            extract_table_tags=True,
        )

    @pytest.fixture
    def source(self, mock_context, config):
        """Fixture for DynamoDB source instance."""
        return DynamoDBSource(ctx=mock_context, config=config, platform="dynamodb")

    @staticmethod
    def extract_tag_urns(workunits):
        """Extract tag URNs from PATCH workunits."""
        tag_urns = set()
        for wu in workunits:
            mcp = wu.metadata
            if mcp.aspectName == "globalTags" and mcp.changeType == "PATCH":
                patch_ops = json.loads(mcp.aspect.value.decode("utf-8"))
                for op in patch_ops:
                    if op["op"] == "add" and "/tags/" in op["path"]:
                        tag_urns.add(op["value"]["tag"])
        return tag_urns

    def test_basic_tag_extraction(self, source):
        """Test basic AWS tag extraction as PATCH operations."""
        aws_tags = [
            {"Key": "env", "Value": "prod"},
            {"Key": "team", "Value": "data"},
        ]

        with patch.object(source, "_get_dynamodb_table_tags", return_value=aws_tags):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=MagicMock(),
                    table_arn="arn:aws:dynamodb:us-west-2:123456789012:table/test",
                    dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,us-west-2.test,PROD)",
                )
            )

        assert len(workunits) == 1
        mcp = workunits[0].metadata
        assert mcp.changeType == "PATCH"
        assert mcp.aspectName == "globalTags"

        tag_urns = self.extract_tag_urns(workunits)
        assert tag_urns == {"urn:li:tag:env:prod", "urn:li:tag:team:data"}

    @pytest.mark.parametrize(
        "aws_tags,expected_urns",
        [
            # Special characters and complex values
            (
                [
                    {"Key": "app-name", "Value": "my-app_v2.0"},
                    {"Key": "owner@domain", "Value": "user+admin@example.com"},
                    {"Key": "env:prod", "Value": "app/test-123"},
                ],
                {
                    "urn:li:tag:app-name:my-app_v2.0",
                    "urn:li:tag:owner@domain:user+admin@example.com",
                    "urn:li:tag:env:prod:app/test-123",
                },
            ),
            # Tags without values
            (
                [
                    {"Key": "production"},
                    {"Key": "critical", "Value": ""},
                ],
                {"urn:li:tag:production", "urn:li:tag:critical"},
            ),
            # Empty list
            ([], set()),
        ],
    )
    def test_tag_format_variations(self, source, aws_tags, expected_urns):
        """Test various tag formats including special characters and empty values."""
        with patch.object(source, "_get_dynamodb_table_tags", return_value=aws_tags):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=MagicMock(),
                    table_arn="arn:aws:dynamodb:us-west-2:123456789012:table/test",
                    dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,us-west-2.test,PROD)",
                )
            )

        tag_urns = self.extract_tag_urns(workunits)
        assert tag_urns == expected_urns

    def test_malformed_tags_skipped(self, source):
        """Test that tags without Key field are skipped with warnings."""
        malformed_tags = [
            {"Value": "orphaned-value"},
            {},
            {"Key": "valid", "Value": "tag"},
        ]

        with patch.object(
            source, "_get_dynamodb_table_tags", return_value=malformed_tags
        ):
            workunits = list(
                source._get_dynamodb_table_tags_wu(
                    dynamodb_client=MagicMock(),
                    table_arn="arn:aws:dynamodb:us-west-2:123456789012:table/test",
                    dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,us-west-2.test,PROD)",
                )
            )

        # Only the valid tag should be extracted
        tag_urns = self.extract_tag_urns(workunits)
        assert tag_urns == {"urn:li:tag:valid:tag"}

        # Verify warnings were logged for malformed tags
        assert len(source.report.warnings) >= 1
        assert any(
            "Skipping tag entry without 'Key' field" in str(w)
            for w in source.report.warnings
        )
