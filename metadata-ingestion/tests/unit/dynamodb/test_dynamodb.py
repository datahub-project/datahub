from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dynamodb.dynamodb import (
    PAGE_SIZE,
    DynamoDBConfig,
    DynamoDBSource,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    UpstreamLineageClass,
)


@pytest.fixture
def mock_context():
    """Fixture for mock pipeline context."""
    mock_ctx = MagicMock(spec=PipelineContext)
    mock_ctx.pipeline_name = "test_pipeline"
    mock_ctx.run_id = "test_run"
    mock_ctx.graph = None
    return mock_ctx


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

        source.construct_schema_from_dynamodb(
            mock_dynamodb_client, "us-west-2", "test_table"
        )

        # Verify paginate was called with the correct MaxItems
        mock_paginator.paginate.assert_called_once()
        call_args = mock_paginator.paginate.call_args
        pagination_config = call_args[1]["PaginationConfig"]

        assert pagination_config["MaxItems"] == 250
        assert pagination_config["PageSize"] == PAGE_SIZE


class TestDynamoDBS3ExportLineage:
    """Test suite for DynamoDB Export to S3 lineage discovery."""

    @pytest.fixture
    def lineage_config(self):
        return DynamoDBConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="us-west-2",
            include_s3_export_lineage=True,
            env="PROD",
        )

    def test_build_export_s3_uri(self):
        assert (
            DynamoDBSource._build_export_s3_uri("my-bucket", None) == "s3://my-bucket"
        )
        assert DynamoDBSource._build_export_s3_uri("my-bucket", "") == "s3://my-bucket"
        assert DynamoDBSource._build_export_s3_uri("my-bucket", "/") == "s3://my-bucket"
        assert (
            DynamoDBSource._build_export_s3_uri("my-bucket", "exports/daily/")
            == "s3://my-bucket/exports/daily"
        )

    def test_collect_and_emit_s3_export_lineage(self, mock_context, lineage_config):
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        table_arn = "arn:aws:dynamodb:us-west-2:123456789012:table/orders"
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dynamodb,123456789012.us-west-2.orders,PROD)"

        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {
                    "ExportArn": "arn:aws:dynamodb:us-west-2:123456789012:table/orders/export/abc",
                    "ExportStatus": "COMPLETED",
                },
                {
                    "ExportArn": "arn:aws:dynamodb:us-west-2:123456789012:table/orders/export/def",
                    "ExportStatus": "FAILED",
                },
                {
                    "ExportArn": "arn:aws:dynamodb:us-west-2:123456789012:table/orders/export/ghi",
                    "ExportStatus": "COMPLETED",
                },
            ]
        }
        mock_client.describe_export.side_effect = [
            {
                "ExportDescription": {
                    "S3Bucket": "export-bucket",
                    "S3Prefix": "dynamo/orders/",
                }
            },
            {
                "ExportDescription": {
                    "S3Bucket": "export-bucket",
                    "S3Prefix": "dynamo/orders/",
                }
            },
        ]

        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn=table_arn,
            dataset_urn=dataset_urn,
            dataset_name="us-west-2.orders",
            field_paths=["orderId", "customer.address"],
        )

        workunits = list(source._emit_s3_export_lineage())
        assert len(workunits) == 1
        assert source.report.s3_export_locations_found == 1
        assert source.report.s3_export_lineage_edges == 1

        mcp = workunits[0].metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert isinstance(mcp.aspect, UpstreamLineageClass)
        assert mcp.aspect.upstreams[0].dataset == dataset_urn
        assert mcp.aspect.fineGrainedLineages is not None
        assert len(mcp.aspect.fineGrainedLineages) == 2
        assert mcp.aspect.fineGrainedLineages[0].upstreams == [
            f"urn:li:schemaField:({dataset_urn},orderId)"
        ]
        assert mcp.aspect.fineGrainedLineages[0].downstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,export-bucket/dynamo/orders,PROD),orderId)"
        ]
        assert (
            mcp.entityUrn
            == "urn:li:dataset:(urn:li:dataPlatform:s3,export-bucket/dynamo/orders,PROD)"
        )
        mock_client.describe_export.assert_called()

    def test_aggregates_multiple_tables_to_same_s3_prefix(
        self, mock_context, lineage_config
    ):
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {
                    "ExportArn": "arn:export/1",
                    "ExportStatus": "COMPLETED",
                }
            ]
        }
        mock_client.describe_export.return_value = {
            "ExportDescription": {
                "S3Bucket": "shared-bucket",
                "S3Prefix": "exports",
            }
        }

        upstream_a = (
            "urn:li:dataset:(urn:li:dataPlatform:dynamodb,123.a.us-west-2.table_a,PROD)"
        )
        upstream_b = (
            "urn:li:dataset:(urn:li:dataPlatform:dynamodb,123.a.us-west-2.table_b,PROD)"
        )
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/a",
            dataset_urn=upstream_a,
            dataset_name="us-west-2.table_a",
            field_paths=["id"],
        )
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/b",
            dataset_urn=upstream_b,
            dataset_name="us-west-2.table_b",
            field_paths=["id", "name"],
        )

        workunits = list(source._emit_s3_export_lineage())
        assert len(workunits) == 1
        mcp = workunits[0].metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert isinstance(mcp.aspect, UpstreamLineageClass)
        upstream_urns = {u.dataset for u in mcp.aspect.upstreams}
        assert upstream_urns == {upstream_a, upstream_b}
        assert source.report.s3_export_lineage_edges == 2
        assert mcp.aspect.fineGrainedLineages is not None
        assert len(mcp.aspect.fineGrainedLineages) == 3

    def test_list_exports_error_is_warning(self, mock_context, lineage_config):
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.side_effect = Exception("AccessDenied")

        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)",
            dataset_name="us-west-2.x",
        )

        assert list(source._emit_s3_export_lineage()) == []
        assert len(source.report.warnings) >= 1
        assert any(
            "Failed to list DynamoDB table exports" in str(w)
            for w in source.report.warnings
        )

    def test_column_lineage_can_be_disabled(self, mock_context, lineage_config):
        lineage_config.include_s3_export_column_lineage = False
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {"ExportArn": "arn:export/1", "ExportStatus": "COMPLETED"}
            ]
        }
        mock_client.describe_export.return_value = {
            "ExportDescription": {"S3Bucket": "b", "S3Prefix": "p"}
        }
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)",
            dataset_name="us-west-2.x",
            field_paths=["id"],
        )
        workunits = list(source._emit_s3_export_lineage())
        mcp = workunits[0].metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert isinstance(mcp.aspect, UpstreamLineageClass)
        assert not mcp.aspect.fineGrainedLineages
