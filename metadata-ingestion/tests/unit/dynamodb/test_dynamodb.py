import json
from types import SimpleNamespace
from typing import List, Tuple
from unittest.mock import MagicMock, call, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dynamodb.dynamodb import (
    PAGE_SIZE,
    DynamoDBConfig,
    DynamoDBSource,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    UpstreamClass,
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


def _patched_lineage(wu: MetadataWorkUnit) -> UpstreamLineageClass:
    """Rebuild the UpstreamLineage the S3 export patch would add."""
    mcp = wu.metadata
    assert isinstance(mcp, MetadataChangeProposalClass)
    assert mcp.changeType == ChangeTypeClass.PATCH
    assert mcp.aspectName == UpstreamLineageClass.ASPECT_NAME
    assert mcp.aspect is not None
    upstreams: List[UpstreamClass] = []
    fine_grained: List[FineGrainedLineageClass] = []
    ops = json.loads(mcp.aspect.value)
    assert len({op["path"] for op in ops}) == len(ops)
    for op in ops:
        assert op["op"] == "add"
        parts = [
            part.replace("~1", "/").replace("~0", "~")
            for part in op["path"].split("/")[1:]
        ]
        if parts[0] == "upstreams":
            assert parts[1] == op["value"]["dataset"]
            upstreams.append(UpstreamClass.from_obj(op["value"]))
        else:
            assert parts[0] == "fineGrainedLineages"
            _, _, downstream, _, upstream = parts
            fine_grained.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[downstream],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[upstream],
                )
            )
    return UpstreamLineageClass(
        upstreams=upstreams, fineGrainedLineages=fine_grained or None
    )


def _lineage_workunits(workunits: List[MetadataWorkUnit]) -> List[MetadataWorkUnit]:
    return [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
    ]


def _mock_orders_table_client() -> MagicMock:
    mock_client = MagicMock()
    mock_client.meta.region_name = "us-west-2"

    list_tables_paginator = MagicMock()
    list_tables_paginator.paginate.return_value = [{"TableNames": ["orders"]}]
    scan_paginator = MagicMock()
    scan_paginator.paginate.return_value = [
        {
            "Items": [
                {
                    "orderId": {"S": "1"},
                    "customer": {"M": {"name": {"S": "a"}}},
                }
            ]
        }
    ]

    def _get_paginator(name):
        return list_tables_paginator if name == "list_tables" else scan_paginator

    mock_client.get_paginator.side_effect = _get_paginator
    mock_client.describe_table.return_value = {
        "Table": {
            "TableArn": "arn:aws:dynamodb:us-west-2:123456789012:table/orders",
            "ItemCount": 1,
            "KeySchema": [{"AttributeName": "orderId", "KeyType": "HASH"}],
        }
    }
    mock_client.list_exports.return_value = {
        "ExportSummaries": [{"ExportArn": "arn:export/1", "ExportStatus": "COMPLETED"}]
    }
    mock_client.describe_export.return_value = {
        "ExportDescription": {"S3Bucket": "export-bucket", "S3Prefix": "orders"}
    }
    return mock_client


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
        lineage_config.include_s3_export_column_lineage = True
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        table_arn = "arn:aws:dynamodb:us-west-2:123456789012:table/orders"
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dynamodb,123456789012.us-west-2.orders,PROD)"
        completed_export_a = (
            "arn:aws:dynamodb:us-west-2:123456789012:table/orders/export/abc"
        )
        completed_export_b = (
            "arn:aws:dynamodb:us-west-2:123456789012:table/orders/export/ghi"
        )

        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {
                    "ExportArn": completed_export_a,
                    "ExportStatus": "COMPLETED",
                },
                {
                    "ExportArn": "arn:aws:dynamodb:us-west-2:123456789012:table/orders/export/def",
                    "ExportStatus": "FAILED",
                },
                {
                    "ExportArn": completed_export_b,
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
        assert not source.report.warnings
        # S3 export datasets are not owned by the DynamoDB source; is_primary_source must stay False
        # so stateful ingestion can't soft-delete them.
        assert workunits[0].is_primary_source is False

        lineage = _patched_lineage(workunits[0])
        assert lineage.upstreams[0].dataset == dataset_urn
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 2
        # graph is None in the fixture, so column lineage falls back to inferred identity mapping
        assert source.report.s3_export_column_lineage_inferred == 2
        assert source.report.s3_export_column_lineage_resolved == 0
        assert lineage.fineGrainedLineages[0].upstreams == [
            f"urn:li:schemaField:({dataset_urn},orderId)"
        ]
        assert lineage.fineGrainedLineages[0].downstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,export-bucket/dynamo/orders,PROD),orderId)"
        ]
        assert (
            workunits[0].get_urn()
            == "urn:li:dataset:(urn:li:dataPlatform:s3,export-bucket/dynamo/orders,PROD)"
        )
        mock_client.describe_export.assert_has_calls(
            [
                call(ExportArn=completed_export_a),
                call(ExportArn=completed_export_b),
            ]
        )
        assert mock_client.describe_export.call_count == 2

    def test_aggregates_multiple_tables_to_same_s3_prefix(
        self, mock_context, lineage_config
    ):
        lineage_config.include_s3_export_column_lineage = True
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
        lineage = _patched_lineage(workunits[0])
        upstream_urns = {u.dataset for u in lineage.upstreams}
        assert upstream_urns == {upstream_a, upstream_b}
        assert source.report.s3_export_lineage_edges == 2
        # Two tables, one shared destination: the counter tracks distinct destinations (1), not
        # (table, destination) pairs (2).
        assert source.report.s3_export_locations_found == 1
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 3

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
        assert any(
            w.title == "DynamoDB S3 Export Lineage" for w in source.report.warnings
        )

    def test_describe_export_error_is_warning(self, mock_context, lineage_config):
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {"ExportArn": "arn:export/1", "ExportStatus": "COMPLETED"}
            ]
        }
        mock_client.describe_export.side_effect = Exception("AccessDenied")

        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)",
            dataset_name="us-west-2.x",
        )

        assert list(source._emit_s3_export_lineage()) == []
        assert source.report.s3_export_locations_found == 0
        assert source.report.s3_export_lineage_edges == 0
        assert any(
            w.title == "DynamoDB S3 Export Lineage" for w in source.report.warnings
        )

    def test_column_lineage_off_by_default(self, mock_context, lineage_config):
        # include_s3_export_column_lineage is omitted, so the default (False)
        # applies: table-level COPY only, no fine-grained lineage.
        assert lineage_config.include_s3_export_column_lineage is False
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
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)"
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn=dataset_urn,
            dataset_name="us-west-2.x",
            field_paths=["id"],
        )
        workunits = list(source._emit_s3_export_lineage())
        lineage = _patched_lineage(workunits[0])
        assert not lineage.fineGrainedLineages
        assert len(lineage.upstreams) == 1
        assert lineage.upstreams[0].dataset == dataset_urn

    def test_column_lineage_resolves_against_s3_schema(
        self, mock_context, lineage_config
    ):
        # When the S3 export dataset is already in DataHub, column lineage links only the
        # fields that match its schema (case-insensitively); unmatched fields are dropped.
        lineage_config.include_s3_export_column_lineage = True
        s3_schema = SimpleNamespace(
            fields=[
                SimpleNamespace(fieldPath="[version=2.0].[type=string].OrderId"),
                SimpleNamespace(fieldPath="total"),
            ]
        )
        mock_context.graph = MagicMock()
        mock_context.graph.get_schema_metadata.return_value = s3_schema
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
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)"
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn=dataset_urn,
            dataset_name="us-west-2.x",
            # orderId matches OrderId (case-insensitive); missing_field has no match.
            field_paths=["orderId", "missing_field"],
        )
        workunits = list(source._emit_s3_export_lineage())
        lineage = _patched_lineage(workunits[0])
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 1
        assert source.report.s3_export_column_lineage_resolved == 1
        assert source.report.s3_export_column_lineage_inferred == 0
        fgl = lineage.fineGrainedLineages[0]
        assert fgl.upstreams == [f"urn:li:schemaField:({dataset_urn},orderId)"]
        # The downstream keeps the S3 dataset's stored field path, v2 prefix and casing included.
        assert fgl.downstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,b/p,PROD),"
            "[version=2.0].[type=string].OrderId)"
        ]

    def _export_lineage_with_graph(
        self,
        mock_context: MagicMock,
        lineage_config: DynamoDBConfig,
        graph: MagicMock,
        field_paths: List[str],
    ) -> Tuple[DynamoDBSource, UpstreamLineageClass]:
        lineage_config.include_s3_export_column_lineage = True
        mock_context.graph = graph
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
            field_paths=field_paths,
        )
        workunits = list(source._emit_s3_export_lineage())
        assert len(workunits) == 1
        return source, _patched_lineage(workunits[0])

    def test_column_lineage_inferred_when_s3_dataset_has_no_schema(
        self, mock_context, lineage_config
    ):
        graph = MagicMock()
        graph.get_schema_metadata.return_value = None
        source, lineage = self._export_lineage_with_graph(
            mock_context, lineage_config, graph, ["orderId"]
        )
        assert source.report.s3_export_column_lineage_inferred == 1
        assert lineage.fineGrainedLineages is not None
        assert lineage.fineGrainedLineages[0].downstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,b/p,PROD),orderId)"
        ]

    def test_column_lineage_skips_ambiguous_s3_fields(
        self, mock_context, lineage_config
    ):
        graph = MagicMock()
        graph.get_schema_metadata.return_value = SimpleNamespace(
            fields=[
                SimpleNamespace(fieldPath="[version=2.0].[type=string].status"),
                SimpleNamespace(fieldPath="[version=2.0].[type=long].status"),
                SimpleNamespace(fieldPath="orderId"),
            ]
        )
        source, lineage = self._export_lineage_with_graph(
            mock_context, lineage_config, graph, ["status", "orderId"]
        )
        assert lineage.fineGrainedLineages is not None
        assert [f.downstreams for f in lineage.fineGrainedLineages] == [
            [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,b/p,PROD),orderId)"
            ]
        ]
        assert any(
            "status" in c
            for i in source.report.infos
            if i.title == "DynamoDB S3 export column lineage skipped ambiguous fields"
            for c in i.context
        )

    def test_column_lineage_graph_error_keeps_table_lineage_only(
        self, mock_context, lineage_config
    ):
        # The S3 dataset may exist with a different schema, so a read error must not fall back
        # to guessed column edges.
        lineage_config.include_s3_export_column_lineage = True
        mock_context.graph = MagicMock()
        mock_context.graph.get_schema_metadata.side_effect = Exception("GMS error")
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
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)"
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn=dataset_urn,
            dataset_name="us-west-2.x",
            field_paths=["orderId"],
        )
        workunits = list(source._emit_s3_export_lineage())
        # Table-level COPY edge must be present.
        assert len(workunits) == 1
        lineage = _patched_lineage(workunits[0])
        assert [u.dataset for u in lineage.upstreams] == [dataset_urn]
        assert not lineage.fineGrainedLineages
        assert source.report.s3_export_column_lineage_inferred == 0
        assert [w.title for w in source.report.warnings] == [
            "Failed to read S3 export schema from DataHub"
        ]

    @pytest.mark.parametrize(
        "dataset_properties, expected_missing",
        [
            pytest.param(
                DatasetPropertiesClass(name="p"), [], id="destination-ingested"
            ),
            pytest.param(
                None,
                ["urn:li:dataset:(urn:li:dataPlatform:s3,b/p,PROD)"],
                id="destination-not-ingested",
            ),
        ],
    )
    def test_s3_export_destination_existence_reported(
        self, mock_context, lineage_config, dataset_properties, expected_missing
    ):
        # A lineage-only entity from a previous run has no datasetProperties, so
        # it still counts as missing.
        mock_context.graph = MagicMock()
        mock_context.graph.get_aspect.return_value = dataset_properties
        source = self._source_with_one_export(mock_context, lineage_config)

        workunits = list(source._emit_s3_export_lineage())

        assert len(workunits) == 1
        assert list(source.report.s3_export_destinations_not_in_datahub) == (
            expected_missing
        )
        # Entity existence is not the signal: a lineage-only stub exists too.
        mock_context.graph.exists.assert_not_called()
        infos = [
            info
            for info in source.report.infos
            if info.title == "S3 export destinations not ingested in DataHub"
        ]
        assert len(infos) == (1 if expected_missing else 0)

    def test_s3_export_destination_check_skipped_without_graph(
        self, mock_context, lineage_config
    ):
        source = self._source_with_one_export(mock_context, lineage_config)

        workunits = list(source._emit_s3_export_lineage())

        assert len(workunits) == 1
        assert list(source.report.s3_export_destinations_not_in_datahub) == []

    def test_s3_export_destination_check_error_keeps_lineage(
        self, mock_context, lineage_config
    ):
        mock_context.graph = MagicMock()
        mock_context.graph.get_aspect.side_effect = Exception("GMS error")
        source = self._source_with_one_export(mock_context, lineage_config)

        workunits = list(source._emit_s3_export_lineage())

        assert len(workunits) == 1
        assert list(source.report.s3_export_destinations_not_in_datahub) == []
        assert [w.title for w in source.report.warnings] == [
            "Failed to check S3 export destination in DataHub"
        ]

    @staticmethod
    def _source_with_one_export(
        mock_context: MagicMock, lineage_config: DynamoDBConfig
    ) -> DynamoDBSource:
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
            field_paths=["orderId"],
        )
        return source

    def test_column_lineage_resolved_but_nothing_matches_warns(
        self, mock_context, lineage_config
    ):
        # When the S3 schema is found but no DynamoDB field matches it, no column lineage is
        # emitted — and that must be distinguishable from column lineage being switched off.
        lineage_config.include_s3_export_column_lineage = True
        mock_context.graph = MagicMock()
        mock_context.graph.get_schema_metadata.return_value = SimpleNamespace(
            fields=[SimpleNamespace(fieldPath="totally_different")]
        )
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
            field_paths=["orderId"],
        )
        workunits = list(source._emit_s3_export_lineage())
        lineage = _patched_lineage(workunits[0])
        assert not lineage.fineGrainedLineages
        assert any(
            w.title == "DynamoDB S3 export column lineage produced no matches"
            for w in source.report.warnings
        )

    def test_s3_export_platform_instance_applied(self, mock_context, lineage_config):
        lineage_config.s3_export_platform_instance = "lake"
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
            "ExportDescription": {"S3Bucket": "bucket", "S3Prefix": "prefix"}
        }
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)",
            dataset_name="us-west-2.x",
        )
        workunits = list(source._emit_s3_export_lineage())
        assert (
            workunits[0].get_urn()
            == "urn:li:dataset:(urn:li:dataPlatform:s3,lake.bucket/prefix,PROD)"
        )

    def test_max_exports_per_table_caps_describe_calls(
        self, mock_context, lineage_config
    ):
        lineage_config.max_exports_per_table = 2
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {"ExportArn": f"arn:export/{i}", "ExportStatus": "COMPLETED"}
                for i in range(5)
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
        )
        assert mock_client.describe_export.call_count == 2
        # The described subset all resolved to one destination, so truncation is reported as info
        # (lineage is almost certainly complete), not a false-alarm warning.
        assert any("single S3 destination" in i.message for i in source.report.infos)
        assert not any(
            w.title == "DynamoDB S3 Export Lineage" for w in source.report.warnings
        )

    def test_truncation_warns_when_described_subset_fans_out(
        self, mock_context, lineage_config
    ):
        lineage_config.max_exports_per_table = 2
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {"ExportArn": f"arn:export/{i}", "ExportStatus": "COMPLETED"}
                for i in range(5)
            ]
        }
        # The two described exports go to different destinations, so the table demonstrably fans
        # out and dropped exports may hide further destinations: that is a real warning.
        mock_client.describe_export.side_effect = [
            {"ExportDescription": {"S3Bucket": "b", "S3Prefix": "p1"}},
            {"ExportDescription": {"S3Bucket": "b", "S3Prefix": "p2"}},
        ]
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)",
            dataset_name="us-west-2.x",
        )
        assert mock_client.describe_export.call_count == 2
        assert any(
            "multiple S3 destinations" in w.message for w in source.report.warnings
        )

    def test_truncation_with_no_resolved_destinations_does_not_claim_complete(
        self, mock_context, lineage_config
    ):
        lineage_config.max_exports_per_table = 2
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        mock_client.list_exports.return_value = {
            "ExportSummaries": [
                {"ExportArn": f"arn:export/{i}", "ExportStatus": "COMPLETED"}
                for i in range(5)
            ]
        }
        # Every described export fails, so nothing resolves to a destination: the truncation info
        # must not claim a single complete destination.
        mock_client.describe_export.side_effect = Exception("AccessDenied")
        source._collect_s3_export_lineage(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dynamodb,x,PROD)",
            dataset_name="us-west-2.x",
        )
        assert not any(
            "single S3 destination" in i.message for i in source.report.infos
        )
        assert any(
            "none of the described subset resolved" in i.message
            for i in source.report.infos
        )

    def test_list_exports_stops_paging_once_max_exports_reached(
        self, mock_context, lineage_config
    ):
        lineage_config.max_exports_per_table = 2
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        # Page one alone already exceeds the cap, so paging must stop before fetching page two.
        mock_client.list_exports.side_effect = [
            {
                "ExportSummaries": [
                    {"ExportArn": f"arn:export/{i}", "ExportStatus": "COMPLETED"}
                    for i in range(3)
                ],
                "NextToken": "page-2",
            },
            {
                "ExportSummaries": [
                    {"ExportArn": "arn:export/later", "ExportStatus": "COMPLETED"}
                ]
            },
        ]
        arns = source._list_completed_export_arns(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_name="us-west-2.x",
            max_exports=2,
        )
        assert mock_client.list_exports.call_count == 1
        assert len(arns) == 3

    def test_resolved_schema_but_no_upstream_fields_does_not_warn(
        self, mock_context, lineage_config
    ):
        # Column lineage on and the S3 schema resolves, but the DynamoDB side contributed no field
        # paths. That is not a "schema found, nothing matched" case, so no warning should fire.
        lineage_config.include_s3_export_column_lineage = True
        mock_context.graph = MagicMock()
        mock_context.graph.get_schema_metadata.return_value = SimpleNamespace(
            fields=[SimpleNamespace(fieldPath="id")]
        )
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
            field_paths=[],
        )
        list(source._emit_s3_export_lineage())
        assert not any(
            w.title == "DynamoDB S3 export column lineage produced no matches"
            for w in source.report.warnings
        )

    def test_list_exports_repeating_token_is_bounded(
        self, mock_context, lineage_config
    ):
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = MagicMock()
        # A buggy service returning a static NextToken would loop forever without the guard.
        mock_client.list_exports.return_value = {
            "ExportSummaries": [],
            "NextToken": "same-token-forever",
        }
        arns = source._list_completed_export_arns(
            dynamodb_client=mock_client,
            table_arn="arn:table/x",
            dataset_name="us-west-2.x",
            max_exports=source.config.max_exports_per_table,
        )
        assert arns == []
        # Two calls: first page, then one more that detects the repeat and stops.
        assert mock_client.list_exports.call_count == 2
        # Assert on the message, not the title (four distinct warnings share this title).
        assert any(
            "repeating pagination token" in w.message for w in source.report.warnings
        )

    def test_get_workunits_internal_skips_s3_export_lineage_by_default(
        self, mock_context
    ):
        source = DynamoDBSource(
            ctx=mock_context,
            config=DynamoDBConfig(
                aws_access_key_id="test",
                aws_secret_access_key="test",
                aws_region="us-west-2",
            ),
            platform="dynamodb",
        )
        mock_client = _mock_orders_table_client()

        with patch.object(
            DynamoDBConfig, "get_dynamodb_client", return_value=mock_client
        ):
            workunits = list(source.get_workunits_internal())

        mock_client.list_exports.assert_not_called()
        assert not _lineage_workunits(workunits)

    def test_get_workunits_internal_emits_s3_export_lineage(
        self, mock_context, lineage_config
    ):
        # Drives the real wiring in get_workunits_internal end-to-end: the table's actual
        # TableArn and the field paths produced by construct_schema_metadata feed the lineage
        # builder, rather than hand-built toy arguments.
        lineage_config.include_s3_export_column_lineage = True
        source = DynamoDBSource(
            ctx=mock_context, config=lineage_config, platform="dynamodb"
        )
        mock_client = _mock_orders_table_client()

        with patch.object(
            DynamoDBConfig, "get_dynamodb_client", return_value=mock_client
        ):
            workunits = list(source.get_workunits_internal())

        lineage_wus = _lineage_workunits(workunits)
        assert len(lineage_wus) == 1
        lineage_wu = lineage_wus[0]
        assert lineage_wu.is_primary_source is False
        aspect = _patched_lineage(lineage_wu)
        ddb_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:dynamodb,"
            "123456789012.us-west-2.orders,PROD)"
        )
        assert [u.dataset for u in aspect.upstreams] == [ddb_urn]
        # graph is None so column lineage is inferred from the real schema fields.
        assert aspect.fineGrainedLineages
        upstream_fields = {
            f.upstreams[0] for f in aspect.fineGrainedLineages if f.upstreams
        }
        assert f"urn:li:schemaField:({ddb_urn},orderId)" in upstream_fields
