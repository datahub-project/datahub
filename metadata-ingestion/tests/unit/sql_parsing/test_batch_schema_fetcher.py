from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.sql_parsing.schema_resolver import BatchSchemaFetcher, SchemaResolver


def create_mock_schema(fields: list) -> SchemaMetadataClass:
    """Helper to create a mock schema with given field names."""
    schema_fields = [
        SchemaFieldClass(
            fieldPath=field,
            nativeDataType="STRING",
            type={"type": {"com.linkedin.schema.StringType": {}}},
        )
        for field in fields
    ]
    return SchemaMetadataClass(
        schemaName="test_schema",
        platform="urn:li:dataPlatform:bigquery",
        version=0,
        hash="",
        platformSchema={"com.linkedin.schema.MySqlDDL": {"tableSchema": ""}},
        fields=schema_fields,
    )


class TestBatchSchemaFetcher:
    """Tests for BatchSchemaFetcher batching and error handling."""

    def test_request_schema_queues_without_fetching(self):
        """Test that request_schema queues URNs without immediate fetch."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_resolver = Mock(spec=SchemaResolver)

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=50,
            auto_flush_threshold=100,
        )

        fetcher.request_schema(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table1,PROD)"
        )

        assert len(fetcher._pending_urns) == 1
        assert fetcher._stats_requested == 1
        assert fetcher._stats_fetched == 0
        mock_graph.get_entities.assert_not_called()

    def test_auto_flush_on_threshold(self):
        """Test that auto-flush triggers when threshold is reached."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.get_entities.return_value = {}

        mock_resolver = Mock(spec=SchemaResolver)
        mock_resolver.add_schema_metadata_from_fetch.return_value = False

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=50,
            auto_flush_threshold=3,
        )

        fetcher.request_schema("urn:li:dataset:1")
        fetcher.request_schema("urn:li:dataset:2")
        assert fetcher._stats_api_calls == 0

        fetcher.request_schema("urn:li:dataset:3")

        assert fetcher._stats_api_calls == 1
        assert len(fetcher._pending_urns) == 0

    def test_batch_fetch_success(self):
        """Test successful batch fetch of multiple schemas."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.get_entities.return_value = {
            "urn:li:dataset:1": {
                "schemaMetadata": (
                    create_mock_schema(["col1", "col2"]),
                    {},
                )
            },
            "urn:li:dataset:2": {
                "schemaMetadata": (
                    create_mock_schema(["col3", "col4"]),
                    {},
                )
            },
        }

        mock_resolver = Mock(spec=SchemaResolver)
        mock_resolver.add_schema_metadata_from_fetch.return_value = True

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=50,
        )

        fetcher.request_schema("urn:li:dataset:1")
        fetcher.request_schema("urn:li:dataset:2")
        fetched_count = fetcher.flush()

        assert fetched_count == 2
        assert mock_graph.get_entities.call_count == 1
        assert mock_resolver.add_schema_metadata_from_fetch.call_count == 2
        assert fetcher._stats_fetched == 2
        assert fetcher._stats_api_calls == 1

    def test_batch_fetch_with_missing_schemas(self):
        """Test batch fetch when some schemas are not found."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.get_entities.return_value = {
            "urn:li:dataset:1": {
                "schemaMetadata": (
                    create_mock_schema(["col1"]),
                    {},
                )
            },
        }

        mock_resolver = Mock(spec=SchemaResolver)
        mock_resolver.add_schema_metadata_from_fetch.return_value = True

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=50,
        )

        fetcher.request_schema("urn:li:dataset:1")
        fetcher.request_schema("urn:li:dataset:2")
        fetcher.request_schema("urn:li:dataset:3")
        fetched_count = fetcher.flush()

        assert mock_resolver.add_schema_metadata_from_fetch.call_count == 3
        assert fetched_count == 1
        assert fetcher._stats_fetched == 1

    def test_batch_fetch_error_fallback(self):
        """Test fallback to single fetch when batch fails."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.get_entities.side_effect = Exception("Batch API error")
        mock_graph.get_aspect.return_value = create_mock_schema(["col1"])

        mock_resolver = Mock(spec=SchemaResolver)
        mock_resolver.add_schema_metadata_from_fetch.return_value = True

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=2,
        )

        fetcher.request_schema("urn:li:dataset:1")
        fetcher.request_schema("urn:li:dataset:2")
        fetcher.flush()

        assert mock_graph.get_entities.call_count == 1
        assert mock_graph.get_aspect.call_count == 2
        assert fetcher._stats_fetched == 2
        assert fetcher._stats_api_calls == 3

    def test_batching_with_large_dataset(self):
        """Test batching behavior with more URNs than batch_size."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.get_entities.return_value = {}

        mock_resolver = Mock(spec=SchemaResolver)
        mock_resolver.add_schema_metadata_from_fetch.return_value = False

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=3,
        )

        for i in range(7):
            fetcher.request_schema(f"urn:li:dataset:{i}")

        fetcher.flush()

        assert mock_graph.get_entities.call_count == 3
        assert fetcher._stats_api_calls == 3

        calls = mock_graph.get_entities.call_args_list
        assert len(calls[0][1]["urns"]) == 3
        assert len(calls[1][1]["urns"]) == 3
        assert len(calls[2][1]["urns"]) == 1

    def test_flush_empty_queue(self):
        """Test flushing with no pending requests."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_resolver = Mock(spec=SchemaResolver)

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=50,
        )

        fetched_count = fetcher.flush()

        assert fetched_count == 0
        mock_graph.get_entities.assert_not_called()

    def test_get_stats(self):
        """Test statistics tracking."""
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.get_entities.return_value = {
            "urn:li:dataset:1": {"schemaMetadata": (create_mock_schema(["col1"]), {})}
        }

        mock_resolver = Mock(spec=SchemaResolver)
        mock_resolver.add_schema_metadata_from_fetch.return_value = True

        fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=mock_resolver,
            batch_size=50,
        )

        stats = fetcher.get_stats()
        assert stats["requested"] == 0
        assert stats["fetched"] == 0
        assert stats["api_calls"] == 0
        assert stats["pending"] == 0

        fetcher.request_schema("urn:li:dataset:1")
        stats = fetcher.get_stats()
        assert stats["pending"] == 1

        fetcher.flush()
        stats = fetcher.get_stats()
        assert stats["requested"] == 1
        assert stats["fetched"] == 1
        assert stats["api_calls"] == 1
        assert stats["pending"] == 0


class TestCachePrecedence:
    """Tests for schema cache precedence (ingestion > DataHub fetch)."""

    def test_ingestion_schema_takes_precedence(self):
        """Test that schemas from ingestion take priority over DataHub."""
        mock_graph = Mock(spec=DataHubGraph)

        resolver = SchemaResolver(
            platform="bigquery",
            env="PROD",
            graph=mock_graph,
        )

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"

        ingestion_schema = {"col1": "STRING", "col2": "INT"}
        resolver.add_raw_schema_info(urn, ingestion_schema)

        datahub_schema = create_mock_schema(["old_col1", "old_col2"])
        accepted = resolver.add_schema_metadata_from_fetch(urn, datahub_schema)

        assert accepted is False

        cached_schema = resolver._schema_cache.get(urn)
        assert cached_schema == ingestion_schema

    def test_datahub_schema_accepted_when_no_ingestion_schema(self):
        """Test that DataHub schemas are accepted when no ingestion schema exists."""
        mock_graph = Mock(spec=DataHubGraph)

        resolver = SchemaResolver(
            platform="bigquery",
            env="PROD",
            graph=mock_graph,
        )

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"

        datahub_schema = create_mock_schema(["col1", "col2"])
        accepted = resolver.add_schema_metadata_from_fetch(urn, datahub_schema)

        assert accepted is True

        cached_schema = resolver._schema_cache.get(urn)
        assert cached_schema is not None
        assert "col1" in cached_schema
        assert "col2" in cached_schema

    def test_none_schema_not_cached_when_batch_fetcher_active(self):
        """Test that None schemas aren't cached when using batch fetcher."""
        mock_graph = Mock(spec=DataHubGraph)

        resolver = SchemaResolver(
            platform="bigquery",
            env="PROD",
            graph=mock_graph,
        )

        batch_fetcher = BatchSchemaFetcher(
            graph=mock_graph,
            schema_resolver=resolver,
            batch_size=50,
        )
        resolver.batch_fetcher = batch_fetcher

        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"

        table = type(
            "obj",
            (object,),
            {"database": "project", "db_schema": "dataset", "table": "table"},
        )()

        resolved_urn, schema = resolver.resolve_table(table)

        assert len(batch_fetcher._pending_urns) == 1

        cached = resolver._schema_cache.get(urn)
        assert cached is None
