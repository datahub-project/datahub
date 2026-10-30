import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_iceberg import (
    SnowflakeIcebergHandler,
    build_storage_location,
    writer_storage_location_candidates,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeExternalVolumeLocation,
    SnowflakeIcebergTableInfo,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

STORAGE_BASE_URL = "s3://my-bucket/landing/"
STORAGE_LOCATION = "s3://my-bucket/landing/events"
WRITER_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.gold.events,PROD)"
)
SF_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
)
S3_FALLBACK_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/landing/events,PROD)"
)


def _config(**overrides: Any) -> SnowflakeV2Config:
    return SnowflakeV2Config.model_validate(
        {"account_id": "test_account", "username": "u", "password": "p", **overrides}
    )


def _table(name: str = "EVENTS", is_iceberg: bool = True) -> SnowflakeTable:
    return SnowflakeTable(
        name=name,
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        is_iceberg=is_iceberg,
    )


def _unmanaged_info(storage_location: Optional[str]) -> SnowflakeIcebergTableInfo:
    return SnowflakeIcebergTableInfo(
        external_volume_name="MY_VOLUME",
        catalog_name="MY_OBJSTORE_CATALOG",
        iceberg_table_type="UNMANAGED",
        base_location="events/",
        storage_location=storage_location,
    )


def _volume_rows(
    locations: List[Any], active: str = "", raw: bool = False
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = [
        {"parent_property": "", "property": "ALLOW_WRITES", "property_value": "false"}
    ]
    for i, loc in enumerate(locations, start=1):
        rows.append(
            {
                "parent_property": "STORAGE_LOCATIONS",
                "property": f"STORAGE_LOCATION_{i}",
                "property_value": loc if raw else json.dumps(loc),
            }
        )
    rows.append(
        {
            "parent_property": "STORAGE_LOCATIONS",
            "property": "ACTIVE",
            "property_value": active,
        }
    )
    return rows


def _data_dictionary(rows: Optional[List[Dict[str, Any]]] = None) -> Any:
    connection = MagicMock(spec=SnowflakeConnection)
    if rows is None:
        connection.query.side_effect = Exception("insufficient privileges")
    else:
        connection.query.return_value = iter(rows)
    report = MagicMock(spec=SnowflakeV2Report)
    return SnowflakeDataDictionary(connection, report), connection, report


class TestConfig:
    def test_external_lineage_requires_metadata_and_table_lineage(self) -> None:
        with pytest.raises(ValueError, match="include_iceberg_metadata"):
            _config(include_iceberg_external_lineage=True)
        with pytest.raises(ValueError, match="include_table_lineage"):
            _config(
                include_iceberg_metadata=True,
                include_iceberg_external_lineage=True,
                include_table_lineage=False,
            )
        config = _config(
            include_iceberg_metadata=True, include_iceberg_external_lineage=True
        )
        assert config.include_iceberg_external_lineage


class TestStorageLocation:
    @pytest.mark.parametrize(
        "base_url,base_location,expected",
        [
            ("s3://b/prefix/", "events/", "s3://b/prefix/events"),
            ("s3://b/prefix", "events", "s3://b/prefix/events"),
            ("s3://b/prefix/", "/nested/events/", "s3://b/prefix/nested/events"),
            ("s3://b/prefix/", None, "s3://b/prefix"),
            ("s3://b/prefix/", "", "s3://b/prefix"),
        ],
    )
    def test_build_storage_location_normalizes_slashes(
        self, base_url: str, base_location: Optional[str], expected: str
    ) -> None:
        assert build_storage_location(base_url, base_location) == expected

    @pytest.mark.parametrize(
        "location,expected",
        [
            ("s3://b/p/t", ["s3://b/p/t", "s3://b/p/t/"]),
            (
                "gcs://b/p/t/",
                ["gcs://b/p/t", "gcs://b/p/t/", "gs://b/p/t", "gs://b/p/t/"],
            ),
            (
                "azure://acct.blob.core.windows.net/cont/p/t",
                [
                    "azure://acct.blob.core.windows.net/cont/p/t",
                    "azure://acct.blob.core.windows.net/cont/p/t/",
                    "abfss://cont@acct.dfs.core.windows.net/p/t",
                    "abfss://cont@acct.dfs.core.windows.net/p/t/",
                ],
            ),
            (
                "azure://acct.blob.core.windows.net/cont",
                [
                    "azure://acct.blob.core.windows.net/cont",
                    "azure://acct.blob.core.windows.net/cont/",
                    "abfss://cont@acct.dfs.core.windows.net",
                    "abfss://cont@acct.dfs.core.windows.net/",
                ],
            ),
        ],
    )
    def test_writer_candidates_cover_spark_spellings(
        self, location: str, expected: List[str]
    ) -> None:
        assert writer_storage_location_candidates(location) == expected

    def test_parse_volume_prefers_active_location(self) -> None:
        data_dict, _, report = _data_dictionary(
            _volume_rows(
                [
                    {"NAME": "old", "STORAGE_BASE_URL": "s3://old/"},
                    {"NAME": "current", "STORAGE_BASE_URL": "s3://new/"},
                ],
                active="current",
            )
        )
        loc = data_dict.get_external_volume_location("VOL")
        assert loc is not None
        assert loc.storage_base_url == "s3://new/"
        report.warning.assert_not_called()

    def test_parse_volume_falls_back_to_first_location(self) -> None:
        data_dict, _, _ = _data_dictionary(
            _volume_rows([{"NAME": "only", "STORAGE_BASE_URL": "gcs://b/p/"}])
        )
        loc = data_dict.get_external_volume_location("VOL")
        assert loc is not None and loc.storage_base_url == "gcs://b/p/"

    def test_parse_volume_without_locations_warns(self) -> None:
        data_dict, _, report = _data_dictionary(_volume_rows([]))
        assert data_dict.get_external_volume_location("VOL") is None
        report.warning.assert_called_once()
        assert "no usable storage location" in report.warning.call_args.kwargs["title"]

    def test_unreadable_location_is_skipped_with_warning(self) -> None:
        data_dict, _, report = _data_dictionary(
            _volume_rows(
                ["not json", json.dumps(["a", "list"]), json.dumps({"NAME": "x"})],
                raw=True,
            )
        )
        assert data_dict.get_external_volume_location("VOL") is None
        # one per unreadable entry, plus the no-usable-location summary
        assert report.warning.call_count == 4

    def test_describe_failure_is_cached_as_none(self) -> None:
        data_dict, connection, report = _data_dictionary(rows=None)

        assert data_dict.get_external_volume_location("VOL") is None
        assert data_dict.get_external_volume_location("VOL") is None
        assert connection.query.call_count == 1
        report.warning.assert_called_once()


class TestEnrichTables:
    def _handler(self, data_dict: Any) -> SnowflakeIcebergHandler:
        return SnowflakeIcebergHandler(
            config=_config(),
            report=SnowflakeV2Report(),
            data_dictionary=data_dict,
            identifiers=MagicMock(spec=SnowflakeIdentifierBuilder),
            graph=None,
            aggregator=None,
        )

    def test_attaches_info_and_storage_location(self) -> None:
        data_dict = MagicMock(spec=SnowflakeDataDictionary)
        data_dict.get_iceberg_tables_for_schema.return_value = {
            "EVENTS": _unmanaged_info(storage_location=None),
            "ORPHANED": SnowflakeIcebergTableInfo(
                external_volume_name="LOCKED_VOLUME", iceberg_table_type="UNMANAGED"
            ),
        }
        data_dict.get_external_volume_location.side_effect = lambda name: (
            SnowflakeExternalVolumeLocation(
                volume_name=name, name="loc", storage_base_url=STORAGE_BASE_URL
            )
            if name == "MY_VOLUME"
            else None
        )
        handler = self._handler(data_dict)
        iceberg = _table("EVENTS")
        orphaned = _table("ORPHANED")
        unlisted = _table("UNLISTED")

        handler.enrich_tables([iceberg, orphaned, unlisted], "MY_DB", "MY_SCHEMA")

        assert iceberg.iceberg is not None
        assert iceberg.iceberg.storage_location == STORAGE_LOCATION
        # Volume not describable: properties still attached, no storage location.
        assert orphaned.iceberg is not None
        assert orphaned.iceberg.storage_location is None
        assert unlisted.iceberg is None
        assert handler.report.iceberg_tables_scanned == 2
        # One warning per schema for tables SHOW did not list, naming them.
        assert len(handler.report.warnings) == 1
        assert "UNLISTED" in str(handler.report.warnings[0].context)
        data_dict.get_iceberg_tables_for_schema.assert_called_once_with(
            db_name="MY_DB", schema_name="MY_SCHEMA"
        )

    def test_custom_properties_skip_empty_values(self) -> None:
        props = SnowflakeIcebergHandler.custom_properties(
            _unmanaged_info(STORAGE_LOCATION)
        )
        assert props == {
            "ICEBERG_TABLE_TYPE": "UNMANAGED",
            "EXTERNAL_VOLUME": "MY_VOLUME",
            "BASE_LOCATION": "events/",
            "STORAGE_LOCATION": STORAGE_LOCATION,
            "CATALOG_INTEGRATION": "MY_OBJSTORE_CATALOG",
        }


def _register_downstream_schema(aggregator: SqlParsingAggregator) -> None:
    aggregator.register_schema(
        SF_TABLE_URN,
        SchemaMetadataClass(
            schemaName="events",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath="occur_date",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="DATE",
                )
            ],
        ),
    )


def _lineage(aggregator: SqlParsingAggregator) -> List[UpstreamLineageClass]:
    return [
        mcp.aspect
        for mcp in aggregator.gen_metadata()
        if isinstance(mcp.aspect, UpstreamLineageClass)
    ]


def _upstreams(aggregator: SqlParsingAggregator) -> List[str]:
    lineage = _lineage(aggregator)
    return [u.dataset for u in lineage[0].upstreams] if lineage else []


class TestRegisterLineage:
    def _handler(
        self, graph_hits: Optional[Any], aggregator: SqlParsingAggregator
    ) -> SnowflakeIcebergHandler:
        """graph_hits: search results, or an Exception, or None for no graph."""
        config = _config()
        report = SnowflakeV2Report()
        graph = None
        if graph_hits is not None:
            graph = MagicMock()
            if isinstance(graph_hits, Exception):
                graph.get_urns_by_filter.side_effect = graph_hits
            else:
                graph.get_urns_by_filter.return_value = iter(graph_hits)
        return SnowflakeIcebergHandler(
            config=config,
            report=report,
            data_dictionary=MagicMock(spec=SnowflakeDataDictionary),
            identifiers=SnowflakeIdentifierBuilder(
                identifier_config=config, structured_reporter=report
            ),
            graph=graph,
            aggregator=aggregator,
        )

    def _register(
        self, handler: SnowflakeIcebergHandler, info: SnowflakeIcebergTableInfo
    ) -> None:
        table = _table("EVENTS")
        table.iceberg = info
        handler.register_lineage(table, "MY_DB", "MY_SCHEMA")

    def test_writer_found_gets_direct_edge_with_cll(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        _register_downstream_schema(aggregator)
        # The Snowflake table itself may carry the same storage_location; it is not a writer.
        handler = self._handler([SF_TABLE_URN, WRITER_URN], aggregator)

        self._register(handler, _unmanaged_info(STORAGE_LOCATION))

        lineage = _lineage(aggregator)
        assert len(lineage) == 1
        assert [u.dataset for u in lineage[0].upstreams] == [WRITER_URN]
        assert lineage[0].upstreams[0].type == "COPY"
        assert lineage[0].fineGrainedLineages
        assert lineage[0].fineGrainedLineages[0].upstreams == [
            f"urn:li:schemaField:({WRITER_URN},occur_date)"
        ]
        assert handler.report.num_iceberg_lineage_writer_resolved == 1
        assert handler.report.num_iceberg_lineage_storage_fallback == 0
        # One search covering both trailing-slash spellings.
        calls = handler.graph.get_urns_by_filter.call_args_list  # type: ignore[union-attr]
        assert len(calls) == 1
        assert calls[0].kwargs["extraFilters"][0]["values"] == [
            f"storage_location={STORAGE_LOCATION}",
            f"storage_location={STORAGE_LOCATION}/",
        ]

    def test_no_writer_falls_back_to_storage_urn(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        handler = self._handler([], aggregator)

        self._register(handler, _unmanaged_info(STORAGE_LOCATION))

        assert _upstreams(aggregator) == [S3_FALLBACK_URN]
        assert handler.report.num_iceberg_lineage_storage_fallback == 1

    def test_ambiguous_writer_falls_back_and_warns(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        handler = self._handler([WRITER_URN, WRITER_URN + "_2"], aggregator)

        self._register(handler, _unmanaged_info(STORAGE_LOCATION))

        assert _upstreams(aggregator) == [S3_FALLBACK_URN]
        assert handler.report.num_iceberg_lineage_writer_ambiguous == 1
        assert handler.report.num_iceberg_lineage_storage_fallback == 1
        assert len(handler.report.warnings) == 1

    def test_lookup_failure_falls_back_and_warns(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        handler = self._handler(Exception("search unavailable"), aggregator)

        self._register(handler, _unmanaged_info(STORAGE_LOCATION))

        assert _upstreams(aggregator) == [S3_FALLBACK_URN]
        assert handler.report.num_iceberg_lineage_writer_resolved == 0
        assert handler.report.num_iceberg_lineage_storage_fallback == 1
        assert len(handler.report.warnings) == 1

    def test_without_graph_uses_storage_urn(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        handler = self._handler(None, aggregator)

        self._register(handler, _unmanaged_info("gcs://my-bucket/landing/events"))

        assert _upstreams(aggregator) == [
            "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/landing/events,PROD)"
        ]
        assert handler.report.num_iceberg_lineage_storage_fallback == 1

    def test_unsupported_scheme_emits_no_edge_and_warns(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        handler = self._handler(None, aggregator)

        self._register(handler, _unmanaged_info("file:///mnt/landing/events"))

        assert _lineage(aggregator) == []
        assert handler.report.num_iceberg_lineage_storage_fallback == 0
        assert len(handler.report.warnings) == 1

    def test_snowflake_managed_or_unlocated_tables_get_no_lineage(self) -> None:
        aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
        handler = self._handler([WRITER_URN], aggregator)

        managed = _unmanaged_info(STORAGE_LOCATION)
        managed.iceberg_table_type = "SNOWFLAKE"
        self._register(handler, managed)
        self._register(handler, _unmanaged_info(storage_location=None))

        assert _lineage(aggregator) == []
        assert handler.report.num_iceberg_lineage_skipped_no_storage_location == 1
        handler.graph.get_urns_by_filter.assert_not_called()  # type: ignore[union-attr]


class TestProcessTablesGating:
    @pytest.mark.parametrize(
        "metadata,lineage,expect_enrich,expect_lineage",
        [
            (False, False, False, False),
            (True, False, True, False),
            (True, True, True, True),
        ],
    )
    def test_flags_gate_the_iceberg_handler(
        self, metadata: bool, lineage: bool, expect_enrich: bool, expect_lineage: bool
    ) -> None:
        gen = SnowflakeSchemaGenerator.__new__(SnowflakeSchemaGenerator)
        gen.aggregator = None
        gen.report = MagicMock()
        gen.identifiers = MagicMock()
        gen.filters = MagicMock()
        gen.config = MagicMock()
        gen.config.include_technical_schema = False
        gen.dynamic_table_identifiers = set()
        gen.include_iceberg_metadata = metadata
        gen.include_iceberg_external_lineage = lineage
        gen.iceberg_handler = MagicMock(spec=SnowflakeIcebergHandler)
        iceberg = _table("EVENTS")

        list(
            gen._process_tables(
                [iceberg, _table("PLAIN", is_iceberg=False)],
                MagicMock(),
                "MY_DB",
                "MY_SCHEMA",
            )
        )

        assert gen.iceberg_handler.enrich_tables.called == expect_enrich
        if expect_enrich:
            # Only the Iceberg tables reach the handler.
            assert gen.iceberg_handler.enrich_tables.call_args.args[0] == [iceberg]
        assert gen.iceberg_handler.register_lineage.called == expect_lineage
