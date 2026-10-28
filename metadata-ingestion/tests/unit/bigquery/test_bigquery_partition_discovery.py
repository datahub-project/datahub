"""PartitionDiscovery coordinator behaviour: strategy priority in
get_required_partition_filters, direct table-query discovery of latest partition
values, strategic date candidates and fallback filters."""

from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.types import (
    CachedPartitionMetadata,
)
from tests.unit.bigquery.test_bigquery_profiling import make_config, make_table

PROJECT = "test-project-123456"
SCHEMA = "ds"
TABLE_REF = f"`{PROJECT}`.`{SCHEMA}`.`test_table`"

ExecuteFn = Any


def row(**fields: Any) -> SimpleNamespace:
    return SimpleNamespace(**fields)


def recording_execute(
    responder: Any,
) -> Tuple[ExecuteFn, List[str]]:
    """Return an execute_query_func that records queries and delegates to responder."""
    queries: List[str] = []

    def execute(query: str, job_config: Any, context: str) -> list:
        queries.append(query)
        return responder(query)

    return execute, queries


def warnings_titled(report: BigQueryV2Report, title: str) -> List[Any]:
    return [w for w in report.warnings if w.title == title]


class TestPartitionRangeFromPartitionId:
    @pytest.mark.parametrize(
        "partition_id,upper",
        [
            ("2024", datetime(2025, 1, 1)),
            ("202403", datetime(2024, 4, 1)),
            ("20240315", datetime(2024, 3, 16)),
            ("2024031510", datetime(2024, 3, 15, 11)),
        ],
    )
    def test_each_granularity_yields_its_bucket(
        self, partition_id: str, upper: datetime
    ) -> None:
        lower, actual_upper = PartitionDiscovery.get_partition_range_from_partition_id(
            partition_id, None
        )
        assert actual_upper == upper
        assert lower < actual_upper

    def test_explicit_datetime_is_floored_to_partition_granularity(self) -> None:
        lower, upper = PartitionDiscovery.get_partition_range_from_partition_id(
            "202403", datetime(2024, 3, 20, 13, 45)
        )
        assert (lower, upper) == (datetime(2024, 3, 1), datetime(2024, 4, 1))

    def test_unrecognised_partition_id_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            PartitionDiscovery.get_partition_range_from_partition_id("2024-03", None)


class TestPartitionColumnsFromDdl:
    def test_missing_or_unpartitioned_ddl_yields_nothing(self) -> None:
        discovery = PartitionDiscovery(make_config())

        def execute(query: str, job_config: Any, context: str) -> list:
            raise AssertionError("no query expected")

        assert (
            discovery.get_partition_columns_from_ddl(
                make_table(ddl=None), PROJECT, SCHEMA, execute
            )
            == {}
        )
        assert (
            discovery.get_partition_columns_from_ddl(
                make_table(ddl="CREATE TABLE `p.d.t` (a INT64)"),
                PROJECT,
                SCHEMA,
                execute,
            )
            == {}
        )

    def test_range_bucket_partition_resolves_to_bucketed_column(self) -> None:
        discovery = PartitionDiscovery(make_config())
        execute, _ = recording_execute(
            lambda q: [row(column_name="user_id", data_type="INT64")]
        )
        table = make_table(
            ddl="CREATE TABLE `p.d.t` (user_id INT64) "
            "PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 100, 10))"
        )

        assert discovery.get_partition_columns_from_ddl(
            table, PROJECT, SCHEMA, execute
        ) == {"user_id": "INT64"}

    def test_unparseable_ddl_is_reported_not_raised(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)
        execute, _ = recording_execute(lambda q: [])

        result = discovery.get_partition_columns_from_ddl(
            make_table(ddl="THIS IS NOT SQL ((("), PROJECT, SCHEMA, execute
        )

        assert result == {}
        assert warnings_titled(report, "Partition columns from DDL failed")


class TestDiscoveryPriority:
    """Each strategy in get_required_partition_filters short-circuits the next."""

    def test_cached_metadata_skips_information_schema_columns_lookups(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)
        execute, queries = recording_execute(lambda q: [])
        cached = CachedPartitionMetadata(
            partition_columns=["dt", "dt"], column_types={"dt": "DATE"}
        )

        filters = discovery.get_required_partition_filters(
            make_table(), PROJECT, SCHEMA, execute, cached_partition_metadata=cached
        )

        # Nothing populated -> profiled unfiltered, but with an explicit warning.
        assert filters == []
        assert warnings_titled(report, "Profiled without a partition filter")
        assert not any("INFORMATION_SCHEMA.COLUMNS" in q for q in queries)

    def test_zero_scan_max_partition_id_wins_over_information_schema(self) -> None:
        class ZeroScan(PartitionDiscovery):
            def _get_partition_filters_from_max_partition_id(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                return ["`dt` = '2024-01-15'"]

        discovery = ZeroScan(make_config())
        execute, queries = recording_execute(lambda q: [])
        table = make_table(
            max_partition_id="20240115", partition_info=PartitionInfo(fields=("dt",))
        )

        filters = discovery.get_required_partition_filters(
            table, PROJECT, SCHEMA, execute
        )

        assert filters == ["`dt` = '2024-01-15'"]
        assert not any("INFORMATION_SCHEMA.PARTITIONS" in q for q in queries)

    def test_information_schema_filters_win_over_direct_table_query(self) -> None:
        class Spy(PartitionDiscovery):
            direct_called = False

            def _get_partition_filters_from_information_schema(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                return ["`dt` = '2024-01-15'"]

            def _find_real_partition_values(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                self.direct_called = True
                return ["`dt` = '1999-01-01'"]

        discovery = Spy(make_config())
        execute, _ = recording_execute(lambda q: [])
        table = make_table(partition_info=PartitionInfo(fields=("dt",)))

        filters = discovery.get_required_partition_filters(
            table, PROJECT, SCHEMA, execute
        )

        assert filters == ["`dt` = '2024-01-15'"]
        assert not discovery.direct_called

    def test_direct_query_and_sampling_are_tried_in_order(self) -> None:
        class DirectWins(PartitionDiscovery):
            def _find_real_partition_values(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                return ["`dt` = '2024-01-15'"]

        class SamplingWins(PartitionDiscovery):
            def _get_partitions_with_sampling(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                return ["`dt` = '2024-01-14'"]

        execute, _ = recording_execute(lambda q: [])
        table = make_table(partition_info=PartitionInfo(fields=("dt",)))

        assert DirectWins(make_config()).get_required_partition_filters(
            table, PROJECT, SCHEMA, execute
        ) == ["`dt` = '2024-01-15'"]
        assert SamplingWins(make_config()).get_required_partition_filters(
            table, PROJECT, SCHEMA, execute
        ) == ["`dt` = '2024-01-14'"]

    @pytest.mark.parametrize(
        "table",
        [
            make_table(
                name="requires_filter",
                partition_info=PartitionInfo(
                    fields=("dt",), require_partition_filter=True
                ),
            ),
            make_table(
                name="external",
                external=True,
                partition_info=PartitionInfo(fields=("dt",)),
            ),
        ],
    )
    def test_tables_that_cannot_be_scanned_unfiltered_are_skipped(
        self, table: BigqueryTable
    ) -> None:
        discovery = PartitionDiscovery(make_config())
        execute, _ = recording_execute(lambda q: [])

        assert (
            discovery.get_required_partition_filters(table, PROJECT, SCHEMA, execute)
            is None
        )

    def test_external_table_without_known_columns_uses_external_strategy(
        self,
    ) -> None:
        class ExternalAware(PartitionDiscovery):
            def _get_external_table_partition_filters(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                return ["`region` = 'eu'"]

        discovery = ExternalAware(make_config())
        execute, _ = recording_execute(lambda q: [])

        filters = discovery.get_required_partition_filters(
            make_table(external=True), PROJECT, SCHEMA, execute
        )

        assert filters == ["`region` = 'eu'"]


class TestPartitionDatetimeOverride:
    def test_composite_key_ignores_configured_partition_datetime(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(
            make_config(partition_datetime="2024-01-15T00:00:00"), report
        )

        result = discovery._get_partition_datetime_override_filters(
            make_table(), ["year", "month"], {"year": "INT64", "month": "INT64"}
        )

        assert result is None
        assert warnings_titled(report, "partition_datetime not applied")

    def test_non_temporal_column_ignores_configured_partition_datetime(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(
            make_config(partition_datetime="2024-01-15T00:00:00"), report
        )

        result = discovery._get_partition_datetime_override_filters(
            make_table(), ["bucket"], {"bucket": "INT64"}
        )

        assert result is None
        assert warnings_titled(report, "partition_datetime not applied")


class TestDirectTableQueryDiscovery:
    def test_composite_key_is_resolved_hierarchically_and_jointly(self) -> None:
        """A DATE column, year/month components and a non-date column: components
        are constrained by the resolved year, and the non-date column is picked within
        all earlier filters so the composite key actually co-occurs."""
        discovery = PartitionDiscovery(make_config())
        column_types = {
            "event_date": "DATE",
            "year": "INT64",
            "month": "INT64",
            "region": "STRING",
        }

        def respond(query: str) -> list:
            if "`event_date` as val" in query:
                return [row(val=date(2024, 3, 15), record_count=10)]
            if "`year` as val" in query:
                return [row(val=2024, record_count=10)]
            if "`month` as val" in query:
                assert "`year` = 2024" in query
                return [row(val=3, record_count=10)]
            if "`region` as val" in query:
                assert "`month` = 3" in query and "`event_date`" in query
                return [row(val="eu", record_count=7)]
            raise AssertionError(f"unexpected query {query}")

        execute, _ = recording_execute(respond)

        values = discovery._get_partition_info_from_table_query(
            make_table(partition_info=PartitionInfo(fields=("event_date",))),
            PROJECT,
            SCHEMA,
            ["event_date", "year", "month", "region"],
            execute,
            cached_metadata=CachedPartitionMetadata(
                partition_columns=[], column_types=column_types
            ),
        )

        assert values == {
            "event_date": date(2024, 3, 15),
            "year": 2024,
            "month": 3,
            "region": "eu",
        }

    def test_no_partition_columns_runs_no_queries(self) -> None:
        discovery = PartitionDiscovery(make_config())
        execute, queries = recording_execute(lambda q: [])

        assert (
            discovery._get_partition_info_from_table_query(
                make_table(), PROJECT, SCHEMA, [], execute
            )
            == {}
        )
        assert queries == []

    def test_incomplete_date_hierarchy_discards_all_values(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)

        def respond(query: str) -> list:
            if "`year` as val" in query:
                return [row(val=2024, record_count=1)]
            if "`month` as val" in query:
                return []
            return [row(val=date(2024, 3, 15), record_count=1)]

        execute, _ = recording_execute(respond)

        values = discovery._get_partition_info_from_table_query(
            make_table(),
            PROJECT,
            SCHEMA,
            ["event_date", "year", "month"],
            execute,
            cached_metadata=CachedPartitionMetadata(
                partition_columns=[],
                column_types={"event_date": "DATE", "year": "INT64", "month": "INT64"},
            ),
        )

        assert values == {}
        assert warnings_titled(report, "Incomplete composite partition key")

    def test_failing_or_empty_columns_are_dropped_with_warnings(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)

        def respond(query: str) -> list:
            if "`created_at`" in query:
                raise RuntimeError("permission denied")
            if "`region`" in query:
                return [row(val=None, record_count=0)]
            return []

        execute, _ = recording_execute(respond)

        values = discovery._get_partition_info_from_table_query(
            make_table(),
            PROJECT,
            SCHEMA,
            ["created_at", "region", "bad;name"],
            execute,
            cached_metadata=CachedPartitionMetadata(
                partition_columns=[],
                column_types={"created_at": "TIMESTAMP", "region": "STRING"},
            ),
        )

        assert values == {}
        assert warnings_titled(report, "Partition value discovery failed for column")
        assert warnings_titled(report, "Partition value discovery found no values")

    def test_component_query_failure_is_reported_and_yields_no_filter(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)

        def execute(query: str, job_config: Any, context: str) -> list:
            raise RuntimeError("timeout")

        filters = discovery._find_max_component_within_constraint(
            "year", TABLE_REF, [], execute, {}, {"year": "INT64"}, "table"
        )

        assert filters == []
        assert warnings_titled(report, "Partition value discovery failed for column")

    def test_partition_columns_from_table_info_include_resolved_columns(self) -> None:
        discovery = PartitionDiscovery(make_config())
        column = BigqueryColumn(
            name="dt",
            ordinal_position=1,
            is_nullable=False,
            data_type="DATE",
            comment=None,
            field_path="dt",
            is_partition_column=True,
            cluster_column_position=None,
        )
        table = make_table(
            partition_info=PartitionInfo(fields=("dt",), columns=(column,))
        )

        assert discovery._get_partition_columns_from_table_info(table) == ["dt"]


class TestValueFilter:
    def test_date_object_is_widened_to_its_day(self) -> None:
        discovery = PartitionDiscovery(make_config())
        filter_str = discovery._value_filter(
            make_table(), "dt", date(2024, 3, 15), "DATE"
        )
        assert "2024-03-15" in filter_str and "2024-03-16" in filter_str

    def test_non_temporal_value_on_temporal_column_falls_back_to_safe_filter(
        self,
    ) -> None:
        discovery = PartitionDiscovery(make_config())
        assert (
            discovery._value_filter(make_table(), "dt", 20240315, "DATE")
            == "`dt` = '2024-03-15'"
        )

    def test_invalid_iso_string_is_not_parsed(self) -> None:
        assert PartitionDiscovery._parse_iso_temporal("2024-13-45") is None


class TestStrategicDateCandidate:
    def _discovery(
        self, verify: Any, report: Optional[BigQueryV2Report] = None, **cfg: Any
    ) -> PartitionDiscovery:
        class Candidate(PartitionDiscovery):
            def _verify_partition_has_data(self, *args: Any, **kwargs: Any) -> bool:
                return verify(*args, **kwargs)

            def _enhance_partition_filters_with_actual_values(
                self, *args: Any, **kwargs: Any
            ) -> Optional[List[str]]:
                return None

        return Candidate(make_config(**cfg), report)

    def test_filters_are_built_per_column_kind(self) -> None:
        discovery = self._discovery(
            lambda *a, **k: True, fallback_partition_values={"shard": "s1"}
        )
        execute, _ = recording_execute(lambda q: [])
        test_date = datetime(2024, 3, 15, tzinfo=timezone.utc)

        filters = discovery._test_date_candidate(
            make_table(),
            PROJECT,
            SCHEMA,
            test_date,
            "today",
            ["event_date", "year", "shard", "region"],
            {"event_date": "STRING", "year": "INT64"},
            execute,
        )

        assert filters == [
            "`event_date` = '2024-03-15'",
            "`year` = 2024",
            "`shard` = 's1'",
            "`region` IS NOT NULL",
        ]

    def test_candidate_without_data_is_rejected(self) -> None:
        discovery = self._discovery(lambda *a, **k: False)
        execute, _ = recording_execute(lambda q: [])

        assert (
            discovery._test_date_candidate(
                make_table(),
                PROJECT,
                SCHEMA,
                datetime(2024, 3, 15, tzinfo=timezone.utc),
                "today",
                ["dt"],
                {"dt": "DATE"},
                execute,
            )
            is None
        )

    def test_verification_error_is_reported_not_raised(self) -> None:
        report = BigQueryV2Report()

        def boom(*args: Any, **kwargs: Any) -> bool:
            raise RuntimeError("quota exceeded")

        discovery = self._discovery(boom, report)
        execute, _ = recording_execute(lambda q: [])

        result = discovery._test_date_candidate(
            make_table(),
            PROJECT,
            SCHEMA,
            datetime(2024, 3, 15, tzinfo=timezone.utc),
            "today",
            ["dt"],
            {"dt": "DATE"},
            execute,
        )

        assert result is None
        assert warnings_titled(report, "Strategic date candidate failed")


class TestFallbackFilters:
    def test_unbuildable_discovered_value_is_dropped_with_warning(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)

        filters = discovery._filters_from_partition_values(
            make_table(),
            {"bucket": "not-a-number", "region": "eu"},
            {"bucket": "INT64", "region": "STRING"},
        )

        assert filters == ["`region` = 'eu'"]
        assert warnings_titled(report, "Partition filter dropped")

    def test_untyped_non_date_column_falls_back_to_full_scan_with_warning(
        self,
    ) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)

        filters = discovery._get_fallback_partition_filters(
            make_table(), PROJECT, SCHEMA, ["region"], {}
        )

        assert filters == ["`region` IS NOT NULL"]
        assert warnings_titled(report, "Partition discovery fell back to full scan")

    def test_fallback_filter_per_column_kind(self) -> None:
        discovery = PartitionDiscovery(make_config())
        fallback_date = datetime(2024, 3, 15, tzinfo=timezone.utc)
        guessed: List[str] = []

        typed_component = discovery._create_fallback_filter_for_column(
            make_table(), "year", fallback_date, "INT64", guessed
        )
        date_like_untyped = discovery._create_fallback_filter_for_column(
            make_table(), "event_date", fallback_date, "", guessed
        )
        unknown = discovery._create_fallback_filter_for_column(
            make_table(), "region", fallback_date, "", guessed
        )

        assert typed_component == "`year` = 2024"
        assert guessed == ["year"]
        assert date_like_untyped == "`event_date` IS NOT NULL"
        assert unknown == "`region` IS NOT NULL"

    def test_invalid_configured_fallback_degrades_to_is_not_null(self) -> None:
        discovery = PartitionDiscovery(
            make_config(fallback_partition_values={"bucket": "abc"})
        )

        filter_str = discovery._create_fallback_filter_for_column(
            make_table(), "bucket", datetime.now(timezone.utc), "INT64"
        )

        assert filter_str == "`bucket` IS NOT NULL"


class TestEnhanceWithActualValues:
    def test_placeholder_only_filters_cannot_be_enhanced(self) -> None:
        discovery = PartitionDiscovery(make_config())
        execute, queries = recording_execute(lambda q: [])

        result = discovery._enhance_partition_filters_with_actual_values(
            make_table(), PROJECT, SCHEMA, ["region"], ["`region` IS NOT NULL"], execute
        )

        assert result is None
        assert queries == []

    def test_unresolvable_non_date_columns_keep_placeholder_and_warn(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)
        column_types = {"dt": "DATE", "region": "STRING", "shard": "STRING"}

        def respond(query: str) -> list:
            if "`region`" in query:
                return []
            if "`shard`" in query:
                raise RuntimeError("timeout")
            raise AssertionError(f"unexpected query {query}")

        execute, _ = recording_execute(respond)

        result = discovery._enhance_partition_filters_with_actual_values(
            make_table(),
            PROJECT,
            SCHEMA,
            ["dt", "region", "shard"],
            ["`dt` = '2024-03-15'", "`region` IS NOT NULL", "`shard` IS NOT NULL"],
            execute,
            cached_metadata=CachedPartitionMetadata(
                partition_columns=[], column_types=column_types
            ),
        )

        assert result == [
            "`dt` = '2024-03-15'",
            "`region` IS NOT NULL",
            "`shard` IS NOT NULL",
        ]
        warning = warnings_titled(report, "Partition discovery fell back to full scan")
        assert warning and "region" in str(warning[0].context)

    def test_invalid_table_reference_is_reported_as_full_scan_fallback(self) -> None:
        report = BigQueryV2Report()
        discovery = PartitionDiscovery(make_config(), report)
        execute, _ = recording_execute(lambda q: [])

        result = discovery._enhance_partition_filters_with_actual_values(
            make_table(name="bad name"),
            PROJECT,
            SCHEMA,
            ["dt"],
            ["`dt` = '2024-03-15'"],
            execute,
        )

        assert result is None
        assert warnings_titled(report, "Partition discovery fell back to full scan")


def test_partition_column_types_prefer_cached_values() -> None:
    discovery = PartitionDiscovery(make_config())
    execute, queries = recording_execute(lambda q: [])
    cached = CachedPartitionMetadata(
        partition_columns=["dt"], column_types={"dt": "DATE", "x": "STRING"}
    )

    types: Dict[str, str] = discovery._get_partition_column_types(
        make_table(), PROJECT, SCHEMA, ["dt"], execute, cached_metadata=cached
    )

    assert types == {"dt": "DATE"}
    assert queries == []
