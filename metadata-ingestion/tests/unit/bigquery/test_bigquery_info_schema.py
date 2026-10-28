"""InfoSchemaQueries: deriving partition filters and column types from
INFORMATION_SCHEMA.PARTITIONS / COLUMNS."""

from types import SimpleNamespace
from typing import Any, List

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.info_schema import (
    InfoSchemaQueries,
)
from tests.unit.bigquery.test_bigquery_profiling import make_table

PROJECT = "test-project-123456"
SCHEMA = "ds"


def partition_rows(*ids: Any) -> List[SimpleNamespace]:
    return [SimpleNamespace(partition_id=i) for i in ids]


def verify_all(*args: Any, **kwargs: Any) -> bool:
    return True


def verify_none(*args: Any, **kwargs: Any) -> bool:
    return False


class TestPartitionColumnTypes:
    def test_no_columns_runs_no_query(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            raise AssertionError("no query expected")

        assert (
            InfoSchemaQueries(None).get_partition_column_types(
                make_table(), PROJECT, SCHEMA, [], execute
            )
            == {}
        )

    def test_only_invalid_column_names_skips_query(self) -> None:
        queries: List[str] = []

        def execute(query: str, job_config: Any, context: str) -> list:
            queries.append(query)
            return []

        types = InfoSchemaQueries(None).get_partition_column_types(
            make_table(), PROJECT, SCHEMA, ["bad col"], execute
        )

        assert types == {}
        assert queries == []


class TestPartitionFiltersFromInformationSchema:
    def test_no_required_columns_means_unfiltered(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            raise AssertionError("no query expected")

        assert (
            InfoSchemaQueries(None).get_partition_filters_from_information_schema(
                make_table(), PROJECT, SCHEMA, [], execute, verify_all, {}
            )
            == []
        )

    def test_no_populated_partitions_yields_none(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            return []

        assert (
            InfoSchemaQueries(None).get_partition_filters_from_information_schema(
                make_table(),
                PROJECT,
                SCHEMA,
                ["dt"],
                execute,
                verify_all,
                {"dt": "DATE"},
            )
            is None
        )

    def test_first_partition_with_data_wins(self) -> None:
        seen: List[List[str]] = []

        def verify(table: Any, p: str, s: str, filters: List[str], fn: Any) -> bool:
            seen.append(filters)
            return len(seen) == 2

        def execute(query: str, job_config: Any, context: str) -> list:
            return partition_rows("20240315", "20240314", "20240313")

        filters = InfoSchemaQueries(None).get_partition_filters_from_information_schema(
            make_table(), PROJECT, SCHEMA, ["dt"], execute, verify, {"dt": "DATE"}
        )

        assert filters == ["`dt` = '2024-03-14'"]
        assert len(seen) == 2

    def test_unconvertible_partition_ids_are_summarised_in_one_warning(self) -> None:
        report = BigQueryV2Report()

        def execute(query: str, job_config: Any, context: str) -> list:
            return partition_rows("garbage", "more-garbage")

        filters = InfoSchemaQueries(
            report
        ).get_partition_filters_from_information_schema(
            make_table(), PROJECT, SCHEMA, ["dt"], execute, verify_all, {"dt": "DATE"}
        )

        assert filters is None
        failed = [
            w for w in report.warnings if w.title == "Partition filter discovery failed"
        ]
        assert len(failed) == 1 and "2 partition id(s)" in str(failed[0].context)

    def test_query_failure_is_reported_not_raised(self) -> None:
        report = BigQueryV2Report()

        def execute(query: str, job_config: Any, context: str) -> list:
            raise RuntimeError("INFORMATION_SCHEMA.PARTITIONS not available")

        filters = InfoSchemaQueries(
            report
        ).get_partition_filters_from_information_schema(
            make_table(), PROJECT, SCHEMA, ["dt"], execute, verify_all, {"dt": "DATE"}
        )

        assert filters is None
        assert any(
            w.title == "Partition filter discovery failed" for w in report.warnings
        )


class TestRangePartitions:
    def _table(self) -> Any:
        return make_table(
            partition_info=PartitionInfo(fields=("bucket",), type=RANGE_PARTITION_NAME)
        )

    def test_max_bucket_falls_back_to_fetched_rows_when_max_query_fails(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            if "SAFE_CAST(partition_id AS INT64) DESC" in query:
                raise RuntimeError("quota")
            return partition_rows("100", None, "300", "abc", "200")

        filters = InfoSchemaQueries(None).get_partition_filters_from_information_schema(
            self._table(),
            PROJECT,
            SCHEMA,
            ["bucket"],
            execute,
            verify_all,
            {"bucket": "INT64"},
        )

        assert filters == ["`bucket` >= 300"]

    def test_no_numeric_bucket_yields_none(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            if "SAFE_CAST(partition_id AS INT64) DESC" in query:
                return []
            return partition_rows("abc")

        assert (
            InfoSchemaQueries(None).get_partition_filters_from_information_schema(
                self._table(),
                PROJECT,
                SCHEMA,
                ["bucket"],
                execute,
                verify_all,
                {"bucket": "INT64"},
            )
            is None
        )

    def test_empty_top_bucket_yields_none(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            if "SAFE_CAST(partition_id AS INT64) DESC" in query:
                return partition_rows("300")
            return partition_rows("300")

        assert (
            InfoSchemaQueries(None).get_partition_filters_from_information_schema(
                self._table(),
                PROJECT,
                SCHEMA,
                ["bucket"],
                execute,
                verify_none,
                {"bucket": "INT64"},
            )
            is None
        )

    def test_composite_range_key_cannot_be_expressed(self) -> None:
        def execute(query: str, job_config: Any, context: str) -> list:
            return partition_rows("300")

        # convert_partition_id_to_filters returns None for a multi-column key, so no
        # filter can be built from the max bucket alone.
        assert (
            InfoSchemaQueries(None).get_partition_filters_from_information_schema(
                self._table(),
                PROJECT,
                SCHEMA,
                ["bucket", "region"],
                execute,
                verify_all,
                {},
            )
            is None
        )
