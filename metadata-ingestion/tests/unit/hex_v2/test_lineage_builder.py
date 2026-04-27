import pytest

from datahub.ingestion.source.hex_v2.lineage_builder import (
    HexLineageBuilder,
    LineageBuilderReport,
)
from datahub.ingestion.source.hex_v2.model import DataConnection, SqlCell


def _make_connection(id: str, conn_type: str, name: str = "Test") -> DataConnection:
    return DataConnection(id=id, name=name, connection_type=conn_type)


def _make_cell(
    sql: str, connection_id: str, cell_id: str = "cell-1", label: str = "Test"
) -> SqlCell:
    return SqlCell(
        cell_id=cell_id,
        cell_label=label,
        sql_source=sql,
        data_connection_id=connection_id,
    )


@pytest.fixture
def report() -> LineageBuilderReport:
    return LineageBuilderReport()


@pytest.fixture
def builder(report: LineageBuilderReport) -> HexLineageBuilder:
    connections = {
        "conn-sf": _make_connection("conn-sf", "snowflake", "Analytics Hub"),
        "conn-bq": _make_connection("conn-bq", "bigquery", "BigQuery Prod"),
    }
    return HexLineageBuilder(
        connections=connections,
        platform_instance=None,
        env="PROD",
        sql_parsing_platform_default="snowflake",
        report=report,
    )


class TestHexLineageBuilder:
    def test_extracts_upstream_from_sql(
        self, builder: HexLineageBuilder, report: LineageBuilderReport
    ) -> None:
        cell = _make_cell(
            "SELECT * FROM analytics_hub.public_core.dim_customer",
            "conn-sf",
        )
        urns = builder.build_upstream_urns([cell])

        assert len(urns) == 1
        assert "dim_customer" in urns[0]
        assert "snowflake" in urns[0]
        assert report.sql_cells_succeeded == 1
        assert report.upstream_datasets_found == 1

    def test_deduplicates_same_table_across_cells(
        self, builder: HexLineageBuilder
    ) -> None:
        sql = "SELECT * FROM db.schema.orders"
        cells = [
            _make_cell(sql, "conn-sf", cell_id="c1"),
            _make_cell(sql, "conn-sf", cell_id="c2"),
        ]
        urns = builder.build_upstream_urns(cells)

        assert len(urns) == 1

    def test_multiple_tables_in_one_query(
        self, builder: HexLineageBuilder, report: LineageBuilderReport
    ) -> None:
        cell = _make_cell(
            "SELECT a.*, b.name FROM db.s.table_a a JOIN db.s.table_b b ON a.id = b.id",
            "conn-sf",
        )
        urns = builder.build_upstream_urns([cell])

        assert len(urns) == 2
        assert report.upstream_datasets_found == 2

    def test_unknown_connection_id_uses_default_platform(
        self, builder: HexLineageBuilder, report: LineageBuilderReport
    ) -> None:
        cell = _make_cell(
            "SELECT * FROM db.s.tbl",
            "conn-unknown",
        )
        urns = builder.build_upstream_urns([cell])

        # Should still produce a URN using the default platform
        assert len(urns) == 1
        assert "snowflake" in urns[0]
        assert "conn-unknown" in report.unknown_connection_ids

    def test_unknown_connection_type_uses_default(
        self, builder: HexLineageBuilder, report: LineageBuilderReport
    ) -> None:
        connections = {
            "conn-exotic": _make_connection("conn-exotic", "exotic_db"),
        }
        b = HexLineageBuilder(
            connections=connections,
            platform_instance=None,
            env="PROD",
            sql_parsing_platform_default="snowflake",
            report=report,
        )
        cell = _make_cell("SELECT * FROM db.s.tbl", "conn-exotic")
        urns = b.build_upstream_urns([cell])

        assert len(urns) == 1
        assert "exotic_db" in report.unknown_connection_types

    def test_sql_parse_failure_skips_gracefully(
        self, builder: HexLineageBuilder, report: LineageBuilderReport
    ) -> None:
        cell = _make_cell("THIS IS NOT SQL !!!###", "conn-sf")
        urns = builder.build_upstream_urns([cell])

        # No crash regardless of whether parse succeeds or fails
        assert urns == []

    def test_bigquery_connection_produces_bq_urn(
        self, builder: HexLineageBuilder
    ) -> None:
        cell = _make_cell(
            "SELECT * FROM `my-project.my_dataset.my_table`",
            "conn-bq",
        )
        urns = builder.build_upstream_urns([cell])

        assert any("bigquery" in u for u in urns)

    def test_empty_cells_returns_empty(self, builder: HexLineageBuilder) -> None:
        urns = builder.build_upstream_urns([])

        assert urns == []

    def test_upstream_datasets_found_accumulates_across_calls(
        self, builder: HexLineageBuilder, report: LineageBuilderReport
    ) -> None:
        # Each call to build_upstream_urns must ADD to the running total,
        # not overwrite it. This protects against = vs += regression.
        cell_a = _make_cell("SELECT * FROM db.s.table_a", "conn-sf", cell_id="c1")
        cell_b = _make_cell("SELECT * FROM db.s.table_b", "conn-sf", cell_id="c2")

        builder.build_upstream_urns([cell_a])  # first project: 1 upstream
        builder.build_upstream_urns([cell_b])  # second project: 1 upstream

        assert report.upstream_datasets_found == 2  # must be sum, not last value

    def test_platform_instance_encoded_in_urn(
        self, report: LineageBuilderReport
    ) -> None:
        b = HexLineageBuilder(
            connections={"conn-sf": _make_connection("conn-sf", "snowflake")},
            platform_instance="my-instance",
            env="PROD",
            sql_parsing_platform_default="snowflake",
            report=report,
        )
        cell = _make_cell("SELECT * FROM db.s.tbl", "conn-sf")
        urns = b.build_upstream_urns([cell])

        assert any("my-instance" in u for u in urns)
