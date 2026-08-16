from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.tableau.tableau import TableauSiteSource

SNOWFLAKE_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"


def _source(
    schema_info: Optional[Dict[str, str]], resolve_error: Optional[Exception] = None
) -> Any:
    """A source stub exercising only the column-name resolution path.

    Tableau's constructor wants a live server, so the collaborators the method
    actually uses are supplied directly. `_ingested_schema` is bound to the real
    implementation so its error handling is under test rather than mocked away.
    """
    source = MagicMock(spec=TableauSiteSource)
    # spec= omits instance attributes, and the failure path reports through it.
    source.report = MagicMock()
    resolver = None
    if schema_info is not None or resolve_error is not None:
        resolver = MagicMock()
        if resolve_error is not None:
            resolver.resolve_urn.side_effect = resolve_error
        else:
            resolver.resolve_urn.return_value = (SNOWFLAKE_URN, schema_info)
    source._upstream_schema_resolver = resolver
    source._failed_schema_lookups = set()
    source._ingested_schema = lambda urn: TableauSiteSource._ingested_schema(
        source, urn
    )
    return source


def _resolve(schema_info: Optional[Dict[str, str]], name: str) -> str:
    return TableauSiteSource._match_snowflake_column_name(
        _source(schema_info), SNOWFLAKE_URN, name
    )


class TestSnowflakeColumnNameMatching:
    @pytest.mark.parametrize(
        ("ingested_field_path", "tableau_name"),
        [
            # The Snowflake source lowercased its field paths.
            ("customer_id", "CUSTOMER_ID"),
            # ...or preserved the warehouse's casing.
            ("CUSTOMER_ID", "CUSTOMER_ID"),
            # ...or preserved a quoted mixed-case name.
            ("MixedCol", "MixedCol"),
        ],
    )
    def test_adopts_the_ingested_field_path(
        self, ingested_field_path: str, tableau_name: str
    ) -> None:
        # Whatever casing the Snowflake source chose, lineage has to point at it.
        assert (
            _resolve({ingested_field_path: "VARCHAR"}, tableau_name)
            == ingested_field_path
        )

    def test_a_case_only_pair_resolves_to_the_column_asked_for(self) -> None:
        # The schema shape preserve_column_case exists to produce: `col` and `COL`
        # are two real columns. Folding before matching collapses them into one
        # index entry, which silently hands a column its sibling's lineage.
        # Both members are exercised -- asking only for the survivor of the
        # collapse passes either way.
        schema = {"col": "VARCHAR", "COL": "VARCHAR"}

        assert _resolve(schema, "col") == "col"
        assert _resolve(schema, "COL") == "COL"

    def test_spaces_become_underscores_before_matching(self) -> None:
        assert _resolve({"CUSTOMER_ID": "VARCHAR"}, "CUSTOMER ID") == "CUSTOMER_ID"

    def test_falls_back_to_lowercase_without_a_graph(self) -> None:
        # No graph configured: keep the previous behaviour rather than guess.
        assert _resolve(None, "CUSTOMER ID") == "customer_id"

    def test_falls_back_to_lowercase_when_not_yet_ingested(self) -> None:
        # The dataset resolves to no schema, so there is nothing to match against.
        assert _resolve({}, "CUSTOMER_ID") == "customer_id"

    def test_column_absent_from_a_known_schema_falls_back(self) -> None:
        # A stale or partial schema must not produce warehouse casing: the column
        # is not there to match, so this is the same unknown case as no schema.
        assert _resolve({"OTHER": "VARCHAR"}, "CUSTOMER_ID") == "customer_id"

    def test_graph_errors_do_not_abort_the_run(self) -> None:
        # This path used to be a local string rewrite. A failing graph call must
        # degrade to that, not propagate out of the site ingest loop.
        source = _source(None, resolve_error=RuntimeError("gms timeout"))

        result = TableauSiteSource._match_snowflake_column_name(
            source, SNOWFLAKE_URN, "CUSTOMER ID"
        )

        assert result == "customer_id"
        assert source.report.warning.called

    def test_a_failed_lookup_is_not_retried_per_column(self) -> None:
        # SchemaResolver caches hits and misses but not exceptions, and this runs
        # once per column — so a GMS outage would otherwise re-request and re-warn
        # for every column on the dataset.
        source = _source(None, resolve_error=RuntimeError("gms timeout"))

        for name in ("CUSTOMER_ID", "AMOUNT", "ORDER_DATE"):
            TableauSiteSource._match_snowflake_column_name(source, SNOWFLAKE_URN, name)

        assert source._upstream_schema_resolver.resolve_urn.call_count == 1
        assert source.report.warning.call_count == 1
