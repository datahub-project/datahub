from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.tableau.tableau import TableauSiteSource

SNOWFLAKE_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"


def _source(schema_info: Optional[Dict[str, str]]) -> Any:
    """A source stub exercising only the column-name resolution path.

    Tableau's constructor wants a live server, so the two collaborators the
    method actually uses are supplied directly.
    """
    source = MagicMock(spec=TableauSiteSource)
    resolver = None
    if schema_info is not None:
        resolver = MagicMock()
        resolver.resolve_urn.return_value = (SNOWFLAKE_URN, schema_info)
    source._upstream_schema_resolver = resolver
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

    def test_spaces_become_underscores_before_matching(self) -> None:
        assert _resolve({"CUSTOMER_ID": "VARCHAR"}, "CUSTOMER ID") == "CUSTOMER_ID"

    def test_falls_back_to_lowercase_without_a_graph(self) -> None:
        # No graph configured: keep the previous behaviour rather than guess.
        assert _resolve(None, "CUSTOMER ID") == "customer_id"

    def test_falls_back_to_lowercase_when_not_yet_ingested(self) -> None:
        # The dataset resolves to no schema, so there is nothing to match against.
        assert _resolve({}, "CUSTOMER_ID") == "customer_id"

    def test_unknown_column_keeps_the_lowercased_name(self) -> None:
        # Schema is known but has no such column; match_columns_to_schema returns
        # the input unchanged, so it must still be normalized.
        assert _resolve({"OTHER": "VARCHAR"}, "CUSTOMER_ID") == "CUSTOMER_ID"
