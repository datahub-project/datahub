"""Tests for _generate_fully_qualified_name and _platform_names_have_2_parts."""

import pytest

from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.lookml_config import LookMLSourceReport
from datahub.ingestion.source.looker.view_upstream import _generate_fully_qualified_name


@pytest.fixture
def reporter() -> LookMLSourceReport:
    return LookMLSourceReport()


def _conn(
    platform: str, default_db: str, default_schema: str
) -> LookerConnectionDefinition:
    return LookerConnectionDefinition(
        platform=platform,
        default_db=default_db,
        default_schema=default_schema,
    )


class TestGenerateFullyQualifiedNameGlue:
    """AWS Glue uses 2-part naming (database.table), not 3-part (database.schema.table)."""

    def test_bare_table_name_expands_to_two_parts(
        self, reporter: LookMLSourceReport
    ) -> None:
        """A bare table name should become default_db.table, not default_db.default_schema.table."""
        conn = _conn("glue", "my_catalog", "my_schema")
        result = _generate_fully_qualified_name("my_table", conn, reporter, "view_name")
        assert result == "my_catalog.my_table"

    def test_two_part_name_is_unchanged(self, reporter: LookMLSourceReport) -> None:
        """A 2-part name already matches the Glue convention and should be returned as-is."""
        conn = _conn("glue", "my_catalog", "my_schema")
        result = _generate_fully_qualified_name(
            "my_db.my_table", conn, reporter, "view_name"
        )
        assert result == "my_db.my_table"

    def test_three_part_name_drops_first_level(
        self, reporter: LookMLSourceReport
    ) -> None:
        """A 3-part name (db.schema.table) should be reduced to 2-part (schema.table)."""
        conn = _conn("glue", "my_catalog", "my_schema")
        result = _generate_fully_qualified_name(
            "my_db.my_schema.my_table", conn, reporter, "view_name"
        )
        assert result == "my_schema.my_table"


class TestGenerateFullyQualifiedNameExistingPlatforms:
    """Regression: existing 2-part and 3-part platforms are unaffected."""

    @pytest.mark.parametrize("platform", ["hive", "mysql", "athena"])
    def test_known_two_part_platforms_bare_table(
        self, platform: str, reporter: LookMLSourceReport
    ) -> None:
        conn = _conn(platform, "my_db", "my_schema")
        result = _generate_fully_qualified_name("my_table", conn, reporter, "view_name")
        assert result == "my_db.my_table"

    def test_snowflake_three_part_bare_table(
        self, reporter: LookMLSourceReport
    ) -> None:
        conn = _conn("snowflake", "my_db", "my_schema")
        result = _generate_fully_qualified_name("my_table", conn, reporter, "view_name")
        assert result == "my_db.my_schema.my_table"
