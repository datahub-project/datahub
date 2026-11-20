"""Doc coverage for Snowflake interactive table ingestion (item 5)."""

from pathlib import Path

_HERE = Path(__file__).resolve()
_METADATA_INGESTION_DIR = _HERE.parents[3]
_REPO_ROOT = _HERE.parents[4]

_README = (_METADATA_INGESTION_DIR / "docs/sources/snowflake/README.md").read_text(
    encoding="utf-8"
)
_SNOWFLAKE_PRE = (
    _METADATA_INGESTION_DIR / "docs/sources/snowflake/snowflake_pre.md"
).read_text(encoding="utf-8")
_SNOWFLAKE_POST = (
    _METADATA_INGESTION_DIR / "docs/sources/snowflake/snowflake_post.md"
).read_text(encoding="utf-8")
_BREAKING_CHANGES = (_REPO_ROOT / "docs/how/breaking-changes.md").read_text(
    encoding="utf-8"
)


def test_readme_concept_row_for_interactive_table() -> None:
    assert "| Interactive Table" in _README
    assert "INTERACTIVE TABLE" in _README
    assert "target lag" in _README.lower()


def test_readme_lineage_row_names_interactive_table_definitions() -> None:
    assert "interactive table definitions" in _README


def test_snowflake_pre_grants_select_on_interactive_tables() -> None:
    assert "grant select on all interactive tables" in _SNOWFLAKE_PRE
    assert "grant select on future interactive tables" in _SNOWFLAKE_PRE


def test_snowflake_pre_show_interactive_tables_select_note() -> None:
    assert "SHOW INTERACTIVE TABLES" in _SNOWFLAKE_PRE
    assert "0 rows" in _SNOWFLAKE_PRE


def test_snowflake_post_features_list_interactive_tables() -> None:
    assert (
        "This includes dynamic tables, interactive tables, semantic views"
        in _SNOWFLAKE_POST
    )


def test_breaking_changes_default_table_types_includes_interactive() -> None:
    next_block = _BREAKING_CHANGES.split("## Next\n", 1)[1].split(
        "\n## Unreleased\n", 1
    )[0]
    assert "INTERACTIVE TABLE" in next_block
    assert "table_types" in next_block
