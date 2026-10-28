"""SnowflakeTagExtractor: object-level lookups and structured property templates."""

from typing import List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTag,
    _SnowflakeTagCache,
)
from datahub.ingestion.source.snowflake.snowflake_tag import SnowflakeTagExtractor
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)

DB = "MY_DB"
SCHEMA = "MY_SCHEMA"


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _tag(name: str) -> SnowflakeTag:
    return SnowflakeTag(database=DB, schema=SCHEMA, name=name, value="v")


def _cache() -> _SnowflakeTagCache:
    cache = _SnowflakeTagCache()
    cache.add_database_tag(DB, _tag("DB_TAG"))
    cache.add_schema_tag(SCHEMA, DB, _tag("SCHEMA_TAG"))
    cache.add_table_tag("T", SCHEMA, DB, _tag("TABLE_TAG"))
    return cache


def _make_extractor(**config_overrides: object) -> SnowflakeTagExtractor:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "extract_tags": "without_lineage",
            **config_overrides,
        }
    )
    report = SnowflakeV2Report()
    data_dictionary = MagicMock()
    data_dictionary.get_tags_for_database_without_propagation.return_value = _cache()
    return SnowflakeTagExtractor(
        config,
        data_dictionary,
        report,
        SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
    )


def _names(tags: List[SnowflakeTag]) -> List[str]:
    return sorted(t.name for t in tags)


def test_tags_are_looked_up_per_domain_without_inheritance() -> None:
    extractor = _make_extractor()

    assert _names(extractor.get_tags_on_object(SnowflakeObjectDomain.DATABASE, DB)) == [
        "DB_TAG"
    ]
    assert _names(
        extractor.get_tags_on_object(SnowflakeObjectDomain.SCHEMA, DB, SCHEMA)
    ) == ["SCHEMA_TAG"]
    assert _names(
        extractor.get_tags_on_object(SnowflakeObjectDomain.TABLE, DB, SCHEMA, "T")
    ) == ["TABLE_TAG"]
    # One account_usage query per database, however many objects are asked about.
    _as_mock(
        extractor.data_dictionary
    ).get_tags_for_database_without_propagation.assert_called_once()


def test_with_lineage_mode_inherits_tags_from_parents() -> None:
    extractor = _make_extractor(extract_tags="with_lineage")

    schema_tags = extractor.get_tags_on_object(SnowflakeObjectDomain.SCHEMA, DB, SCHEMA)
    table_tags = extractor.get_tags_on_object(
        SnowflakeObjectDomain.TABLE, DB, SCHEMA, "T"
    )

    assert _names(schema_tags) == ["DB_TAG", "SCHEMA_TAG"]
    assert _names(table_tags) == ["DB_TAG", "SCHEMA_TAG", "TABLE_TAG"]
    assert {t.name for t in table_tags if t.is_inherited} == {"DB_TAG", "SCHEMA_TAG"}


def test_column_domain_is_not_an_object_level_lookup() -> None:
    extractor = _make_extractor()

    with pytest.raises(ValueError):
        extractor.get_tags_on_object(SnowflakeObjectDomain.COLUMN, DB, SCHEMA, "T")


def test_tag_cache_load_failure_disables_tags_for_that_database_only() -> None:
    extractor = _make_extractor()
    _as_mock(
        extractor.data_dictionary
    ).get_tags_for_database_without_propagation.side_effect = RuntimeError(
        "TAG_REFERENCES unavailable"
    )

    assert extractor.get_tags_on_object(SnowflakeObjectDomain.DATABASE, DB) == []
    assert extractor.get_tags_on_object(SnowflakeObjectDomain.DATABASE, DB) == []
    assert len(extractor.report.warnings) == 1  # not retried per object


def test_structured_property_templates_follow_the_property_pattern() -> None:
    extractor = _make_extractor(
        extract_tags_as_structured_properties=True,
        structured_property_pattern={"deny": [".*\\.internal_.*"]},
    )
    _as_mock(extractor.data_dictionary).get_all_tags.return_value = [
        _tag("PUBLIC_TAG"),
        _tag("INTERNAL_TAG"),
    ]

    workunits = list(extractor.create_structured_property_templates())

    assert [wu.get_urn() for wu in workunits] == [
        f"urn:li:structuredProperty:snowflake.{DB.lower()}.{SCHEMA.lower()}.public_tag"
    ]
    assert extractor.report.num_structured_property_templates_created == 1


def test_structured_property_templates_skip_when_tags_cannot_be_listed() -> None:
    extractor = _make_extractor(extract_tags_as_structured_properties=True)
    _as_mock(extractor.data_dictionary).get_all_tags.side_effect = RuntimeError(
        "denied"
    )

    assert list(extractor.create_structured_property_templates()) == []
    assert [w.title for w in extractor.report.warnings] == [
        "Failed to create structured property templates"
    ]
