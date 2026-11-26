"""SnowflakeTagExtractor: per-domain lookups and structured property templates.

Inheritance, cache loading and cache-load failures are covered by
test_snowflake_tag_inheritance.py (TestSnowflakeTagExtractorRouting).
"""

from typing import List
from unittest.mock import create_autospec

import pytest

from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeTag,
    _SnowflakeTagCache,
)
from datahub.ingestion.source.snowflake.snowflake_tag import SnowflakeTagExtractor
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from tests.unit.snowflake.conftest import (
    DB,
    SCHEMA,
    as_mock,
    log_keys,
    make_v2_config,
    tag,
)


def _cache() -> _SnowflakeTagCache:
    cache = _SnowflakeTagCache()
    cache.add_database_tag(DB, tag("DB_TAG"))
    cache.add_schema_tag(SCHEMA, DB, tag("SCHEMA_TAG"))
    cache.add_table_tag("T", SCHEMA, DB, tag("TABLE_TAG"))
    return cache


def _make_extractor(**config_overrides: object) -> SnowflakeTagExtractor:
    config = make_v2_config(**{"extract_tags": "without_lineage", **config_overrides})
    report = SnowflakeV2Report()
    data_dictionary = create_autospec(SnowflakeDataDictionary, instance=True)
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


def test_column_domain_is_not_an_object_level_lookup() -> None:
    extractor = _make_extractor()

    with pytest.raises(ValueError):
        extractor.get_tags_on_object(SnowflakeObjectDomain.COLUMN, DB, SCHEMA, "T")


def test_structured_property_templates_follow_the_property_pattern() -> None:
    extractor = _make_extractor(
        extract_tags_as_structured_properties=True,
        structured_property_pattern={"deny": [".*\\.internal_.*"]},
    )
    as_mock(extractor.data_dictionary).get_all_tags.return_value = [
        tag("PUBLIC_TAG"),
        tag("INTERNAL_TAG"),
    ]

    workunits = list(extractor.create_structured_property_templates())

    assert [wu.get_urn() for wu in workunits] == [
        f"urn:li:structuredProperty:snowflake.{DB.lower()}.{SCHEMA.lower()}.public_tag"
    ]
    assert extractor.report.num_structured_property_templates_created == 1


def test_structured_property_templates_skip_when_tags_cannot_be_listed() -> None:
    extractor = _make_extractor(extract_tags_as_structured_properties=True)
    as_mock(extractor.data_dictionary).get_all_tags.side_effect = RuntimeError("denied")

    assert list(extractor.create_structured_property_templates()) == []
    assert "Failed to create structured property templates" in log_keys(
        extractor.report.warnings
    )
