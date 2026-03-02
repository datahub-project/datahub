from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTag,
    _SnowflakeTagCache,
)


def _tag(db: str, schema: str, name: str, value: str) -> SnowflakeTag:
    return SnowflakeTag(database=db, schema=schema, name=name, value=value)


def _build_cache() -> _SnowflakeTagCache:
    """Build a cache with tags at every level to test inheritance."""
    cache = _SnowflakeTagCache()

    # Database-level tag
    cache.add_database_tag("DB", _tag("TAGS_DB", "TAGS_SCH", "env", "production"))

    # Schema-level tag
    cache.add_schema_tag("SCH", "DB", _tag("TAGS_DB", "TAGS_SCH", "team", "data-eng"))

    # Table-level tags
    cache.add_table_tag("TBL", "SCH", "DB", _tag("TAGS_DB", "TAGS_SCH", "pii", "true"))
    # Override: same tag name as database, different value
    cache.add_table_tag(
        "TBL", "SCH", "DB", _tag("TAGS_DB", "TAGS_SCH", "env", "staging")
    )

    # Column-level tag
    cache.add_column_tag(
        "COL", "TBL", "SCH", "DB", _tag("TAGS_DB", "TAGS_SCH", "sensitivity", "high")
    )

    return cache


class TestSnowflakeTagIsInherited:
    def test_default_is_not_inherited(self):
        tag = _tag("D", "S", "n", "v")
        assert tag.is_inherited is False
        assert tag.inherited_from is None

    def test_as_inherited_returns_copy(self):
        tag = _tag("D", "S", "n", "v")
        inherited = tag.as_inherited(SnowflakeObjectDomain.DATABASE)
        assert inherited.is_inherited is True
        assert inherited.inherited_from == SnowflakeObjectDomain.DATABASE
        assert tag.is_inherited is False  # original unchanged
        assert tag.inherited_from is None
        assert inherited.value == tag.value
        assert inherited.name == tag.name


class TestDeduplicateTags:
    def test_empty(self):
        assert _SnowflakeTagCache._deduplicate_tags([]) == []

    def test_no_duplicates(self):
        tags = [
            _tag("D", "S", "a", "1"),
            _tag("D", "S", "b", "2"),
        ]
        assert _SnowflakeTagCache._deduplicate_tags(tags) == tags

    def test_direct_beats_inherited(self):
        """Direct tag should override inherited one regardless of order."""
        direct = _tag("D", "S", "env", "staging")
        inherited = _tag("D", "S", "env", "production").as_inherited(
            SnowflakeObjectDomain.DATABASE
        )
        # inherited listed first, but direct should still win
        result = _SnowflakeTagCache._deduplicate_tags([inherited, direct])
        assert len(result) == 1
        assert result[0].value == "staging"
        assert result[0].is_inherited is False

    def test_direct_beats_inherited_when_direct_first(self):
        direct = _tag("D", "S", "env", "staging")
        inherited = _tag("D", "S", "env", "production").as_inherited(
            SnowflakeObjectDomain.DATABASE
        )
        result = _SnowflakeTagCache._deduplicate_tags([direct, inherited])
        assert len(result) == 1
        assert result[0].value == "staging"

    def test_two_inherited_keeps_first(self):
        """When both are inherited, first occurrence wins."""
        a = _tag("D", "S", "env", "from-schema").as_inherited(
            SnowflakeObjectDomain.SCHEMA
        )
        b = _tag("D", "S", "env", "from-database").as_inherited(
            SnowflakeObjectDomain.DATABASE
        )
        result = _SnowflakeTagCache._deduplicate_tags([a, b])
        assert len(result) == 1
        assert result[0].value == "from-schema"

    def test_different_tag_names_preserved(self):
        tags = [
            _tag("D", "S", "env", "staging"),
            _tag("D", "S", "team", "eng"),
            _tag("D", "S", "env", "production").as_inherited(
                SnowflakeObjectDomain.DATABASE
            ),
        ]
        result = _SnowflakeTagCache._deduplicate_tags(tags)
        assert len(result) == 2
        values = {t.name: t.value for t in result}
        assert values["env"] == "staging"
        assert values["team"] == "eng"


class TestSchemaTagsWithInheritance:
    def test_inherits_database_tags(self):
        cache = _build_cache()
        tags = cache.get_schema_tags_with_inheritance("SCH", "DB")

        tag_map = {t.name: t for t in tags}
        assert "team" in tag_map  # direct schema tag
        assert tag_map["team"].is_inherited is False
        assert tag_map["team"].inherited_from is None
        assert "env" in tag_map  # inherited from database
        assert tag_map["env"].is_inherited is True
        assert tag_map["env"].inherited_from == SnowflakeObjectDomain.DATABASE

    def test_schema_without_direct_tags_gets_database_tags(self):
        cache = _build_cache()
        tags = cache.get_schema_tags_with_inheritance("OTHER_SCH", "DB")
        assert len(tags) == 1
        assert tags[0].name == "env"
        assert tags[0].value == "production"
        assert tags[0].is_inherited is True
        assert tags[0].inherited_from == SnowflakeObjectDomain.DATABASE


class TestTableTagsWithInheritance:
    def test_inherits_from_all_levels(self):
        cache = _build_cache()
        tags = cache.get_table_tags_with_inheritance("TBL", "SCH", "DB")

        tag_map = {t.name: t for t in tags}
        assert tag_map["pii"].value == "true"
        assert tag_map["pii"].is_inherited is False  # direct table tag
        assert tag_map["pii"].inherited_from is None
        assert tag_map["team"].value == "data-eng"
        assert tag_map["team"].is_inherited is True  # from schema
        assert tag_map["team"].inherited_from == SnowflakeObjectDomain.SCHEMA
        # env is set at both table and database — table (direct) wins
        assert tag_map["env"].value == "staging"
        assert tag_map["env"].is_inherited is False
        assert tag_map["env"].inherited_from is None

    def test_table_override_beats_database(self):
        cache = _build_cache()
        tags = cache.get_table_tags_with_inheritance("TBL", "SCH", "DB")
        env_tags = [t for t in tags if t.name == "env"]
        assert len(env_tags) == 1
        assert env_tags[0].value == "staging"
        assert env_tags[0].is_inherited is False
        assert env_tags[0].inherited_from is None

    def test_table_without_direct_tags(self):
        cache = _build_cache()
        tags = cache.get_table_tags_with_inheritance("OTHER_TBL", "SCH", "DB")
        tag_map = {t.name: t for t in tags}
        assert tag_map["team"].value == "data-eng"
        assert tag_map["team"].is_inherited is True
        assert tag_map["team"].inherited_from == SnowflakeObjectDomain.SCHEMA
        assert tag_map["env"].value == "production"
        assert tag_map["env"].is_inherited is True
        assert tag_map["env"].inherited_from == SnowflakeObjectDomain.DATABASE


class TestColumnTagsWithInheritance:
    def test_inherits_from_all_levels(self):
        cache = _build_cache()
        col_tags = cache.get_column_tags_for_table_with_inheritance("TBL", "SCH", "DB")

        assert "COL" in col_tags
        tag_map = {t.name: t for t in col_tags["COL"]}
        assert tag_map["sensitivity"].value == "high"
        assert tag_map["sensitivity"].is_inherited is False  # direct column tag
        assert tag_map["sensitivity"].inherited_from is None
        assert tag_map["pii"].value == "true"
        assert tag_map["pii"].is_inherited is True  # from table
        assert tag_map["pii"].inherited_from == SnowflakeObjectDomain.TABLE
        assert tag_map["team"].value == "data-eng"
        assert tag_map["team"].is_inherited is True  # from schema
        assert tag_map["team"].inherited_from == SnowflakeObjectDomain.SCHEMA
        assert tag_map["env"].value == "staging"
        assert tag_map["env"].is_inherited is True  # from table (not db)
        assert tag_map["env"].inherited_from == SnowflakeObjectDomain.TABLE

    def test_no_column_tags_no_parent_tags(self):
        cache = _SnowflakeTagCache()
        col_tags = cache.get_column_tags_for_table_with_inheritance("TBL", "SCH", "DB")
        assert col_tags == {}

    def test_no_column_tags_but_has_parent_tags(self):
        """Columns without direct tags should NOT appear — we don't know column names."""
        cache = _build_cache()
        col_tags = cache.get_column_tags_for_table_with_inheritance("TBL", "SCH", "DB")
        assert "OTHER_COL" not in col_tags

    def test_without_inheritance_only_returns_direct(self):
        """Verify the non-inheritance method still works as before."""
        cache = _build_cache()
        col_tags = cache.get_column_tags_for_table("TBL", "SCH", "DB")
        assert "COL" in col_tags
        assert len(col_tags["COL"]) == 1
        assert col_tags["COL"][0].name == "sensitivity"
        assert col_tags["COL"][0].is_inherited is False
