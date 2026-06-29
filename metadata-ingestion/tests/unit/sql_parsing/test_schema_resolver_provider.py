from unittest.mock import MagicMock

from datahub.sql_parsing.schema_resolver_provider import SchemaResolverProvider

_WITH_SCHEMA = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.HAS_SCHEMA,PROD)"
)
_SCHEMALESS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.NO_SCHEMA,PROD)"


def _graph_yielding(captured: dict) -> MagicMock:
    """A graph whose bulk fetch yields one schema-bearing and one schemaless entity.

    The schemaless one is only yielded when include_schemaless is requested, mirroring
    the real _bulk_fetch_schema_info_by_filter behavior.
    """

    def fake_bulk(
        *, platform, platform_instance, env, batch_size, include_schemaless=False
    ):
        captured["include_schemaless"] = include_schemaless
        yield (
            _WITH_SCHEMA,
            {"fields": [{"fieldPath": "amount", "nativeDataType": "int"}]},
        )
        if include_schemaless:
            yield _SCHEMALESS, None

    graph = MagicMock()
    graph._bulk_fetch_schema_info_by_filter.side_effect = fake_bulk
    return graph


def test_provider_default_does_not_populate_membership() -> None:
    captured: dict = {}
    resolver = SchemaResolverProvider(graph=_graph_yielding(captured)).get(
        platform="snowflake", platform_instance=None, env="PROD"
    )
    assert captured["include_schemaless"] is False  # schema-only scroll
    assert resolver.get_cached_schema_info(_WITH_SCHEMA) is not None
    assert resolver.find_by_casing(_WITH_SCHEMA) == []  # membership not populated


def test_provider_populate_membership_includes_schemaless() -> None:
    captured: dict = {}
    resolver = SchemaResolverProvider(graph=_graph_yielding(captured)).get(
        platform="snowflake",
        platform_instance=None,
        env="PROD",
        populate_membership=True,
    )
    assert captured["include_schemaless"] is True  # single scroll covers everything
    # Schemas are still populated...
    assert resolver.get_cached_schema_info(_WITH_SCHEMA) is not None
    assert resolver.get_cached_schema_info(_SCHEMALESS) is None  # exists, no schema
    # ...and membership covers both schema-bearing and schemaless entities.
    assert resolver.find_by_casing(_WITH_SCHEMA) == [_WITH_SCHEMA]
    assert resolver.find_by_casing(_SCHEMALESS) == [_SCHEMALESS]
    assert resolver.known_urn_count() == 2
