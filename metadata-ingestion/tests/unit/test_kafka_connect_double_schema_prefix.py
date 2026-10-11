"""Regression tests for kafka-connect double schema prefix in lineage URNs.

When a topic name encodes a schema (e.g. `public.users`) and the destination
sink connector is configured with the same `db.schema` value, sinks that
build a 3-tier URN can produce a doubled schema segment — e.g.
`mydb.public.public.users`.

These tests cover two areas:

1. (JDBC sink, e.g. Postgres / SQL Server / Oracle) Topic name already
   contains the schema (`public.users` with `schema_name="public"`) and the
   default `table.name.format="${topic}"`. Without a guard, the lineage URN
   becomes `mydb.public.public.users`. The fix in `JdbcSinkConnector`
   detects that `table_name` already starts with `schema_name.` and skips
   the duplicate prepend.

2. (Snowflake sink, pinned-current-behavior) Two cases pinned: with and
   without `snowflake.topic2table.map`. The Snowflake URN must always match
   the actual Snowflake table that will be created by the connector (the
   default `re.sub` substitution is documented Snowflake behavior, not a
   bug to "prettify").
"""

from unittest.mock import Mock

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    JdbcSinkConnector,
    SnowflakeSinkConnector,
)


def _make_kafka_connect_config_mock() -> Mock:
    config = Mock(spec=KafkaConnectSourceConfig)
    config.use_schema_resolver = False
    config.schema_resolver_finegrained_lineage = False
    config.env = "PROD"
    config.connect_to_platform_map = None
    config.platform_instance_map = None
    config.convert_lineage_urns_to_lowercase = True
    return config


def test_jdbc_sink_with_schema_qualified_topic_does_not_double_schema() -> None:
    """JDBC sink must not duplicate the schema when the topic name already
    encodes the schema.

    When a topic is named `public.users` and the sink connector is
    configured with `db.schema=public` (or the postgres default `public`)
    and the default `table.name.format="${topic}"`, the lineage target URN
    must be `mydb.public.users` — not `mydb.public.public.users`.
    """
    manifest = ConnectorManifest(
        name="test-postgres-sink",
        type="sink",
        config={
            "connector.class": "PostgresSink",
            "topics": "public.users",
            "connection.host": "db.example.invalid",
            "connection.port": "5432",
            "db.name": "mydb",
            "db.schema": "public",
        },
        tasks=[],
        topic_names=["public.users"],
    )

    config = _make_kafka_connect_config_mock()
    report = Mock(spec=KafkaConnectSourceReport)

    connector = JdbcSinkConnector(manifest, config, report, platform="postgres")

    lineages = connector.extract_lineages()
    assert len(lineages) == 1
    lineage = lineages[0]

    assert lineage.source_dataset == "public.users"
    assert lineage.target_dataset == "mydb.public.users", (
        f"JDBC sink must not duplicate the schema when the topic already "
        f"encodes it; got {lineage.target_dataset!r}"
    )


def test_jdbc_sink_without_schema_qualified_topic_still_prepends_schema() -> None:
    """Sanity check: when the topic does NOT start with the configured
    schema, the JDBC sink must still prepend the schema as before.

    Topic `users` (no schema prefix) with `db.schema=public` should yield
    target URN `mydb.public.users`. This guards against the dedup fix
    accidentally stripping a legitimate schema prepend for unprefixed
    topic names.
    """
    manifest = ConnectorManifest(
        name="test-postgres-sink",
        type="sink",
        config={
            "connector.class": "PostgresSink",
            "topics": "users",
            "connection.host": "db.example.invalid",
            "connection.port": "5432",
            "db.name": "mydb",
            "db.schema": "public",
        },
        tasks=[],
        topic_names=["users"],
    )

    config = _make_kafka_connect_config_mock()
    report = Mock(spec=KafkaConnectSourceReport)

    connector = JdbcSinkConnector(manifest, config, report, platform="postgres")

    lineages = connector.extract_lineages()
    assert len(lineages) == 1
    assert lineages[0].target_dataset == "mydb.public.users"


def test_snowflake_sink_default_naming_uses_native_table_name() -> None:
    """Without `snowflake.topic2table.map`, Snowflake's default rule
    replaces non-alphanumeric chars in the topic with `_`. Topic
    `public.users` becomes the Snowflake table `public_users` in the
    configured schema. DataHub's URN must reflect that real table.

    This is *not* a bug. A future "prettification" PR that emits
    `target_db.public.users` would orphan the URN — Snowflake has no such
    table unless `snowflake.topic2table.map` is configured.
    """
    manifest = ConnectorManifest(
        name="test-snowflake-sink",
        type="sink",
        config={
            "connector.class": "SnowflakeSinkConnector",
            "topics": "public.users",
            "snowflake.database.name": "target_db",
            "snowflake.schema.name": "public",
        },
        tasks=[],
        topic_names=["public.users"],
    )

    config = _make_kafka_connect_config_mock()
    report = Mock(spec=KafkaConnectSourceReport)

    connector = SnowflakeSinkConnector(
        connector_manifest=manifest,
        config=config,
        report=report,
    )

    lineages = connector.extract_lineages()
    assert len(lineages) == 1
    lineage = lineages[0]

    assert lineage.source_dataset == "public.users"
    assert lineage.target_dataset == "target_db.public.public_users", (
        "Snowflake sink URN must match the actual Snowflake table name "
        "(topic dots replaced with underscores per Snowflake connector "
        f"default); got {lineage.target_dataset!r}. If you are intentionally "
        "changing this, ensure the new URN points to a Snowflake table that "
        "actually exists — e.g. via snowflake.topic2table.map."
    )


def test_snowflake_sink_with_topic2table_map_uses_mapped_table_name() -> None:
    """With `snowflake.topic2table.map` configured, the URN must point to
    the explicitly-mapped table name — *not* the default `_`-substituted
    name.

    For topic `public.users` and `snowflake.topic2table.map=public.users:users`,
    the actual Snowflake table is `target_db.public.users`, and the URN
    must match. The existing code already handles this; this test pins
    that behavior so it doesn't silently regress.
    """
    manifest = ConnectorManifest(
        name="test-snowflake-sink",
        type="sink",
        config={
            "connector.class": "SnowflakeSinkConnector",
            "topics": "public.users",
            "snowflake.database.name": "target_db",
            "snowflake.schema.name": "public",
            "snowflake.topic2table.map": "public.users:users",
        },
        tasks=[],
        topic_names=["public.users"],
    )

    config = _make_kafka_connect_config_mock()
    report = Mock(spec=KafkaConnectSourceReport)

    connector = SnowflakeSinkConnector(
        connector_manifest=manifest,
        config=config,
        report=report,
    )

    lineages = connector.extract_lineages()
    assert len(lineages) == 1
    lineage = lineages[0]

    assert lineage.source_dataset == "public.users"
    assert lineage.target_dataset == "target_db.public.users", (
        f"with snowflake.topic2table.map configured, URN must use the "
        f"mapped table name; got {lineage.target_dataset!r}"
    )
