"""Connection-mapped URN construction for Airflow Assets and OpenLineage datasets.

A table reachable through both the Airflow Asset path and the OpenLineage facet path
used to produce two DataHub URNs, neither matching the one the warehouse's own connector
emits, because each writer named the table differently:

    ingestion (snowflake source)  my_db.my_schema.events   dotted, lowercase
    OpenLineage facets            MY_DB.MY_SCHEMA.EVENTS   dotted, UPPERCASE
    Airflow Asset URI             acct/MY_DB/MY_SCHEMA/EVENTS   slashes + account

DataHub identity is exact string matching, so that is three datasets for one table. A
user-supplied connection mapping (the same pattern as Fivetran's
sources_to_platform_instance and Spark's metadata.dataset.connections) lets both writers
resolve to the warehouse's own naming instead of guessing.
"""

from typing import Dict, Optional

import pytest
from openlineage.client.run import Dataset as OpenLineageDataset

from datahub_airflow_plugin._airflow_asset_adapter import translate_airflow_asset_to_urn
from datahub_airflow_plugin._config import AssetConnectionDetail
from datahub_airflow_plugin._datahub_ol_adapter import translate_ol_to_datahub_urn


class FakeAsset:
    """Minimal stand-in for airflow.sdk.Asset (detected by MRO name + `uri`)."""

    def __init__(self, uri: str) -> None:
        self.uri = uri


SNOWFLAKE_CONNECTIONS: Dict[str, AssetConnectionDetail] = {
    "snowflake://myacct": AssetConnectionDetail()
}


def _urn(platform: str, name: str, instance: Optional[str] = None) -> str:
    if instance:
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}.{name},PROD)"
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


# --- Airflow Asset path: derivation without any mapping ----------------------
#
# Each connector's naming is a fixed rule, not a guess, so a URI alone is enough for
# everything except platform_instance:
#
#   snowflake  drop authority (an account)  db.schema.table    lowercased
#   postgres   drop authority (a host)      db.schema.table    lowercased
#   mysql      drop authority (a host)      db.table           lowercased
#   bigquery   KEEP authority (the project) project.ds.table   case kept
#
# Casing comes from PLATFORMS_WITH_CASE_SENSITIVE_TABLES, the same constant the SQL parser
# uses, so all three of the plugin's writers agree.


def test_unmapped_snowflake_uri_derives_the_warehouse_naming():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/MY_DB/MY_SCHEMA/EVENTS"), connections={}
    )

    assert urn == _urn("snowflake", "my_db.my_schema.events")


def test_unmapped_postgres_uri_is_lowercased():
    """Case follows DataHub's PLATFORMS_WITH_CASE_SENSITIVE_TABLES, the same rule the SQL
    parser applies, so all three of the plugin's writers agree. Postgres is
    case-insensitive, so its URNs are lowercased."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("postgresql://db.host:5432/MyDb/MySchema/MyTable"), connections={}
    )

    assert urn == _urn("postgres", "mydb.myschema.mytable")


def test_unmapped_mysql_uri_is_two_part():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("mysql://db.host/mydb/mytable"), connections={}
    )

    assert urn == _urn("mysql", "mydb.mytable")


def test_unmapped_bigquery_uri_keeps_the_project():
    """BigQuery puts the project in the authority, where the others put a connection."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("bigquery://my-project/MyDataset/MyTable"), connections={}
    )

    assert urn == _urn("bigquery", "my-project.MyDataset.MyTable")


def test_unexpected_segment_count_still_emits_lineage():
    """A hand-written URI may omit the account, making the shape ambiguous. Emit the
    best-effort URN so no lineage is lost; the warning names it for diagnosis."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://MY_DB/MY_SCHEMA"), connections={}
    )

    assert urn == _urn("snowflake", "my_schema")


# --- Airflow Asset path: mapping supplies what a URI cannot ------------------


def test_mapping_supplies_platform_instance():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/DB/SCH/TBL"),
        connections={
            "snowflake://myacct": AssetConnectionDetail(platform_instance="prod_acct")
        },
    )

    assert urn == _urn("snowflake", "db.sch.tbl", instance="prod_acct")


def test_mapping_can_override_the_platform_case_default():
    """For a warehouse whose recipe sets convert_urns_to_lowercase=False, the mapping has
    to be able to switch lowercasing back off."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("postgresql://db.host:5432/MyDb/MySchema/MyTable"),
        connections={
            "postgres://db.host:5432": AssetConnectionDetail(
                convert_urns_to_lowercase=False
            )
        },
    )

    assert urn == _urn("postgres", "MyDb.MySchema.MyTable")


def test_database_fills_in_a_segment_the_uri_omits():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("postgresql://db.host:5432/public/events"),
        connections={
            "postgres://db.host:5432": AssetConnectionDetail(database="warehouse")
        },
    )

    assert urn == _urn("postgres", "warehouse.public.events")


def test_the_mapping_never_changes_the_environment():
    """Every writer uses the plugin-wide cluster. OpenLineage datasets carry no
    environment, so the synthetic round trip in the SQL-parsing path cannot propagate a
    per-connection one — applying it to a single writer splits a table reachable by two
    writers into two URNs differing only by env."""
    mapped = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/DB/SCH/TBL"),
        env="DEV",
        connections={"snowflake://myacct": AssetConnectionDetail()},
    )
    unmapped = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/DB/SCH/TBL"), env="DEV", connections={}
    )

    assert mapped == unmapped
    assert mapped == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.tbl,DEV)"


def test_the_mapping_cannot_declare_an_environment():
    """A stray `env` key must be rejected rather than silently ignored, so nobody
    configures one and believes it took effect."""
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        AssetConnectionDetail.model_validate({"platform_instance": "x", "env": "DEV"})


@pytest.mark.parametrize(
    "uri,expected_platform,expected_name",
    [
        ("s3://bucket/path/to/data", "s3", "bucket/path/to/data"),
        ("gs://bucket/folder", "gcs", "bucket/folder"),
        ("file:///tmp/data.csv", "file", "tmp/data.csv"),
    ],
)
def test_path_based_schemes_are_unchanged(uri, expected_platform, expected_name):
    """For object stores the URI form already IS the connector's naming."""
    urn = translate_airflow_asset_to_urn(FakeAsset(uri), connections={})

    assert urn == _urn(expected_platform, expected_name)


def test_bare_asset_name_is_unchanged():
    urn = translate_airflow_asset_to_urn(FakeAsset("my_asset"), connections={})

    assert urn == _urn("airflow", "my_asset")


def test_connection_key_matching_is_case_insensitive():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("Snowflake://MyAcct/DB/SCH/TBL"),
        connections={"snowflake://myacct": AssetConnectionDetail()},
    )

    assert urn == _urn("snowflake", "db.sch.tbl")


# --- OpenLineage facet path ----------------------------------------------------


def test_mapped_ol_dataset_is_lowercased_to_match_ingestion():
    """OL producers report Snowflake names in upper case; ingestion lowercases. Without
    the mapping that alone is a duplicate."""
    urn = translate_ol_to_datahub_urn(
        OpenLineageDataset("snowflake://myacct", "MY_DB.MY_SCHEMA.EVENTS"),
        connections=SNOWFLAKE_CONNECTIONS,
    )

    assert urn == _urn("snowflake", "my_db.my_schema.events")


def test_mapped_ol_dataset_applies_platform_instance():
    urn = translate_ol_to_datahub_urn(
        OpenLineageDataset("snowflake://myacct", "DB.SCH.TBL"),
        connections={
            "snowflake://myacct": AssetConnectionDetail(platform_instance="prod_acct")
        },
    )

    assert urn == _urn("snowflake", "db.sch.tbl", instance="prod_acct")


def test_unmapped_ol_dataset_follows_the_platform_case_default():
    """OL producers report Snowflake names in upper case while the Snowflake source
    lowercases them, so an unmapped OL dataset was a duplicate of the ingested one on
    casing alone. Applying the platform's own default fixes that with no config — and
    keeps this writer in step with the Asset path, which derives the same way."""
    urn = translate_ol_to_datahub_urn(
        OpenLineageDataset("snowflake://myacct", "MY_DB.MY_SCHEMA.EVENTS"),
        connections={},
    )

    assert urn == _urn("snowflake", "my_db.my_schema.events")


def test_unmapped_ol_dataset_on_a_case_sensitive_platform_is_untouched():
    """BigQuery is in PLATFORMS_WITH_CASE_SENSITIVE_TABLES, so its names pass through.

    The namespace is bare "bigquery" (BIGQUERY_NAMESPACE in the Google provider), with the
    project carried in the name — so passing the name through is what matches the
    connector, and there is no authority to fold in.
    """
    urn = translate_ol_to_datahub_urn(
        OpenLineageDataset("bigquery", "my-project.MyDataset.MyTable"),
        connections={},
    )

    assert urn == _urn("bigquery", "my-project.MyDataset.MyTable")


def test_bigquery_writers_converge_despite_the_project_living_in_different_places():
    """The Asset URI puts the project in the authority while OpenLineage puts it in the
    name, so the two writers reach the same URN by different routes. Asserting it guards
    against "fixing" one side into divergence."""
    from_asset = translate_airflow_asset_to_urn(
        FakeAsset("bigquery://my-project/MyDataset/MyTable"), connections={}
    )
    from_ol = translate_ol_to_datahub_urn(
        OpenLineageDataset("bigquery", "my-project.MyDataset.MyTable"), connections={}
    )

    assert from_asset == from_ol == _urn("bigquery", "my-project.MyDataset.MyTable")


def test_both_writers_converge_with_no_mapping_at_all():
    """The zero-config case: the two writers agree without any asset_connections entry."""
    from_asset = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/MY_DB/MY_SCHEMA/EVENTS"), connections={}
    )
    from_ol = translate_ol_to_datahub_urn(
        OpenLineageDataset("snowflake://myacct", "MY_DB.MY_SCHEMA.EVENTS"),
        connections={},
    )

    assert from_asset == from_ol == _urn("snowflake", "my_db.my_schema.events")


def test_both_writers_converge_on_one_urn_when_mapped():
    """The point of the whole feature: one table, one URN."""
    connections = {
        "snowflake://myacct": AssetConnectionDetail(platform_instance="prod_acct")
    }

    from_asset = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/MY_DB/MY_SCHEMA/EVENTS"),
        connections=connections,
    )
    from_ol = translate_ol_to_datahub_urn(
        OpenLineageDataset("snowflake://myacct", "MY_DB.MY_SCHEMA.EVENTS"),
        connections=connections,
    )

    assert from_asset == from_ol
    assert from_asset == _urn(
        "snowflake", "my_db.my_schema.events", instance="prod_acct"
    )


# --- config parsing -----------------------------------------------------------


def test_connection_detail_leaves_casing_to_the_platform_by_default():
    """Only snowflake overrides convert_urns_to_lowercase to True; postgres, mysql and
    bigquery inherit False. An unset value must defer to the platform, not force one."""
    assert AssetConnectionDetail().convert_urns_to_lowercase is None


def test_connections_parse_from_a_json_string():
    """airflow.cfg is INI, so nested config arrives as JSON (same as dag_filter_str)."""
    from datahub_airflow_plugin._config import parse_asset_connections

    parsed = parse_asset_connections(
        '{"snowflake://myacct": {"platform_instance": "prod_acct"}}'
    )

    assert parsed["snowflake://myacct"].platform_instance == "prod_acct"
    assert parsed["snowflake://myacct"].convert_urns_to_lowercase is None


def test_empty_connections_config_parses_to_an_empty_mapping():
    from datahub_airflow_plugin._config import parse_asset_connections

    assert parse_asset_connections(None) == {}
    assert parse_asset_connections("{}") == {}


# --- default database for SQL parsing -----------------------------------------


def test_default_database_prefers_what_the_operator_and_connection_report():
    """The operator argument and the connection's own database are authoritative; the
    mapping must not override a value the connection actually reported."""
    from datahub_airflow_plugin._connection_mapping import resolve_default_database

    detail = AssetConnectionDetail(database="from_config")

    assert (
        resolve_default_database("from_operator", "from_connection", detail)
        == "from_operator"
    )
    assert (
        resolve_default_database(None, "from_connection", detail) == "from_connection"
    )


def test_default_database_falls_back_to_the_mapping_when_unknown():
    """Without a database the SQL parser cannot fully qualify table names, so a configured
    one is better than none."""
    from datahub_airflow_plugin._connection_mapping import resolve_default_database

    detail = AssetConnectionDetail(database="from_config")

    assert resolve_default_database(None, None, detail) == "from_config"


def test_default_database_stays_none_when_nothing_supplies_one():
    from datahub_airflow_plugin._connection_mapping import resolve_default_database

    assert resolve_default_database(None, None, None) is None
    assert resolve_default_database(None, None, AssetConnectionDetail()) is None


# --- findings from CI review ---------------------------------------------------


def test_config_key_written_with_the_uri_scheme_still_matches():
    """A user copies the scheme straight off the Asset URI, so `postgresql://` must resolve
    to the same entry as `postgres://` — the docs promise they share one."""
    from datahub_airflow_plugin._config import parse_asset_connections

    conns = parse_asset_connections(
        '{"postgresql://db.host:5432": {"platform_instance": "pg_inst"}}'
    )

    urn = translate_airflow_asset_to_urn(
        FakeAsset("postgresql://db.host:5432/mydb/public/events"), connections=conns
    )

    assert urn == _urn("postgres", "mydb.public.events", instance="pg_inst")


def test_unmapped_ol_namespace_keeps_its_original_platform():
    """The OpenLineage path must stay byte-identical when no mapping is configured. Only
    OL_SCHEME_TWEAKS applied before, so canonicalising more schemes would silently re-key
    existing datasets."""
    for namespace, name, expected_platform in [
        ("gs://bucket", "folder/file", "gs"),
        ("s3a://bucket", "key", "s3a"),
        ("abfs://container", "path", "abfs"),
        # the two OL_SCHEME_TWEAKS entries are the exception and must still apply
        ("sqlserver://host", "db.sch.tbl", "mssql"),
        ("awsathena://host", "db.tbl", "athena"),
    ]:
        urn = translate_ol_to_datahub_urn(
            OpenLineageDataset(namespace, name), connections={}
        )
        assert urn == _urn(expected_platform, name), namespace


def test_platform_override_outside_the_table_naming_table_does_not_raise():
    """An override to a platform with no naming rule must degrade, not crash — a KeyError
    here drops the asset's lineage, and can clear a whole alias-resolution batch."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://acct/DB/SCH/TBL"),
        connections={
            "snowflake://acct": AssetConnectionDetail(platform="some_other_platform")
        },
    )

    # Naming falls back to snowflake's shape, but casing keys off the overridden platform,
    # which is unrecognised and therefore case-preserving.
    assert urn == _urn("some_other_platform", "DB.SCH.TBL")


def test_database_is_not_prepended_when_the_authority_already_supplied_it():
    """BigQuery keeps its authority as the project segment, so a mapping that also sets
    `database` must not double it into project.project.dataset.table."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("bigquery://my-project/ds/tbl"),
        connections={
            "bigquery://my-project": AssetConnectionDetail(database="my-project")
        },
    )

    assert urn == _urn("bigquery", "my-project.ds.tbl")


# --- warning volume ------------------------------------------------------------


def _count_warnings(assets):
    import logging

    from datahub_airflow_plugin._airflow_asset_adapter import extract_urns_from_iolets

    class Collect(logging.Handler):
        def __init__(self):
            super().__init__()
            self.warnings = []

        def emit(self, record):
            if record.levelno >= logging.WARNING:
                self.warnings.append(record.getMessage())

    handler = Collect()
    logger = logging.getLogger("datahub_airflow_plugin")
    logger.addHandler(handler)
    previous_level = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        urns = extract_urns_from_iolets(assets, capture_airflow_assets=True)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    return urns, handler.warnings


class Asset:
    """The class name matters: extract_urns_from_iolets recognises an Airflow asset by
    walking the MRO for a class literally named Asset or Dataset, so a differently-named
    stub is silently skipped and any warning assertion passes vacuously."""

    def __init__(self, uri: str) -> None:
        self.uri = uri


def test_many_unparseable_assets_do_not_flood_the_task_log():
    """A DAG can declare hundreds of assets on one connection. Two warnings per asset -
    detail from the translator plus a generic one from the caller - buries the task log,
    which is what a reviewer hit in practice."""
    _reset_warning_dedup()

    urns, warnings = _count_warnings([Asset(f"snowflake://[bad{i}") for i in range(25)])

    assert urns == []
    assert len(warnings) == 1, warnings


def test_many_assets_with_no_table_path_do_not_flood_the_task_log():
    _reset_warning_dedup()

    urns, warnings = _count_warnings([Asset("snowflake://acct") for _ in range(25)])

    assert urns == []
    assert len(warnings) == 1, warnings


def test_the_first_occurrence_still_names_the_offending_uri():
    """Deduplication must not cost diagnosability: the surviving warning has to identify
    the asset so it stays actionable."""
    _reset_warning_dedup()

    _, warnings = _count_warnings([Asset("snowflake://[bad0") for _ in range(5)])

    assert warnings
    assert "snowflake://[bad0" in warnings[0]


def _reset_warning_dedup():
    from datahub_airflow_plugin import _connection_mapping

    _connection_mapping._warned_keys.clear()


def test_assets_that_cannot_yield_a_urn_are_never_dropped_silently():
    """Every path that returns None must warn, because the caller only logs at debug now.
    An asset vanishing from lineage with nothing in the task log is worse than noise."""
    for label, uri in [
        ("empty uri", ""),
        ("scheme only", "s3://"),
        ("whitespace", "   "),
    ]:
        _reset_warning_dedup()
        urns, warnings = _count_warnings([Asset(uri)])
        assert urns == [], label
        assert len(warnings) == 1, f"{label}: {warnings}"
        assert uri.strip() in warnings[0] or "empty" in warnings[0].lower(), (
            f"{label}: {warnings}"
        )


def test_a_non_string_uri_does_not_raise():
    """`uri` reaches us from a user-authored Asset, so a non-string must degrade to a
    warning rather than an AttributeError inside the error handler itself."""
    _reset_warning_dedup()

    class Asset:
        uri = 12345

    urns, warnings = _count_warnings([Asset()])

    assert urns == []
    assert len(warnings) == 1, warnings
