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


# --- Airflow Asset path --------------------------------------------------------


def test_mapped_snowflake_asset_uri_matches_the_warehouse_naming():
    """Authority dropped, slashes become dots, lowercased by default."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/MY_DB/MY_SCHEMA/EVENTS"),
        connections=SNOWFLAKE_CONNECTIONS,
    )

    assert urn == _urn("snowflake", "my_db.my_schema.events")


def test_mapped_asset_uri_applies_platform_instance():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("snowflake://myacct/DB/SCH/TBL"),
        connections={
            "snowflake://myacct": AssetConnectionDetail(platform_instance="prod_acct")
        },
    )

    assert urn == _urn("snowflake", "db.sch.tbl", instance="prod_acct")


def test_mapped_asset_uri_can_preserve_case():
    """BigQuery preserves case, so its mapping must be able to opt out."""
    urn = translate_airflow_asset_to_urn(
        FakeAsset("bigquery://my-project/MyDataset/MyTable"),
        connections={
            "bigquery://my-project": AssetConnectionDetail(
                convert_urns_to_lowercase=False, database="my-project"
            )
        },
    )

    assert urn == _urn("bigquery", "my-project.MyDataset.MyTable")


def test_database_fills_in_a_segment_the_uri_omits():
    urn = translate_airflow_asset_to_urn(
        FakeAsset("postgresql://db.host:5432/public/events"),
        connections={
            "postgres://db.host:5432": AssetConnectionDetail(database="warehouse")
        },
    )

    assert urn == _urn("postgres", "warehouse.public.events")


def test_unmapped_table_scheme_is_skipped_rather_than_guessed():
    """Without a mapping we cannot know the account from the database, so emitting
    anything would mint a plausible-but-wrong URN. Skip instead."""
    assert (
        translate_airflow_asset_to_urn(
            FakeAsset("snowflake://myacct/MY_DB/MY_SCHEMA/EVENTS"),
            connections={},
        )
        is None
    )


@pytest.mark.parametrize(
    "uri,expected_platform,expected_name",
    [
        ("s3://bucket/path/to/data", "s3", "bucket/path/to/data"),
        ("gs://bucket/folder", "gcs", "bucket/folder"),
        ("file:///tmp/data.csv", "file", "tmp/data.csv"),
    ],
)
def test_path_based_schemes_are_unchanged(uri, expected_platform, expected_name):
    """For object stores the URI form already IS the connector's naming, so these need
    no mapping and must keep working exactly as before."""
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


def test_unmapped_ol_dataset_keeps_todays_behaviour():
    """The OL path runs for every plugin user, so an absent mapping must change nothing."""
    urn = translate_ol_to_datahub_urn(
        OpenLineageDataset("snowflake://myacct", "MY_DB.MY_SCHEMA.EVENTS"),
        connections={},
    )

    assert urn == _urn("snowflake", "MY_DB.MY_SCHEMA.EVENTS")


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


def test_connection_detail_defaults_to_lowercasing():
    """Snowflake/Databricks/dbt all default convert_urns_to_lowercase=True, and matching
    those is the point of the mapping."""
    assert AssetConnectionDetail().convert_urns_to_lowercase is True


def test_connections_parse_from_a_json_string():
    """airflow.cfg is INI, so nested config arrives as JSON (same as dag_filter_str)."""
    from datahub_airflow_plugin._config import parse_asset_connections

    parsed = parse_asset_connections(
        '{"snowflake://myacct": {"platform_instance": "prod_acct"}}'
    )

    assert parsed["snowflake://myacct"].platform_instance == "prod_acct"
    assert parsed["snowflake://myacct"].convert_urns_to_lowercase is True


def test_empty_connections_config_parses_to_an_empty_mapping():
    from datahub_airflow_plugin._config import parse_asset_connections

    assert parse_asset_connections(None) == {}
    assert parse_asset_connections("{}") == {}
