"""Tests for datastore -> DataHub dataset URN resolution.

The highest-consequence logic in the connector: everything else attaches to whatever
URN comes out of here, and a wrong URN is worse than no URN because the metadata is
both invisible and misleading. So these cover the full store-type matrix, the
precedence between explicit config and inference, and -- most importantly -- that
unresolvable datastores are skipped and reported rather than guessed at.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.qualytics import config as config_module
from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig
from datahub.ingestion.source.qualytics.models import (
    Container,
    parse_container,
    parse_datastore,
)
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.ingestion.source.qualytics.urn_resolver import UrnResolver


def _resolver(**overrides: Any) -> tuple[UrnResolver, QualyticsSourceReport]:
    config = QualyticsSourceConfig.model_validate(
        {"base_url": "https://acme.qualytics.io/api", "token": "t", **overrides}
    )
    report = QualyticsSourceReport()
    return UrnResolver(config, report), report


def _jdbc(qualytics_type: str, **overrides: Any) -> Any:
    ds, _ = parse_datastore(
        {
            "id": 1,
            "name": "warehouse",
            "store_type": "jdbc",
            "type": qualytics_type,
            "jdbc_url": "jdbc:x://host/db",
            "database": "SALES",
            "schema": "PUBLIC",
            **overrides,
        }
    )
    return ds


def _table(name: str = "ORDERS") -> Container:
    container, _ = parse_container(
        {
            "id": 10,
            "name": name,
            "container_type": "table",
            "table_type": "table",
            "status": "Available",
            "datastore": {
                "id": 1,
                "name": "warehouse",
                "store_type": "jdbc",
                "type": "snowflake",
            },
        }
    )
    return container


# --- platform mapping across every Qualytics connection type ----------------------


@pytest.mark.parametrize(
    ("qualytics_type", "expected_platform"),
    [
        # Identity mappings across the JDBC connectors.
        ("athena", "athena"),
        ("bigquery", "bigquery"),
        ("databricks", "databricks"),
        ("db2", "db2"),
        ("dremio", "dremio"),
        ("hana", "hana"),
        ("hive", "hive"),
        ("mysql", "mysql"),
        ("oracle", "oracle"),
        ("presto", "presto"),
        ("redshift", "redshift"),
        ("snowflake", "snowflake"),
        ("teradata", "teradata"),
        ("trino", "trino"),
        # Renames. Each of these produces an unmatchable URN if left alone.
        ("postgresql", "postgres"),
        ("sqlserver", "mssql"),
        ("synapse", "mssql"),
        ("timescale", "postgres"),
        # mariadb maps to itself, not to mysql -- DataHub
        # ships a dedicated mariadb source that emits mariadb URNs.
        ("mariadb", "mariadb"),
    ],
)
def test_jdbc_connection_types_infer_the_right_platform(
    qualytics_type: str, expected_platform: str
) -> None:
    resolver, _ = _resolver()

    urn = resolver.dataset_urn(_jdbc(qualytics_type), _table())

    assert urn is not None
    assert f"urn:li:dataPlatform:{expected_platform}," in urn


@pytest.mark.parametrize(
    ("qualytics_type", "expected_platform"),
    [("s3", "s3"), ("gcs", "gcs"), ("abfs", "abs")],
)
def test_dfs_connection_types_infer_the_right_platform(
    qualytics_type: str, expected_platform: str
) -> None:
    resolver, _ = _resolver()
    ds, _ = parse_datastore(
        {
            "id": 2,
            "name": "lake",
            "store_type": "dfs",
            "type": qualytics_type,
            "uri": f"{qualytics_type}://bucket",
            "root_path": "/events",
        }
    )

    urn = resolver.dataset_urn(ds, _table("2026"))

    assert urn is not None
    assert f"urn:li:dataPlatform:{expected_platform}," in urn


@pytest.mark.parametrize(
    ("qualytics_type", "expected_platform"),
    [
        ("databricks_native", "databricks"),
        ("unity_native", "databricks"),
        ("hive_native", "hive"),
        ("glue_native", "glue"),
    ],
)
def test_native_connection_types_drop_the_native_suffix(
    qualytics_type: str, expected_platform: str
) -> None:
    # The _native suffix is a Qualytics implementation detail; DataHub knows the
    # platform by its plain name.
    resolver, _ = _resolver()
    ds, _ = parse_datastore(
        {
            "id": 3,
            "name": "uc",
            "store_type": "native",
            "type": qualytics_type,
            "catalog": "main",
            "schema": "gold",
        }
    )

    urn = resolver.dataset_urn(ds, _table("customers"))

    assert urn is not None
    assert f"urn:li:dataPlatform:{expected_platform}," in urn


# --- dataset naming ----------------------------------------------------------------


def test_jdbc_dataset_name_is_database_schema_table() -> None:
    resolver, _ = _resolver()

    urn = resolver.dataset_urn(_jdbc("postgresql"), _table("ORDERS"))

    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:postgres,SALES.PUBLIC.ORDERS,PROD)"
    )


def test_jdbc_dataset_name_omits_missing_levels_rather_than_leaving_empty_segments() -> (
    None
):
    # A datastore with no schema must produce `db.table`, not `db..table` -- the
    # latter matches nothing.
    resolver, _ = _resolver()
    ds = _jdbc("postgresql", schema=None)

    urn = resolver.dataset_urn(ds, _table("orders"))

    assert urn == "urn:li:dataset:(urn:li:dataPlatform:postgres,SALES.orders,PROD)"


def test_native_dataset_name_is_catalog_schema_table() -> None:
    resolver, _ = _resolver()
    ds, _ = parse_datastore(
        {
            "id": 3,
            "name": "uc",
            "store_type": "native",
            "type": "databricks_native",
            "catalog": "main",
            "schema": "gold",
        }
    )

    urn = resolver.dataset_urn(ds, _table("customers"))

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:databricks,main.gold.customers,PROD)"
    )


@pytest.mark.parametrize(
    ("qualytics_type", "expected_urn"),
    [
        (
            "hive_native",
            "urn:li:dataset:(urn:li:dataPlatform:hive,gold.customers,PROD)",
        ),
        (
            "glue_native",
            "urn:li:dataset:(urn:li:dataPlatform:glue,gold.customers,PROD)",
        ),
    ],
)
def test_hive_and_glue_dataset_names_leave_out_the_spark_catalog(
    qualytics_type: str, expected_urn: str
) -> None:
    # Qualytics derives a catalog name for these only because Spark needs one. The
    # Hive and Glue sources name datasets database.table, so including it would put
    # every assertion on a dataset that does not exist.
    resolver, _ = _resolver()
    ds, _ = parse_datastore(
        {
            "id": 3,
            "name": "metastore",
            "store_type": "native",
            "type": qualytics_type,
            "catalog": "hive_metastore_1a2b",
            "schema": "gold",
        }
    )

    urn = resolver.dataset_urn(ds, _table("customers"))

    assert urn == expected_urn


def test_dfs_dataset_name_strips_the_scheme_and_joins_the_path() -> None:
    # Mirrors what DataHub's s3 source emits: the table path minus the URI scheme,
    # slash-trimmed.
    resolver, _ = _resolver()
    ds, _ = parse_datastore(
        {
            "id": 2,
            "name": "lake",
            "store_type": "dfs",
            "type": "s3",
            "uri": "s3://analytics-bucket",
            "root_path": "/warehouse/",
        }
    )
    container, _ = parse_container(
        {
            "id": 11,
            "name": "events.parquet",
            "container_type": "file",
            "status": "Available",
            "datastore": {"id": 2, "name": "lake", "store_type": "dfs", "type": "s3"},
            "relative_path": "/events/daily",
            "file_name": "events.parquet",
            "extension": "parquet",
        }
    )

    urn = resolver.dataset_urn(ds, container)

    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:s3,analytics-bucket/warehouse/events/daily,PROD)"
    )


def test_dfs_resolutions_are_counted_as_path_reconstructions() -> None:
    # Object-store names depend on the customer's path_spec, so they are the ones most
    # likely to miss. The report has to distinguish them from confident resolutions or
    # an operator has no signal to reach for the explicit map.
    resolver, report = _resolver()
    ds, _ = parse_datastore(
        {
            "id": 2,
            "name": "lake",
            "store_type": "dfs",
            "type": "s3",
            "uri": "s3://b",
            "root_path": "/",
        }
    )

    resolver.dataset_urn(ds, _table("f.parquet"))

    assert report.urns_resolved == 1
    assert report.urns_resolved_by_path_reconstruction == 1


def test_mapped_dfs_resolutions_are_still_counted_as_path_reconstructions() -> None:
    # The map pins the platform; the object path is reconstructed either way.
    resolver, report = _resolver(datastore_to_platform_map={"lake": {"platform": "s3"}})
    ds, _ = parse_datastore(
        {
            "id": 2,
            "name": "lake",
            "store_type": "dfs",
            "type": "s3",
            "uri": "s3://b",
            "root_path": "/",
        }
    )

    resolver.dataset_urn(ds, _table("f.parquet"))

    assert report.urns_resolved_by_path_reconstruction == 1


def test_jdbc_resolutions_are_not_counted_as_path_reconstructions() -> None:
    resolver, report = _resolver()

    resolver.dataset_urn(_jdbc("snowflake"), _table())

    assert report.urns_resolved == 1
    assert report.urns_resolved_by_path_reconstruction == 0


# --- explicit map ------------------------------------------------------------------


def test_explicit_map_by_name_overrides_inference() -> None:
    # The Qualytics type says postgresql, but the operator knows these tables were
    # catalogued under a different platform. Explicit config must win.
    resolver, _ = _resolver(
        datastore_to_platform_map={
            "warehouse": {
                "platform": "redshift",
                "platform_instance": "prod-rs",
                "env": "DEV",
            }
        }
    )

    urn = resolver.dataset_urn(_jdbc("postgresql"), _table("orders"))

    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,prod-rs.SALES.PUBLIC.orders,DEV)"
    )


def test_explicit_map_can_be_keyed_by_datastore_id() -> None:
    # Names are renamable in Qualytics; a rename would silently stop a name-keyed
    # mapping matching. The id is the immutable alternative.
    resolver, _ = _resolver(
        datastore_to_platform_map={"1": {"platform": "redshift"}},
    )

    urn = resolver.dataset_urn(_jdbc("postgresql"), _table("orders"))

    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:redshift,SALES.PUBLIC.orders,PROD)"
    )


def test_name_takes_precedence_over_id_when_both_are_mapped() -> None:
    resolver, _ = _resolver(
        datastore_to_platform_map={
            "warehouse": {"platform": "snowflake"},
            "1": {"platform": "redshift"},
        },
    )

    urn = resolver.dataset_urn(_jdbc("postgresql"), _table())

    assert urn is not None
    assert "dataPlatform:snowflake" in urn


def test_a_typo_in_the_configured_platform_is_rejected_at_config_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Otherwise it produces well-formed URNs under a platform nothing emits: the run
    # "succeeds" and nothing appears in DataHub.
    #
    # The registry is patched rather than read: it is absent from released
    # acryl-datahub wheels (see test_platform_validation_degrades_...), so relying on
    # the real one would make this test pass or skip depending on how DataHub was
    # installed. Patching keeps it testing our validator.
    monkeypatch.setattr(
        config_module, "get_known_data_platforms", lambda: frozenset({"snowflake"})
    )

    with pytest.raises(ValidationError) as exc:
        QualyticsSourceConfig.model_validate(
            {
                "base_url": "https://acme.qualytics.io/api",
                "token": "t",
                "datastore_to_platform_map": {"warehouse": {"platform": "snowflke"}},
            }
        )

    message = str(exc.value)
    assert "Unknown DataHub platform" in message
    assert "snowflake" in message  # the suggestion


def test_a_known_platform_passes_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config_module, "get_known_data_platforms", lambda: frozenset({"snowflake"})
    )

    config = QualyticsSourceConfig.model_validate(
        {
            "base_url": "https://acme.qualytics.io/api",
            "token": "t",
            "datastore_to_platform_map": {"warehouse": {"platform": "snowflake"}},
        }
    )

    assert config.datastore_to_platform_map["warehouse"].platform == "snowflake"


def test_platform_validation_is_skipped_when_the_registry_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Documented degradation, and not hypothetical: acryl-datahub's package_data
    # covers `datahub.ingestion.autogenerated` but not its `connector_registry`
    # subpackage, so datahub.json is missing from every released wheel and the
    # registry loads as None for pip-installed users. Blocking the run in that case
    # would make the connector unusable wherever validation happens to be impossible.
    monkeypatch.setattr(config_module, "get_known_data_platforms", lambda: None)

    config = QualyticsSourceConfig.model_validate(
        {
            "base_url": "https://acme.qualytics.io/api",
            "token": "t",
            "datastore_to_platform_map": {
                "warehouse": {"platform": "definitely-not-real"}
            },
        }
    )

    assert (
        config.datastore_to_platform_map["warehouse"].platform == "definitely-not-real"
    )


# --- refusing to guess -------------------------------------------------------------


def test_unmappable_connection_type_is_skipped_and_reported() -> None:
    resolver, report = _resolver()
    ds = _jdbc("fabric")

    urn = resolver.dataset_urn(ds, _table())

    assert urn is None
    assert report.datastores_unresolved == 1
    assert "warehouse" in list(report.unresolved_datastores)


def test_an_unknown_connection_type_is_skipped_rather_than_used_as_a_platform() -> None:
    # Passing an unknown type through as a platform name once turned glue_native
    # datastores into urn:li:dataPlatform:glue_native, which no source emits.
    resolver, report = _resolver()

    urn = resolver.dataset_urn(_jdbc("newdb"), _table())

    assert urn is None
    assert report.datastores_unresolved == 1


def test_inference_disabled_means_unmapped_datastores_are_skipped() -> None:
    resolver, report = _resolver(infer_source_platform=False)

    urn = resolver.dataset_urn(_jdbc("snowflake"), _table())

    assert urn is None
    assert report.datastores_unresolved == 1


def test_an_unknown_store_type_produces_no_urn_rather_than_a_wrong_one() -> None:
    # parse_datastore degrades an unrecognised store_type to the base model. The
    # resolver must then decline to name it -- there is no shape to work from.
    resolver, report = _resolver()
    ds, recognised = parse_datastore(
        {"id": 9, "name": "quantum", "store_type": "quantum", "type": "snowflake"}
    )

    assert not recognised
    assert resolver.dataset_urn(ds, _table()) is None
    # containers_unnamed, not datastores_unresolved: the datastore resolved to a
    # platform, it is this container's shape we cannot name. Counting it as a
    # datastore failure reported "500 datastores unresolved" for one datastore.
    assert report.containers_unnamed == 1
    assert report.datastores_unresolved == 0


def test_the_unresolved_warning_names_the_datastore_and_the_fix() -> None:
    resolver, report = _resolver(infer_source_platform=False)

    resolver.dataset_urn(_jdbc("snowflake"), _table())

    warnings = [str(w) for w in report.warnings]
    assert any("warehouse" in w for w in warnings)
    assert any("datastore_to_platform_map" in w for w in warnings)


# --- casing ------------------------------------------------------------------------


def test_unset_casing_follows_each_platforms_own_source_default() -> None:
    # DataHub's Snowflake source lowercases by default; its Postgres source preserves
    # case. One recipe spanning both must match both, which a single framework-wide
    # default cannot. Found on a live DataHub where the two sat side by side.
    resolver, _ = _resolver()

    snowflake = resolver.dataset_urn(_jdbc("snowflake"), _table("ORDERS"))
    postgres = resolver.dataset_urn(
        _jdbc("postgresql", id=2, name="pg"), _table("ORDERS")
    )

    assert (
        snowflake
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.public.orders,PROD)"
    )
    assert (
        postgres
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,SALES.PUBLIC.ORDERS,PROD)"
    )


def test_a_mapped_snowflake_datastore_also_gets_the_snowflake_default() -> None:
    resolver, _ = _resolver(
        datastore_to_platform_map={"warehouse": {"platform": "snowflake"}}
    )

    urn = resolver.dataset_urn(_jdbc("snowflake"), _table("ORDERS"))

    assert urn is not None
    assert "sales.public.orders" in urn


def test_an_explicit_top_level_casing_applies_to_every_platform() -> None:
    # For a Snowflake source ingested with lowercasing turned off. Explicit beats the
    # per-platform default, including an explicit false.
    resolver, _ = _resolver(convert_urns_to_lowercase=False)

    urn = resolver.dataset_urn(_jdbc("snowflake"), _table("ORDERS"))

    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES.PUBLIC.ORDERS,PROD)"
    )


def test_column_paths_are_cased_the_way_the_dataset_name_is() -> None:
    # Found on a live DataHub: its Snowflake source stored `c_custkey`, we emitted
    # `C_CUSTKEY`, and every field profile and column-level assertion missed.
    resolver, _ = _resolver()
    snowflake, postgres = _jdbc("snowflake"), _jdbc("postgresql", id=2, name="pg")

    assert resolver.field_path(snowflake, "C_CUSTKEY") == "c_custkey"
    assert resolver.field_path(postgres, "MixedCase") == "MixedCase"


def test_column_paths_keep_their_case_when_lowercasing_is_turned_off() -> None:
    resolver, _ = _resolver(convert_urns_to_lowercase=False)

    assert resolver.field_path(_jdbc("snowflake"), "C_CUSTKEY") == "C_CUSTKEY"


def test_per_datastore_casing_overrides_the_top_level_setting() -> None:
    # Real mixed estates: one warehouse ingested with lowercasing, another without.
    resolver, _ = _resolver(
        convert_urns_to_lowercase=True,
        datastore_to_platform_map={
            "warehouse": {"platform": "snowflake", "convert_urns_to_lowercase": False}
        },
    )

    urn = resolver.dataset_urn(_jdbc("snowflake"), _table("ORDERS"))

    assert urn is not None
    assert "SALES.PUBLIC.ORDERS" in urn


def test_the_qualytics_deployment_instance_never_leaks_into_the_source_urn() -> None:
    # platform_instance identifies the *Qualytics* deployment; it namespaces assertion
    # URNs. Pushing it into an inferred Snowflake dataset URN would target
    # `snowflake,acme.SALES.PUBLIC.ORDERS` -- a dataset their warehouse source never
    # emitted, so every assertion would land nowhere.
    resolver, _ = _resolver(platform_instance="acme-qualytics")

    urn = resolver.dataset_urn(_jdbc("snowflake"), _table("ORDERS"))

    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.public.orders,PROD)"
    )


def test_the_warehouse_platform_instance_is_configured_separately() -> None:
    resolver, _ = _resolver(
        platform_instance="acme-qualytics", default_source_platform_instance="prod-sf"
    )

    urn = resolver.dataset_urn(_jdbc("snowflake"), _table("ORDERS"))

    assert urn is not None
    assert "prod-sf.sales.public.orders" in urn
    assert "acme-qualytics" not in urn


def test_platform_resolution_is_memoised_per_datastore() -> None:
    # source.py's comment promised this. Without it resolve_platform re-ran for every
    # container, and the single-warning guarantee rested on the caller remembering to
    # skip rather than on the resolver.
    resolver, report = _resolver(infer_source_platform=False)
    datastore = _jdbc("snowflake")

    for _ in range(5):
        resolver.resolve_platform(datastore)

    assert report.datastores_unresolved == 1
    assert len(list(report.unresolved_datastores)) == 1


def test_a_typo_in_default_source_env_is_rejected() -> None:
    # It feeds straight into the URN. An unvalidated typo yields a well-formed URN
    # pointing at nothing -- the same invisible failure platform validation prevents.
    with pytest.raises(ValidationError, match="default_source_env"):
        QualyticsSourceConfig.model_validate(
            {
                "base_url": "https://acme.qualytics.io/api",
                "token": "t",
                "default_source_env": "prdo-typo",
            }
        )


def test_ui_base_url_without_a_scheme_is_rejected() -> None:
    # It is concatenated into the deep link on every emitted entity.
    with pytest.raises(ValidationError, match="scheme"):
        QualyticsSourceConfig.model_validate(
            {
                "base_url": "https://acme.qualytics.io/api",
                "token": "t",
                "ui_base_url": "acme.qualytics.io",
            }
        )
