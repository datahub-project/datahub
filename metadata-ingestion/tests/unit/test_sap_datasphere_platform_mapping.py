from typing import Dict

from datahub.ingestion.source.sap_datasphere.config import (
    _BUILTIN_PLATFORM_TYPE_DEFAULTS,
    MANAGED_CONNECTION_KEY,
    ConnectionPlatformConfig,
    SapDatasphereConfig,
)
from datahub.ingestion.source.sap_datasphere.platform_mapping import (
    ConnectionRecord,
    PlatformMappingResolver,
    ResolvedPlatform,  # noqa: F401 – exported symbol, useful for type hints in call sites
    ResolveSkipReason,
)
from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport


def test_builtin_platform_type_defaults_cover_observed_typeids():
    expected = {
        "HANA",
        "MSSQL",
        "S3",
        "GCS",
        "ABAP",
        "SAPS4HANACLOUD",
        "SAPBWMODELTRANSFER",
    }
    assert expected.issubset(set(_BUILTIN_PLATFORM_TYPE_DEFAULTS.keys()))


def test_config_accepts_custom_per_connection_map():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "connection_to_platform_map": {
                "_managed": {"platform": "hana", "platform_instance": "prod_hana"},
                "SNOWFLAKE_PROD": {
                    "platform": "snowflake",
                    "platform_instance": "acct_xyz",
                },
            },
        }
    )
    assert cfg.connection_to_platform_map["_managed"].platform_instance == "prod_hana"
    sf = cfg.connection_to_platform_map["SNOWFLAKE_PROD"]
    assert sf.platform == "snowflake"
    assert sf.platform_instance == "acct_xyz"


def test_config_accepts_custom_platform_type_defaults_override():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "platform_type_defaults": {
                "SNOWFLAKE": {"platform": "snowflake", "platform_instance": "acct"},
                "S3": {"platform": "s3", "enabled": False},
            },
        }
    )
    # Built-in defaults preserved for typeIds not overridden
    assert cfg.platform_type_defaults["HANA"].platform == "hana"
    # User overrides win
    assert cfg.platform_type_defaults["S3"].enabled is False
    assert cfg.platform_type_defaults["SNOWFLAKE"].platform_instance == "acct"


# ---------------------------------------------------------------------------
# PlatformMappingResolver tests
# ---------------------------------------------------------------------------


def _config_with(map_overrides=None, type_defaults_overrides=None):
    cfg_dict = {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    if map_overrides:
        cfg_dict["connection_to_platform_map"] = map_overrides
    if type_defaults_overrides:
        cfg_dict["platform_type_defaults"] = type_defaults_overrides
    return SapDatasphereConfig.model_validate(cfg_dict)


def test_resolver_managed_default_returns_sap_datasphere():
    """The synthetic `_managed` key always resolves to the sap-datasphere
    platform — managed assets are Datasphere assets, not HANA assets."""
    cfg = _config_with()
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    resolved, reason = resolver.resolve("_managed")
    assert resolved is not None
    assert reason is None
    assert resolved.platform == "sap-datasphere"


def test_resolver_explicit_map_overrides_typeid_default():
    """Per-connection entries in connection_to_platform_map override typeId
    defaults for federated connections."""
    cfg = _config_with(
        map_overrides={
            "SF_PROD": {"platform": "snowflake", "platform_instance": "custom"},
        }
    )
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "HANA"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved, _ = resolver.resolve("SF_PROD")
    assert resolved is not None
    assert resolved.platform == "snowflake"
    assert resolved.platform_instance == "custom"


def test_resolver_named_connection_falls_back_to_typeid_default():
    cfg = _config_with()
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "S3"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved, _ = resolver.resolve("SF_PROD")
    assert resolved is not None
    assert resolved.platform == "s3"


def test_resolver_disabled_returns_none():
    """A federated connection marked enabled=False resolves to DISABLED skip."""
    cfg = _config_with(
        map_overrides={"SF_PROD": {"platform": "snowflake", "enabled": False}}
    )
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "S3"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved, reason = resolver.resolve("SF_PROD")
    assert resolved is None
    assert reason == ResolveSkipReason.DISABLED


def test_resolver_unknown_typeid_returns_none_and_records_warning():
    cfg = _config_with()
    connections: Dict[str, ConnectionRecord] = {
        "X": {"name": "X", "typeId": "SNOWFLAKE"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved, reason = resolver.resolve("X")
    assert resolved is None
    assert reason == ResolveSkipReason.UNKNOWN_TYPEID
    assert "SNOWFLAKE" in "\n".join(resolver.unknown_typeids_seen)


def test_resolver_unknown_connection_name_returns_unknown_connection_reason():
    cfg = _config_with()
    # Empty connections list — the asset references "MYSTERY" which the API never reported.
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    resolved, reason = resolver.resolve("MYSTERY")
    assert resolved is None
    assert reason == ResolveSkipReason.UNKNOWN_CONNECTION


def test_resolver_env_falls_back_to_connector_env():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "env": "DEV",
        }
    )
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    resolved, _ = resolver.resolve("_managed")
    assert resolved is not None
    assert resolved.env == "DEV"


def test_resolver_env_explicit_in_map_wins_over_connector_env():
    """An explicit `env` in a federated connection_to_platform_map entry
    overrides the connector-level env."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "env": "DEV",
            "connection_to_platform_map": {
                "SF_PROD": {"platform": "snowflake", "env": "PROD"},
            },
        }
    )
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "S3"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved, _ = resolver.resolve("SF_PROD")
    assert resolved is not None
    assert resolved.env == "PROD"


def test_resolver_unknown_typeid_emits_report_warning():
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    connections: Dict[str, ConnectionRecord] = {
        "X": {"name": "X", "typeId": "SNOWFLAKE"}
    }
    resolver = PlatformMappingResolver(
        cfg, connections_by_name=connections, report=report
    )

    resolved, reason = resolver.resolve("X")
    assert resolved is None
    assert reason == ResolveSkipReason.UNKNOWN_TYPEID
    # report.warnings is a list of StructuredLogEntry; each has a `.message` attribute.
    warning_messages = [w.message for w in report.warnings]
    assert any("SNOWFLAKE" in m for m in warning_messages), (
        f"Expected unknown-typeId warning in report; got: {warning_messages}"
    )


def test_resolver_unknown_typeid_warning_deduplicated_in_report():
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    connections: Dict[str, ConnectionRecord] = {
        "X1": {"name": "X1", "typeId": "BIGQUERY"},
        "X2": {"name": "X2", "typeId": "BIGQUERY"},
        "X3": {"name": "X3", "typeId": "BIGQUERY"},
    }
    resolver = PlatformMappingResolver(
        cfg, connections_by_name=connections, report=report
    )
    # All three resolve calls must return the UNKNOWN_TYPEID skip reason.
    for name in ("X1", "X2", "X3"):
        resolved, reason = resolver.resolve(name)
        assert resolved is None
        assert reason == ResolveSkipReason.UNKNOWN_TYPEID
    # Only ONE report warning for BIGQUERY despite 3 calls
    bigquery_warnings = [w for w in report.warnings if "BIGQUERY" in w.message]
    assert len(bigquery_warnings) == 1, (
        f"Expected exactly 1 deduplicated warning; got {len(bigquery_warnings)}"
    )


def test_managed_connection_resolves_to_sap_datasphere_regardless_of_config():
    """The synthetic `_managed` key always emits on the Datasphere platform.
    A user-provided `_managed: hana` override is ignored — managed assets are
    Datasphere assets, not HANA assets.
    """
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
        platform_instance="acme_tenant",
        connection_to_platform_map={
            MANAGED_CONNECTION_KEY: ConnectionPlatformConfig(
                platform="hana", platform_instance="ignored"
            ),
        },
    )
    resolver = PlatformMappingResolver(config, connections_by_name={})
    resolved, reason = resolver.resolve(MANAGED_CONNECTION_KEY)
    assert reason is None
    assert resolved is not None
    assert resolved.platform == "sap-datasphere"
    assert resolved.platform_instance == "acme_tenant"
    assert resolved.env == "PROD"


def test_managed_connection_inherits_top_level_platform_instance():
    """Managed assets' platform_instance comes from top-level config.platform_instance,
    not from any `_managed` entry."""
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
        platform_instance="tenant_eu_prod",
        connection_to_platform_map={},
    )
    resolver = PlatformMappingResolver(config, connections_by_name={})
    resolved, _ = resolver.resolve(MANAGED_CONNECTION_KEY)
    assert resolved is not None
    assert resolved.platform_instance == "tenant_eu_prod"


def test_managed_can_be_disabled_via_explicit_override():
    """Users who want to skip Datasphere's managed assets entirely can set
    `_managed: {enabled: false}` — the only field still honored on `_managed`."""
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
        connection_to_platform_map={
            MANAGED_CONNECTION_KEY: ConnectionPlatformConfig(
                platform="hana", enabled=False
            ),
        },
    )
    resolver = PlatformMappingResolver(config, connections_by_name={})
    resolved, reason = resolver.resolve(MANAGED_CONNECTION_KEY)
    assert resolved is None
    assert reason == ResolveSkipReason.DISABLED


def test_federated_connection_unchanged():
    """Federated connections still route via connection_to_platform_map —
    only `_managed` short-circuits to sap-datasphere."""
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
        connection_to_platform_map={
            "MY_SF": ConnectionPlatformConfig(
                platform="snowflake", platform_instance="acct_xyz"
            ),
        },
    )
    resolver = PlatformMappingResolver(
        config,
        connections_by_name={"MY_SF": {"name": "MY_SF", "typeId": "SNOWFLAKE"}},
    )
    resolved, _ = resolver.resolve("MY_SF")
    assert resolved is not None
    assert resolved.platform == "snowflake"
    assert resolved.platform_instance == "acct_xyz"
