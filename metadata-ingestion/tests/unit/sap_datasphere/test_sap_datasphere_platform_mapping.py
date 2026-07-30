from typing import Dict

from datahub.ingestion.source.sap_datasphere.config import (
    _BUILTIN_PLATFORM_TYPE_DEFAULTS,
    ConnectionPlatformConfig,
    SapDatasphereConfig,
)
from datahub.ingestion.source.sap_datasphere.constants import MANAGED_CONNECTION_KEY
from datahub.ingestion.source.sap_datasphere.models import (
    ConnectionRecord,
    ResolvedPlatform,  # noqa: F401 – exported symbol, useful for type hints in call sites
    ResolveSkipReason,
)
from datahub.ingestion.source.sap_datasphere.platform_mapping import (
    PlatformMappingResolver,
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
        "BIGQUERY",
    }
    assert expected.issubset(set(_BUILTIN_PLATFORM_TYPE_DEFAULTS.keys()))
    # BigQuery must stitch to DataHub's `bigquery` platform (typeId token confirmed from a live tenant warning).
    assert _BUILTIN_PLATFORM_TYPE_DEFAULTS["BIGQUERY"].platform == "bigquery"


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
    assert cfg.platform_type_defaults["HANA"].platform == "hana"
    assert cfg.platform_type_defaults["S3"].enabled is False
    assert cfg.platform_type_defaults["SNOWFLAKE"].platform_instance == "acct"


def _config_with(map_overrides=None, type_defaults_overrides=None):
    cfg_dict = {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    if map_overrides:
        cfg_dict["connection_to_platform_map"] = map_overrides
    if type_defaults_overrides:
        cfg_dict["platform_type_defaults"] = type_defaults_overrides
    return SapDatasphereConfig.model_validate(cfg_dict)


def test_resolver_managed_default_returns_sap_datasphere():
    """The synthetic _managed key always resolves to sap-datasphere — managed assets are Datasphere assets, not HANA."""
    cfg = _config_with()
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    result = resolver.resolve("_managed")
    resolved = result.platform
    reason = result.skip_reason
    assert resolved is not None
    assert reason is None
    assert resolved.platform == "sap-datasphere"


def test_resolver_explicit_map_overrides_typeid_default():
    cfg = _config_with(
        map_overrides={
            "SF_PROD": {"platform": "snowflake", "platform_instance": "custom"},
        }
    )
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "HANA"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved = resolver.resolve("SF_PROD").platform
    assert resolved is not None
    assert resolved.platform == "snowflake"
    assert resolved.platform_instance == "custom"


def test_resolver_named_connection_falls_back_to_typeid_default():
    cfg = _config_with()
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "S3"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    resolved = resolver.resolve("SF_PROD").platform
    assert resolved is not None
    assert resolved.platform == "s3"


def test_resolver_disabled_returns_none():
    cfg = _config_with(
        map_overrides={"SF_PROD": {"platform": "snowflake", "enabled": False}}
    )
    connections: Dict[str, ConnectionRecord] = {
        "SF_PROD": {"name": "SF_PROD", "typeId": "S3"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    result = resolver.resolve("SF_PROD")
    resolved = result.platform
    reason = result.skip_reason
    assert resolved is None
    assert reason == ResolveSkipReason.DISABLED


def test_resolver_unknown_typeid_returns_none_and_records_warning():
    cfg = _config_with()
    connections: Dict[str, ConnectionRecord] = {
        "X": {"name": "X", "typeId": "SNOWFLAKE"}
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    result = resolver.resolve("X")
    resolved = result.platform
    reason = result.skip_reason
    assert resolved is None
    assert reason == ResolveSkipReason.UNKNOWN_TYPEID
    assert "SNOWFLAKE" in "\n".join(resolver.unknown_typeids_seen)


def test_resolver_unknown_connection_name_returns_unknown_connection_reason():
    cfg = _config_with()
    # Empty connections list — the asset references a connection the API never reported.
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    result = resolver.resolve("MYSTERY")
    resolved = result.platform
    reason = result.skip_reason
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
    resolved = resolver.resolve("_managed").platform
    assert resolved is not None
    assert resolved.env == "DEV"


def test_resolver_env_explicit_in_map_wins_over_connector_env():
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
    resolved = resolver.resolve("SF_PROD").platform
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

    result = resolver.resolve("X")
    resolved = result.platform
    reason = result.skip_reason
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
    # Mixed casing of the same typeId — the dedup key is case-folded, so KAFKA/kafka must still warn once.
    connections: Dict[str, ConnectionRecord] = {
        "X1": {"name": "X1", "typeId": "KAFKA"},
        "X2": {"name": "X2", "typeId": "kafka"},
        "X3": {"name": "X3", "typeId": "Kafka"},
    }
    resolver = PlatformMappingResolver(
        cfg, connections_by_name=connections, report=report
    )
    for name in ("X1", "X2", "X3"):
        result = resolver.resolve(name)
        assert result.platform is None
        assert result.skip_reason == ResolveSkipReason.UNKNOWN_TYPEID
    kafka_warnings = [w for w in report.warnings if "kafka" in w.message.lower()]
    assert len(kafka_warnings) == 1, (
        f"Expected exactly 1 deduplicated warning; got {len(kafka_warnings)}"
    )
    assert resolver.unknown_typeids_seen == {"KAFKA"}


def test_builtin_typeid_default_resolves_with_builtin_casing():
    """Cross-connector URN stitching depends on the casing a federated builtin resolves with; guard BIGQUERY's convert_urns_to_lowercase default."""
    cfg = _config_with()
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    resolved = resolver.resolve_external("NOT_IN_LIST", "BIGQUERY").platform
    assert resolved is not None
    assert resolved.platform == "bigquery"
    assert resolved.convert_urns_to_lowercase is True
    assert _BUILTIN_PLATFORM_TYPE_DEFAULTS["BIGQUERY"].convert_urns_to_lowercase is True


def test_managed_connection_resolves_to_sap_datasphere_regardless_of_config():
    """The synthetic _managed key always emits on Datasphere; a user _managed: hana override is ignored."""
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
    result = resolver.resolve(MANAGED_CONNECTION_KEY)
    resolved = result.platform
    reason = result.skip_reason
    assert reason is None
    assert resolved is not None
    assert resolved.platform == "sap-datasphere"
    assert resolved.platform_instance == "acme_tenant"
    assert resolved.env == "PROD"


def test_managed_connection_inherits_top_level_platform_instance():
    """Managed assets' platform_instance comes from top-level config, not any _managed entry."""
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
        platform_instance="tenant_eu_prod",
        connection_to_platform_map={},
    )
    resolver = PlatformMappingResolver(config, connections_by_name={})
    resolved = resolver.resolve(MANAGED_CONNECTION_KEY).platform
    assert resolved is not None
    assert resolved.platform_instance == "tenant_eu_prod"


def test_managed_can_be_disabled_via_explicit_override():
    """enabled=false is the only field still honored on _managed, letting users skip managed assets entirely."""
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
    result = resolver.resolve(MANAGED_CONNECTION_KEY)
    resolved = result.platform
    reason = result.skip_reason
    assert resolved is None
    assert reason == ResolveSkipReason.DISABLED


def test_federated_connection_unchanged():
    """Federated connections still route via connection_to_platform_map — only _managed short-circuits."""
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
    resolved = resolver.resolve("MY_SF").platform
    assert resolved is not None
    assert resolved.platform == "snowflake"
    assert resolved.platform_instance == "acct_xyz"


def test_resolve_external_uses_connection_type_when_name_absent():
    """An endpoint names a connectionId absent from the connections list; its connectionType resolves it via type defaults."""
    cfg = _config_with()
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    result = resolver.resolve_external("NOT_IN_LIST", "S3")
    resolved = result.platform
    reason = result.skip_reason
    assert resolved is not None
    assert reason is None
    assert resolved.platform == "s3"


def test_resolve_external_prefers_explicit_name_map_over_type():
    cfg = _config_with(
        map_overrides={"SRC_CONN": {"platform": "snowflake", "platform_instance": "a"}}
    )
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    # connectionType would map to S3, but the explicit name mapping wins.
    resolved = resolver.resolve_external("SRC_CONN", "S3").platform
    assert resolved is not None
    assert resolved.platform == "snowflake"
    assert resolved.platform_instance == "a"


def test_resolve_external_propagates_database_and_lowercase_override():
    """A per-connection entry can carry both a lowercase override and an explicit database (e.g. the BigQuery project); both must reach ResolvedPlatform for stitching."""
    cfg = _config_with(
        map_overrides={
            "BQ_CONN": {
                "platform": "bigquery",
                "convert_urns_to_lowercase": False,
                "database": "my-gcp-project",
            }
        }
    )
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    resolved = resolver.resolve_external("BQ_CONN", "BIGQUERY").platform
    assert resolved is not None
    assert resolved.platform == "bigquery"
    assert resolved.database == "my-gcp-project"
    assert resolved.convert_urns_to_lowercase is False


def test_resolve_external_unknown_type_and_name_returns_none():
    cfg = _config_with()
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    result = resolver.resolve_external("MYSTERY", "SNOWFLAKE")
    resolved = result.platform
    reason = result.skip_reason
    assert resolved is None
    assert reason == ResolveSkipReason.UNKNOWN_CONNECTION


def test_resolve_external_disabled_type_default_returns_none():
    cfg = _config_with(
        type_defaults_overrides={"S3": {"platform": "s3", "enabled": False}}
    )
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    result = resolver.resolve_external(None, "S3")
    # A disabled type default with no name to fall back on is unresolved with reason DISABLED, not UNKNOWN_CONNECTION.
    assert result.platform is None
    assert result.skip_reason == ResolveSkipReason.DISABLED


def test_resolve_external_disabled_type_default_reports_disabled_not_unknown():
    """Regression: a disabled type default with an unknown connection name must report DISABLED, not fall through to UNKNOWN_CONNECTION."""
    cfg = _config_with(
        type_defaults_overrides={"S3": {"platform": "s3", "enabled": False}}
    )
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    result = resolver.resolve_external("NOT_IN_LIST", "S3")
    assert result.platform is None
    assert result.skip_reason == ResolveSkipReason.DISABLED


def test_resolve_external_connection_type_is_case_insensitive():
    """A flow endpoint may report connectionType in any case; it must still match the uppercase platform_type_defaults key."""
    cfg = _config_with()
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    for connection_type in ("bigquery", "BigQuery", "BIGQUERY"):
        resolved = resolver.resolve_external("NOT_IN_LIST", connection_type).platform
        assert resolved is not None, connection_type
        assert resolved.platform == "bigquery"


def test_typeid_default_matches_regardless_of_typeid_case():
    """The connections-list typeId can arrive lowercased; the resolver folds case before matching type defaults."""
    cfg = _config_with()
    connections = {
        "GBQ": ConnectionRecord(name="GBQ", typeId="bigquery"),
    }
    resolver = PlatformMappingResolver(cfg, connections_by_name=connections)
    result = resolver.resolve("GBQ")
    assert result.platform is not None
    assert result.platform.platform == "bigquery"
    assert not resolver.unknown_typeids_seen


def test_user_supplied_lowercase_type_default_key_matches_uppercase_typeid():
    """A user-supplied lowercased platform_type_defaults key must still match SAP's canonical uppercase typeId."""
    cfg = _config_with(type_defaults_overrides={"snowflake": {"platform": "snowflake"}})
    resolver = PlatformMappingResolver(cfg, connections_by_name={})
    resolved = resolver.resolve_external("NOT_IN_LIST", "SNOWFLAKE").platform
    assert resolved is not None
    assert resolved.platform == "snowflake"
