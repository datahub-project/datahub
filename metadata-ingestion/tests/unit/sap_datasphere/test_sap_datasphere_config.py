import warnings

import pytest

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.source.sap_datasphere.config import (
    ConnectionPlatformConfig,
    SapDatasphereConfig,
    SpaceContainerKey,
)


def test_xsuaa_url_derived_from_base_url():
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "t"}
    )
    assert cfg.xsuaa_url == "https://myco.authentication.eu10.hana.ondemand.com"


def test_xsuaa_url_explicit_overrides_derived():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "t",
            "xsuaa_url": "https://custom.auth.example.com",
        }
    )
    assert cfg.xsuaa_url == "https://custom.auth.example.com"


def test_base_url_trailing_slash_stripped():
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap/", "token": "t"}
    )
    assert not cfg.base_url.endswith("/")


def test_base_url_without_scheme_rejected():
    """Reject a scheme-less base_url at load time; otherwise it fails far downstream as a confusing OAuth error."""
    with pytest.raises(ValueError, match="base_url must start with"):
        SapDatasphereConfig.model_validate(
            {"base_url": "myco.eu10.hcs.cloud.sap", "token": "t"}
        )


def test_tenant_url_backcompat_alias_maps_to_base_url():
    cfg = SapDatasphereConfig.model_validate(
        {"tenant_url": "https://myco.eu10.hcs.cloud.sap", "token": "t"}
    )
    assert cfg.base_url == "https://myco.eu10.hcs.cloud.sap"


def test_space_container_key_guid_is_stable():
    k1 = SpaceContainerKey(platform="sap-datasphere", space="DEMO_SPACE")
    k2 = SpaceContainerKey(platform="sap-datasphere", space="DEMO_SPACE")
    assert k1.guid() == k2.guid()


def test_space_container_key_different_spaces_differ():
    k1 = SpaceContainerKey(platform="sap-datasphere", space="SPACE_A")
    k2 = SpaceContainerKey(platform="sap-datasphere", space="SPACE_B")
    assert k1.guid() != k2.guid()


def test_connection_platform_config_rejects_invalid_env():
    with pytest.raises(ValueError, match="env must be one of"):
        ConnectionPlatformConfig(platform="hana", env="banana")


def test_connection_platform_config_normalizes_env_case():
    # Mirrors EnvConfigMixin so a per-connection env behaves like the top-level env.
    cfg = ConnectionPlatformConfig(platform="hana", env="prod")
    assert cfg.env == "PROD"


def test_platform_rejects_uppercase():
    with pytest.raises(ValueError, match="lowercase"):
        ConnectionPlatformConfig(platform="Snowflake")


def test_platform_rejects_empty_string():
    with pytest.raises(ValueError, match="non-empty"):
        ConnectionPlatformConfig(platform="")
    with pytest.raises(ValueError, match="non-empty"):
        ConnectionPlatformConfig(platform="   ")


def test_platform_accepts_lowercase_with_hyphen():
    cfg = ConnectionPlatformConfig(platform="sap-hana")
    assert cfg.platform == "sap-hana"
    cfg2 = ConnectionPlatformConfig(platform="my_custom_platform")
    assert cfg2.platform == "my_custom_platform"


def test_refresh_token_requires_xsuaa_url():
    with pytest.raises(ValueError, match="xsuaa_url"):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.example.com",  # not derivable
                "refresh_token": "r",
                "client_id": "c",
            }
        )


def test_refresh_token_requires_client_id():
    with pytest.raises(ValueError, match="client_id"):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.eu10.hcs.cloud.sap",
                "refresh_token": "r",
            }
        )


def test_client_secret_requires_xsuaa_url():
    with pytest.raises(ValueError, match="xsuaa_url"):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.example.com",  # not derivable
                "client_id": "c",
                "client_secret": "s",
            }
        )


def test_stateful_lineage_field_absent():
    """The connector emits full lineage from CSN, so the misleading no-op stateful-lineage knob must not exist."""
    assert "enable_stateful_lineage_ingestion" not in SapDatasphereConfig.model_fields


def test_stale_entity_removal_still_works_without_lineage_mixin():
    """Stale-entity soft-delete comes from StatefulIngestionConfigBase, not the lineage mixin."""
    cfg = SapDatasphereConfig(
        base_url="https://myco.eu10.hcs.cloud.sap",
        token="t",
        stateful_ingestion={"enabled": True},
    )
    assert cfg.stateful_ingestion is not None
    assert cfg.stateful_ingestion.enabled is True


def test_include_table_lineage_backcompat_alias_maps_to_include_lineage():
    """Legacy include_table_lineage maps to include_lineage and emits a ConfigurationWarning."""
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        cfg = SapDatasphereConfig.model_validate(
            {
                "base_url": "https://test.eu10.hcs.cloud.sap",
                "token": "t",
                "include_table_lineage": True,
            }
        )
    assert cfg.include_lineage is True

    # pydantic_renamed_field emits the deprecation via warnings.warn(..., ConfigurationWarning).
    relevant = [
        w
        for w in captured
        if issubclass(w.category, ConfigurationWarning)
        and "include_table_lineage" in str(w.message)
    ]
    assert relevant, (
        f"Expected a ConfigurationWarning mentioning 'include_table_lineage'; "
        f"got: {[(w.category.__name__, str(w.message)) for w in captured]}"
    )
    assert "include_lineage" in str(relevant[0].message)


def test_connection_to_platform_map_defaults_to_empty():
    """Managed assets resolve to sap-datasphere via the resolver short-circuit; this map is only for federated routing."""
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
    )
    assert config.connection_to_platform_map == {}
