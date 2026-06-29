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
    """A base_url missing the scheme would silently produce broken request URLs
    and an unmatched xsuaa_url derivation, surfacing far downstream as a confusing
    OAuth error. Reject it at config-load time with an actionable message."""
    with pytest.raises(ValueError, match="base_url must start with"):
        SapDatasphereConfig.model_validate(
            {"base_url": "myco.eu10.hcs.cloud.sap", "token": "t"}
        )


def test_tenant_url_backcompat_alias_maps_to_base_url():
    """The legacy `tenant_url` field name still works for back-compat."""
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
    with pytest.raises(ValueError, match="FabricType"):
        ConnectionPlatformConfig(platform="hana", env="banana")


def test_platform_rejects_uppercase():
    """L11: platform values must be kebab-case-ish; ``Snowflake`` is a typo."""
    with pytest.raises(ValueError, match="lowercase"):
        ConnectionPlatformConfig(platform="Snowflake")


def test_platform_rejects_empty_string():
    """L11: empty / whitespace platform values must raise."""
    with pytest.raises(ValueError, match="non-empty"):
        ConnectionPlatformConfig(platform="")
    with pytest.raises(ValueError, match="non-empty"):
        ConnectionPlatformConfig(platform="   ")


def test_platform_accepts_lowercase_with_hyphen():
    """L11: kebab-case platform names (e.g. ``sap-hana``) must validate."""
    cfg = ConnectionPlatformConfig(platform="sap-hana")
    assert cfg.platform == "sap-hana"
    cfg2 = ConnectionPlatformConfig(platform="my_custom_platform")
    assert cfg2.platform == "my_custom_platform"


def test_connection_platform_config_accepts_valid_env():
    cfg = ConnectionPlatformConfig(platform="hana", env="PROD")
    assert cfg.env == "PROD"


def test_connection_platform_config_accepts_none_env():
    cfg = ConnectionPlatformConfig(platform="hana", env=None)
    assert cfg.env is None


def test_refresh_token_requires_xsuaa_url():
    """M6: refresh_token without xsuaa_url must raise a clear validation error."""
    with pytest.raises(ValueError, match="xsuaa_url"):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.example.com",  # not derivable
                "refresh_token": "r",
                "client_id": "c",
                # xsuaa_url intentionally omitted and not derivable from base_url
            }
        )


def test_refresh_token_requires_client_id():
    """M6: refresh_token without client_id must raise a validation error."""
    with pytest.raises(ValueError, match="client_id"):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.eu10.hcs.cloud.sap",
                "refresh_token": "r",
                # client_id intentionally omitted
            }
        )


def test_client_secret_requires_xsuaa_url():
    """M6: client_secret without xsuaa_url must raise a validation error."""
    with pytest.raises(ValueError, match="xsuaa_url"):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.example.com",  # not derivable
                "client_id": "c",
                "client_secret": "s",
                # xsuaa_url intentionally omitted and not derivable from base_url
            }
        )


def test_request_timeout_sec_rejects_zero():
    """L8: ``request_timeout_sec`` is bounded `ge=1` — zero must raise."""
    with pytest.raises(ValueError):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.eu10.hcs.cloud.sap",
                "token": "t",
                "request_timeout_sec": 0,
            }
        )


def test_max_retries_rejects_negative():
    """L8: ``max_retries`` is bounded `ge=0` — negative values must raise."""
    with pytest.raises(ValueError):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.eu10.hcs.cloud.sap",
                "token": "t",
                "max_retries": -1,
            }
        )


def test_stateful_lineage_field_absent():
    """The connector emits full lineage from CSN and never consumes
    enable_stateful_lineage_ingestion, so StatefulLineageConfigMixin (and its
    misleading no-op knob) must not be part of the config."""
    assert "enable_stateful_lineage_ingestion" not in SapDatasphereConfig.model_fields


def test_stale_entity_removal_still_works_without_lineage_mixin():
    """Stale-entity soft-delete comes from StatefulIngestionConfigBase, not the
    lineage mixin — a config with stateful_ingestion still constructs."""
    cfg = SapDatasphereConfig(
        base_url="https://myco.eu10.hcs.cloud.sap",
        token="t",
        stateful_ingestion={"enabled": True},
    )
    assert cfg.stateful_ingestion is not None
    assert cfg.stateful_ingestion.enabled is True


def test_include_table_lineage_backcompat_alias_maps_to_include_lineage():
    """The legacy `include_table_lineage` field name still maps to include_lineage
    AND emits a ConfigurationWarning so operators see the deprecation message."""
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        cfg = SapDatasphereConfig.model_validate(
            {
                "base_url": "https://test.eu10.hcs.cloud.sap",
                "token": "t",
                "include_table_lineage": True,  # legacy name
            }
        )
    assert cfg.include_lineage is True

    # `pydantic_renamed_field` calls warnings.warn(..., ConfigurationWarning).
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
    """The `_managed: hana` default is gone — managed assets resolve to
    sap-datasphere via the resolver short-circuit, not via this config field.
    Customers only populate connection_to_platform_map for FEDERATED routing.
    """
    config = SapDatasphereConfig(
        base_url="https://example.com",
        token="t",
    )
    assert config.connection_to_platform_map == {}


def test_config_asset_batch_size_bounds_rejected():
    """asset_batch_size is bounded ge=1, le=100000 — out-of-range must raise.

    (The default value and the lowercase-URN behavior are covered behaviorally
    in test_sap_datasphere_source.py, so no default-assertion test here.)"""
    with pytest.raises(ValueError):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.eu10.hcs.cloud.sap",
                "token": "t",
                "asset_batch_size": 0,
            }
        )

    with pytest.raises(ValueError):
        SapDatasphereConfig.model_validate(
            {
                "base_url": "https://myco.eu10.hcs.cloud.sap",
                "token": "t",
                "asset_batch_size": 100001,
            }
        )
