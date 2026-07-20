from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sap_mdg.config import SapMdgSourceConfig
from datahub.ingestion.source.sap_mdg.source import SapMdgSource


def _source(**overrides: object) -> SapMdgSource:
    config = SapMdgSourceConfig.model_validate(
        {
            "base_url": "https://sap-gw.example.com:44300",
            "services": ["/sap/opu/odata/sap/ZMDG_DEMO_SRV"],
            "token": "abc",
            "env": "PROD",
            **overrides,
        }
    )
    return SapMdgSource(config, PipelineContext(run_id="test"))


def test_configured_mapping_wins_over_known_fallback():
    source = _source(
        logical_system_to_platform={
            "hana": {"platform": "snowflake", "platform_instance": "prod", "env": "DEV"}
        }
    )
    # "hana" is also a known-fallback key, but the explicit config must take priority.
    urn = source._target_dataset_urn("hana", "DB.SCHEMA.Customer")
    assert urn == make_dataset_urn_with_platform_instance(
        platform="snowflake",
        name="db.schema.customer",
        platform_instance="prod",
        env="DEV",
    )


def test_known_fallback_used_when_unmapped():
    detail = _source()._target_platform_detail("S4HANA")
    assert detail is not None
    assert detail.platform == "hana"


def test_unknown_logical_system_is_unresolved():
    assert _source()._target_dataset_urn("Z_CUSTOM_SYS", "Anything") is None


def test_casing_preserved_when_lowercase_disabled():
    source = _source(
        logical_system_to_platform={
            "SYS": {"platform": "mssql", "convert_urns_to_lowercase": False}
        }
    )
    urn = source._target_dataset_urn("SYS", "MixedCase")
    assert urn == make_dataset_urn_with_platform_instance(
        platform="mssql", name="MixedCase", platform_instance=None, env="PROD"
    )


def test_target_env_defaults_to_source_env():
    source = _source(
        env="STG", logical_system_to_platform={"SYS": {"platform": "hana"}}
    )
    urn = source._target_dataset_urn("SYS", "t")
    assert urn == make_dataset_urn_with_platform_instance(
        platform="hana", name="t", platform_instance=None, env="STG"
    )
