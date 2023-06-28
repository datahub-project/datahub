from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


def test_profile_table_level_only():
    config = GEProfilingConfig.parse_obj(
        {"enabled": True, "profile_table_level_only": True}
    )
    assert config.any_field_level_metrics_enabled() is False
