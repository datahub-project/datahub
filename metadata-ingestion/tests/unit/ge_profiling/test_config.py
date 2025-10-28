import pytest

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


def test_profile_table_level_only():
    config = GEProfilingConfig.model_validate(
        {"enabled": True, "profile_table_level_only": True}
    )
    assert config.any_field_level_metrics_enabled() is False

    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "profile_table_level_only": True,
            "include_field_max_value": False,
        }
    )
    assert config.any_field_level_metrics_enabled() is False


def test_profile_table_level_only_fails_with_field_metric_enabled():
    with pytest.raises(
        ValueError,
        match="Cannot enable field-level metrics if profile_table_level_only is set",
    ):
        GEProfilingConfig.model_validate(
            {
                "enabled": True,
                "profile_table_level_only": True,
                "include_field_max_value": True,
            }
        )


def test_tags_to_ignore_profiling_config():
    """Test that tags_to_ignore_profiling configuration is properly parsed."""
    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "tags_to_ignore_profiling": ["PII", "Sensitive"],
        }
    )
    assert config.tags_to_ignore_profiling == ["PII", "Sensitive"]

    # Test with None (default)
    config = GEProfilingConfig.model_validate({"enabled": True})
    assert config.tags_to_ignore_profiling is None


def test_tags_to_ignore_profiling_vs_sampling():
    """Test that both tags_to_ignore_profiling and tags_to_ignore_sampling can be configured."""
    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "tags_to_ignore_profiling": ["PII", "Sensitive"],
            "tags_to_ignore_sampling": ["HighCardinality"],
        }
    )
    assert config.tags_to_ignore_profiling == ["PII", "Sensitive"]
    assert config.tags_to_ignore_sampling == ["HighCardinality"]


def test_tags_to_ignore_profiling_empty_list():
    """Test that tags_to_ignore_profiling can be an empty list."""
    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "tags_to_ignore_profiling": [],
        }
    )
    assert config.tags_to_ignore_profiling == []


def test_tags_to_ignore_profiling_single_tag():
    """Test that tags_to_ignore_profiling works with a single tag."""
    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "tags_to_ignore_profiling": ["PII"],
        }
    )
    assert config.tags_to_ignore_profiling == ["PII"]


def test_tags_to_ignore_profiling_multiple_tags():
    """Test that tags_to_ignore_profiling works with multiple tags."""
    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "tags_to_ignore_profiling": [
                "PII",
                "Sensitive",
                "Confidential",
                "Internal",
            ],
        }
    )
    assert config.tags_to_ignore_profiling == [
        "PII",
        "Sensitive",
        "Confidential",
        "Internal",
    ]


def test_tags_to_ignore_profiling_with_other_config():
    """Test that tags_to_ignore_profiling works alongside other profiling configuration."""
    config = GEProfilingConfig.model_validate(
        {
            "enabled": True,
            "tags_to_ignore_profiling": ["PII"],
            "include_field_null_count": True,
            "include_field_distinct_count": False,
            "max_number_of_fields_to_profile": 100,
            "profile_table_size_limit": 5000000,
        }
    )
    assert config.tags_to_ignore_profiling == ["PII"]
    assert config.include_field_null_count is True
    assert config.include_field_distinct_count is False
    assert config.max_number_of_fields_to_profile == 100
    assert config.profile_table_size_limit == 5000000


def test_tags_to_ignore_profiling_validation():
    """Test validation of tags_to_ignore_profiling field."""
    # Test that non-list values are rejected
    with pytest.raises(ValueError):
        GEProfilingConfig.model_validate(
            {
                "enabled": True,
                "tags_to_ignore_profiling": "PII",  # Should be a list, not string
            }
        )

    # Test that non-string list items are rejected
    with pytest.raises(ValueError):
        GEProfilingConfig.model_validate(
            {
                "enabled": True,
                "tags_to_ignore_profiling": ["PII", 123],  # 123 is not a string
            }
        )
