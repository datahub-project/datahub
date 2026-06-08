"""Unit tests for bigid_config: BigIDSourceConfig validation and BIGID_TYPE_TO_PLATFORM."""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.bigid.bigid_config import (
    BIGID_TYPE_TO_PLATFORM,
    BigIDSourceConfig,
)

# ---------------------------------------------------------------------------
# BIGID_TYPE_TO_PLATFORM
# ---------------------------------------------------------------------------


def test_bigid_type_to_platform_mapping_invariants():
    """Structural invariants on BIGID_TYPE_TO_PLATFORM — no blanks, canonical spot-checks."""
    # All values must be non-empty strings
    for bigid_type, platform in BIGID_TYPE_TO_PLATFORM.items():
        assert isinstance(platform, str) and platform, (
            f"Mapping entry {bigid_type!r} has blank/invalid platform: {platform!r}"
        )

    # Spot-check a representative set of canonical BigID connection types that are
    # known to be used in the field — these assert specific values, not just presence
    spot_checks = {
        "snowflake": "snowflake",
        "rdb-postgresql": "postgres",
        "gcp-big-query": "bigquery",
        "sharepoint-online-v2": "sharepoint",
    }
    for bigid_type, expected_platform in spot_checks.items():
        assert BIGID_TYPE_TO_PLATFORM.get(bigid_type) == expected_platform, (
            f"Canonical mapping changed: {bigid_type!r} should be {expected_platform!r}, "
            f"got {BIGID_TYPE_TO_PLATFORM.get(bigid_type)!r}"
        )


# ---------------------------------------------------------------------------
# BigIDSourceConfig validation
# ---------------------------------------------------------------------------


def test_config_requires_token():
    with pytest.raises(ValidationError, match="user_token or access_token"):
        BigIDSourceConfig.model_validate({"bigid_url": "https://bigid.example.com"})


def test_config_access_token_accepted():
    cfg = BigIDSourceConfig.model_validate(
        {"bigid_url": "https://bigid.example.com", "access_token": "tok"}
    )
    assert cfg.access_token.get_secret_value() == "tok"


def test_config_user_token_accepted():
    cfg = BigIDSourceConfig.model_validate(
        {"bigid_url": "https://bigid.example.com", "user_token": "long-tok"}
    )
    assert cfg.user_token.get_secret_value() == "long-tok"


# ---------------------------------------------------------------------------
# minimum_confidence_threshold validation
# ---------------------------------------------------------------------------


def test_confidence_threshold_below_zero_raises():
    with pytest.raises(ValidationError, match="minimum_confidence_threshold"):
        BigIDSourceConfig.model_validate({
            "bigid_url": "https://x.com", "access_token": "t",
            "minimum_confidence_threshold": -0.1,
        })


def test_confidence_threshold_above_one_raises():
    with pytest.raises(ValidationError, match="minimum_confidence_threshold"):
        BigIDSourceConfig.model_validate({
            "bigid_url": "https://x.com", "access_token": "t",
            "minimum_confidence_threshold": 1.5,
        })


def test_confidence_threshold_boundary_values_accepted():
    cfg_low = BigIDSourceConfig.model_validate({
        "bigid_url": "https://x.com", "access_token": "t",
        "minimum_confidence_threshold": 0.0,
    })
    cfg_high = BigIDSourceConfig.model_validate({
        "bigid_url": "https://x.com", "access_token": "t",
        "minimum_confidence_threshold": 1.0,
    })
    assert cfg_low.minimum_confidence_threshold == 0.0
    assert cfg_high.minimum_confidence_threshold == 1.0
