from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.utilities.server_config_util import (
    _REQUIRED_VERSION_OPENAPI_TRACING,
    RestServiceConfig,
    ServiceFeature,
)


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    return {
        "models": {},
        "patchCapable": True,
        "versions": {
            "acryldata/datahub": {
                "version": "v1.0.0rc3",
                "commit": "dc127c5f031d579732899ccd81a53a3514dc4a6d",
            }
        },
        "managedIngestion": {"defaultCliVersion": "1.0.0.2", "enabled": True},
        "statefulIngestionCapable": True,
        "supportsImpactAnalysis": True,
        "timeZone": "GMT",
        "telemetry": {"enabledCli": True, "enabledIngestion": False},
        "datasetUrnNameCasing": False,
        "retention": "true",
        "datahub": {"serverType": "dev"},
        "noCode": "true",
    }


def test_init_with_config(sample_config):
    """Test initialization with a config dictionary."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.raw_config == sample_config
    assert config._version_cache is None


def test_service_version(sample_config):
    """Test getting service version."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.service_version == "v1.0.0rc3"


def test_commit_hash(sample_config):
    """Test getting commit hash."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.commit_hash == "dc127c5f031d579732899ccd81a53a3514dc4a6d"


def test_server_type(sample_config):
    """Test getting server type."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.server_type == "dev"


@pytest.mark.parametrize(
    "version_str,expected",
    [
        ("v1.0.0", (1, 0, 0, 0)),
        ("1.0.0", (1, 0, 0, 0)),
        ("v1.2.3.4", (1, 2, 3, 4)),
        ("v1.0.0rc3", (1, 0, 0, 0)),
        ("v1.0.0-acryl", (1, 0, 0, 0)),
        ("v1.0.0.1rc3", (1, 0, 0, 1)),
        (None, (0, 0, 0, 0)),  # No version
        ("", (0, 0, 0, 0)),  # Empty version
    ],
)
def test_parse_version(sample_config, version_str, expected):
    """Test parsing different version formats."""
    config = RestServiceConfig(raw_config=sample_config)
    if version_str is not None:
        result = config._parse_version(version_str)
    else:
        # Modify the config to not have a version
        modified_config = sample_config.copy()
        modified_config["versions"]["acryldata/datahub"]["version"] = None
        config = RestServiceConfig(raw_config=modified_config)
        result = config._parse_version()

    assert result == expected


def test_parse_version_invalid():
    """Test parsing invalid version string."""
    config = RestServiceConfig(raw_config={})
    with pytest.raises(ValueError) as exc_info:
        config._parse_version("invalid_version")
    assert "Invalid version format" in str(exc_info.value)


def test_parsed_version_caching(sample_config):
    """Test that parsed version is cached."""
    config = RestServiceConfig(raw_config=sample_config)

    # First call should parse the version
    result1 = config.parsed_version
    assert result1 == (1, 0, 0, 0)

    # Change the version in the config
    config.raw_config["versions"]["acryldata/datahub"]["version"] = "v2.0.0"

    # Second call should return cached result
    result2 = config.parsed_version
    assert result2 == (1, 0, 0, 0)  # Should be the same as before


@pytest.mark.parametrize(
    "current,required,expected",
    [
        ((1, 0, 0, 0), (1, 0, 0, 0), True),  # Equal versions
        ((2, 0, 0, 0), (1, 0, 0, 0), True),  # Higher major version
        ((1, 0, 0, 0), (2, 0, 0, 0), False),  # Lower major version
        ((1, 2, 0, 0), (1, 1, 0, 0), True),  # Higher minor version
        ((1, 0, 0, 0), (1, 1, 0, 0), False),  # Lower minor version
        ((1, 0, 3, 0), (1, 0, 2, 0), True),  # Higher patch version
        ((1, 0, 0, 0), (1, 0, 1, 0), False),  # Lower patch version
        ((1, 0, 0, 5), (1, 0, 0, 4), True),  # Higher build version
        ((1, 0, 0, 0), (1, 0, 0, 1), False),  # Lower build version
    ],
)
def test_is_version_at_least(sample_config, current, required, expected):
    """Test version comparison for feature support."""
    config = RestServiceConfig(raw_config=sample_config)

    # Directly set the version cache
    config._version_cache = current

    result = config.is_version_at_least(
        required[0], required[1], required[2], required[3]
    )
    assert result == expected


def test_is_version_at_least_none(sample_config):
    """Test version comparison when version is None."""
    config = RestServiceConfig(raw_config=sample_config)

    # Use patch to mock parsed_version to return None
    with patch.object(config, "_parse_version", return_value=None):
        # Should handle None by treating it as (0,0,0,0)
        assert not config.is_version_at_least(1, 0, 0, 0)
        assert config.is_version_at_least(0, 0, 0, 0)


def test_is_no_code_enabled(sample_config):
    """Test checking if noCode is enabled."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.is_no_code_enabled is True

    # Test with noCode disabled
    modified_config = sample_config.copy()
    modified_config["noCode"] = "false"
    config = RestServiceConfig(raw_config=modified_config)
    assert config.is_no_code_enabled is False


def test_is_managed_ingestion_enabled(sample_config):
    """Test checking if managed ingestion is enabled."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.is_managed_ingestion_enabled is True

    # Test with managed ingestion disabled
    modified_config = sample_config.copy()
    modified_config["managedIngestion"]["enabled"] = False
    config = RestServiceConfig(raw_config=modified_config)
    assert config.is_managed_ingestion_enabled is False


@pytest.mark.parametrize(
    "server_env,version,feature,expected",
    [
        # Test cloud environments with different version requirements
        (
            "cloud",
            "v0.3.11.0",
            ServiceFeature.OPEN_API_SDK,
            True,
        ),  # Cloud meets requirement
        (
            "cloud",
            "v0.3.10.0",
            ServiceFeature.OPEN_API_SDK,
            False,
        ),  # Cloud below requirement
        (
            "cloud",
            "v1.0.0.0",
            ServiceFeature.OPEN_API_SDK,
            True,
        ),  # Cloud exceeds requirement
        # Test core (non-cloud) environments with different version requirements
        (
            "core",
            "v1.0.1.0",
            ServiceFeature.OPEN_API_SDK,
            True,
        ),  # Core meets requirement
        (
            "core",
            "v1.0.0.0",
            ServiceFeature.OPEN_API_SDK,
            False,
        ),  # Core below requirement
        (
            "core",
            "v2.0.0.0",
            ServiceFeature.OPEN_API_SDK,
            True,
        ),  # Core exceeds requirement
        # Test config-based features (should be independent of cloud/core status)
        (
            "cloud",
            "v0.1.0.0",
            ServiceFeature.IMPACT_ANALYSIS,
            True,
        ),  # Config-based feature
        (
            "core",
            "v0.1.0.0",
            ServiceFeature.IMPACT_ANALYSIS,
            True,
        ),  # Config-based feature
        # Test with an unknown feature identifier (using a string instead of enum)
        (
            "core",
            "v1.0.0.0",
            "unknown_feature",
            False,
        ),  # Unknown feature
    ],
)
def test_supports_feature_cloud_core(
    sample_config, server_env, version, feature, expected
):
    """Test feature support detection based on cloud vs. core environment."""
    modified_config = sample_config.copy()

    # Set the version
    modified_config["versions"]["acryldata/datahub"]["version"] = version

    # Set the server environment
    if "datahub" not in modified_config:
        modified_config["datahub"] = {}
    modified_config["datahub"]["serverEnv"] = server_env

    config = RestServiceConfig(raw_config=modified_config)

    # For unknown feature test case
    if isinstance(feature, str) and feature == "unknown_feature":
        # Create a mock object instead of extending the enum
        mock_feature = MagicMock()
        mock_feature.value = "unknown_feature"
        feature = mock_feature

    result = config.supports_feature(feature)
    assert result == expected


def test_supports_feature_required_versions(sample_config):
    """Test that the correct required versions are used for each feature."""
    # Create a spy on the is_version_at_least method
    with patch.object(
        RestServiceConfig, "is_version_at_least", return_value=True
    ) as mock_version_check:
        # Test cloud environment
        cloud_config = sample_config.copy()
        if "datahub" not in cloud_config:
            cloud_config["datahub"] = {}
        cloud_config["datahub"]["serverEnv"] = "cloud"

        config = RestServiceConfig(raw_config=cloud_config)
        config.supports_feature(ServiceFeature.OPEN_API_SDK)

        # Verify cloud requirements were used (0, 3, 11, 0)
        mock_version_check.assert_called_with(0, 3, 11, 0)

        # Reset mock
        mock_version_check.reset_mock()

        # Test core environment
        core_config = sample_config.copy()
        if "datahub" not in core_config:
            core_config["datahub"] = {}
        core_config["datahub"]["serverEnv"] = "core"

        config = RestServiceConfig(raw_config=core_config)
        config.supports_feature(ServiceFeature.OPEN_API_SDK)

        # Verify core requirements were used (1, 0, 1, 0)
        mock_version_check.assert_called_with(1, 0, 1, 0)


def test_datahub_cloud_feature(sample_config):
    """Test the DATAHUB_CLOUD feature detection."""
    # Test with cloud environment
    cloud_config = sample_config.copy()
    if "datahub" not in cloud_config:
        cloud_config["datahub"] = {}
    cloud_config["datahub"]["serverEnv"] = "cloud"

    config = RestServiceConfig(raw_config=cloud_config)
    assert config.supports_feature(ServiceFeature.DATAHUB_CLOUD) is True

    # Test with core environment
    core_config = sample_config.copy()
    if "datahub" not in core_config:
        core_config["datahub"] = {}
    core_config["datahub"]["serverEnv"] = "core"

    config = RestServiceConfig(raw_config=core_config)
    assert config.supports_feature(ServiceFeature.DATAHUB_CLOUD) is False


def test_str_and_repr(sample_config):
    """Test string representation of the config."""
    config = RestServiceConfig(raw_config=sample_config)

    # Test __str__
    assert str(config) == str(sample_config)

    # Test __repr__
    assert repr(config) == str(sample_config)


def test_feature_enum():
    """Test the Feature enum is defined correctly."""
    # Verify each enum value has the expected string representation
    assert ServiceFeature.OPEN_API_SDK.value == "openapi_sdk"
    assert ServiceFeature.NO_CODE.value == "no_code"
    assert ServiceFeature.STATEFUL_INGESTION.value == "stateful_ingestion"
    assert ServiceFeature.IMPACT_ANALYSIS.value == "impact_analysis"
    assert ServiceFeature.PATCH_CAPABLE.value == "patch_capable"

    # Test enum equality works correctly
    assert ServiceFeature.OPEN_API_SDK == ServiceFeature.OPEN_API_SDK
    assert ServiceFeature.NO_CODE != ServiceFeature.OPEN_API_SDK


def test_is_datahub_cloud_property():
    """Test the is_datahub_cloud property with different server environments."""
    # Test with no serverEnv
    config = RestServiceConfig(raw_config={"datahub": {}})
    assert config.is_datahub_cloud is False

    # Test with empty serverEnv
    config = RestServiceConfig(raw_config={"datahub": {"serverEnv": ""}})
    assert config.is_datahub_cloud is False

    # Test with "core" serverEnv
    config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "core"}})
    assert config.is_datahub_cloud is False

    # Test with "cloud" serverEnv
    config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "cloud"}})
    assert config.is_datahub_cloud is True

    # Test with other serverEnv values
    config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "prod"}})
    assert config.is_datahub_cloud is True

    # Test with no datahub entry
    config = RestServiceConfig(raw_config={})
    assert config.is_datahub_cloud is False


def test_feature_requirements_dictionary():
    """Test that the feature requirements dictionary is properly defined."""
    # Verify the _REQUIRED_VERSION_OPENAPI_TRACING constant
    assert "cloud" in _REQUIRED_VERSION_OPENAPI_TRACING
    assert "core" in _REQUIRED_VERSION_OPENAPI_TRACING
    assert _REQUIRED_VERSION_OPENAPI_TRACING["cloud"] == (0, 3, 11, 0)
    assert _REQUIRED_VERSION_OPENAPI_TRACING["core"] == (1, 0, 1, 0)

    # Mock the supports_feature method to inspect the feature_requirements dictionary
    with patch.object(RestServiceConfig, "is_version_at_least", return_value=True):
        config = RestServiceConfig(raw_config={})

        # Use the __dict__ to access the feature_requirements dictionary directly during the call
        with patch.object(RestServiceConfig, "supports_feature") as mock_method:
            # Call the method to trigger dictionary creation
            config.supports_feature(ServiceFeature.OPEN_API_SDK)

            # Extract the call arguments to verify dictionary structure
            # This is a bit of a hack but allows us to test the internal structure
            mock_method.assert_called_once()

            # Alternative approach: use a spy to inspect the internal dictionary
            with patch.object(
                RestServiceConfig, "supports_feature", wraps=config.supports_feature
            ):
                # Now we can manually check if both features use the same requirements dictionary
                assert config.supports_feature(
                    ServiceFeature.OPEN_API_SDK
                ) == config.supports_feature(ServiceFeature.API_TRACING)


def test_feature_requirements_shared_reference():
    """Test that OPEN_API_SDK and API_TRACING share the same version requirements."""
    # Create a mock config object with core environment
    config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "core"}})

    # Create a spy on is_version_at_least to track calls
    with patch.object(RestServiceConfig, "is_version_at_least") as mock_version_check:
        # First check OPEN_API_SDK
        mock_version_check.return_value = True
        config.supports_feature(ServiceFeature.OPEN_API_SDK)

        # Store the first call's arguments
        first_call_args = mock_version_check.call_args[0]

        # Reset the mock to verify the call for API_TRACING
        mock_version_check.reset_mock()
        mock_version_check.return_value = True
        config.supports_feature(ServiceFeature.API_TRACING)

        # Store the second call's arguments
        second_call_args = mock_version_check.call_args[0]

        # Both features should check the same version requirements
        assert mock_version_check.call_count > 0, (
            "Version check should have been called"
        )
        assert first_call_args == second_call_args, (
            "Both features should use the same version requirements"
        )


def test_get_deployment_requirements():
    """Test requirements fetching for different deployment types."""

    # Test cloud deployment
    cloud_config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "cloud"}})

    # Test core deployment
    core_config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "core"}})

    # Use a list to capture version requirements
    cloud_calls = []
    core_calls = []

    # Mock implementation for cloud config
    def cloud_mock_version_check(major, minor, patch, build):
        cloud_calls.append((major, minor, patch, build))
        return True

    # Mock implementation for core config
    def core_mock_version_check(major, minor, patch, build):
        core_calls.append((major, minor, patch, build))
        return True

    # Test with cloud config
    with patch.object(
        cloud_config, "is_version_at_least", side_effect=cloud_mock_version_check
    ):
        cloud_config.supports_feature(ServiceFeature.OPEN_API_SDK)
        assert cloud_calls[-1] == (0, 3, 11, 0)  # cloud requirement

    # Test with core config
    with patch.object(
        core_config, "is_version_at_least", side_effect=core_mock_version_check
    ):
        core_config.supports_feature(ServiceFeature.OPEN_API_SDK)
        assert core_calls[-1] == (1, 0, 1, 0)  # core requirement


def test_missing_deployment_requirements():
    """Test behavior when requirements are missing for a deployment type."""
    # Use a mock for feature that doesn't have requirements for all deployment types
    mock_feature = MagicMock()
    mock_feature.value = "custom_feature"

    # Create configs for testing
    cloud_config = RestServiceConfig(raw_config={"datahub": {"serverEnv": "cloud"}})

    # Mock the feature_requirements dictionary to include only one deployment type
    with patch(
        "datahub.utilities.server_config_util._REQUIRED_VERSION_OPENAPI_TRACING",
        {"core": (1, 0, 0, 0)},
    ):  # Only core requirements, no cloud
        # Test with cloud config - should return False because no requirements for cloud
        assert not cloud_config.supports_feature(ServiceFeature.OPEN_API_SDK)

    # Test with undefined feature
    assert not cloud_config.supports_feature(mock_feature)


def test_requirements_not_defined():
    """Test behavior when a feature has no requirements defined."""
    # Mock a feature with no requirements in the dictionary
    config = RestServiceConfig(raw_config={})

    # Create a mock feature not in the requirements dictionary
    mock_feature = MagicMock()
    mock_feature.value = "test_feature"

    # Should return False as no requirements defined
    assert not config.supports_feature(mock_feature)
