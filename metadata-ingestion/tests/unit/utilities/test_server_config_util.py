from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.configuration.common import ConfigurationError
from datahub.utilities.server_config_util import RestServiceConfig, ServiceFeature


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


@pytest.fixture
def mock_session():
    session = MagicMock(spec=requests.Session)
    return session


@pytest.fixture
def mock_response(sample_config):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = sample_config
    response.text = str(sample_config)
    return response


def test_init_with_config(sample_config):
    """Test initialization with a config dictionary."""
    config = RestServiceConfig(raw_config=sample_config)
    assert config.config == sample_config
    assert config.session is None
    assert config.url is None
    assert config._version_cache is None


def test_init_with_session_and_url(mock_session):
    """Test initialization with session and URL."""
    url = "http://localhost:8080/config"
    config = RestServiceConfig(session=mock_session, url=url)
    assert not config.raw_config  # Should be empty
    assert config.session == mock_session
    assert config.url == url
    assert config._version_cache is None


def test_load_config_success(mock_session, mock_response, sample_config):
    """Test successful config loading from server."""
    url = "http://localhost:8080/config"
    mock_session.get.return_value = mock_response

    config = RestServiceConfig(session=mock_session, url=url)
    loaded_config = config.fetch_config()

    mock_session.get.assert_called_once_with(url)
    assert loaded_config == sample_config


def test_load_config_failure_no_session_url():
    """Test failure when trying to load config without session and URL."""
    config = RestServiceConfig()
    with pytest.raises(ConfigurationError) as exc_info:
        config.fetch_config()
    assert "Session and URL are required" in str(exc_info.value)


def test_load_config_failure_wrong_service(mock_session, mock_response):
    """Test failure when connecting to wrong service."""
    url = "http://localhost:8080/config"

    # Modify the response to indicate wrong service
    mock_response.json.return_value = {"noCode": "false"}
    mock_session.get.return_value = mock_response

    config = RestServiceConfig(session=mock_session, url=url)
    with pytest.raises(ConfigurationError) as exc_info:
        config.fetch_config()
    assert "frontend service" in str(exc_info.value)


def test_load_config_failure_http_error(mock_session):
    """Test failure due to HTTP error."""
    url = "http://localhost:8080/config"

    # Create error response
    error_response = MagicMock()
    error_response.status_code = 404
    error_response.text = "Not Found"
    mock_session.get.return_value = error_response

    config = RestServiceConfig(session=mock_session, url=url)
    with pytest.raises(ConfigurationError) as exc_info:
        config.fetch_config()
    assert "status_code: 404" in str(exc_info.value)


def test_load_config_failure_auth_error(mock_session):
    """Test failure due to authentication error."""
    url = "http://localhost:8080/config"

    # Create auth error response
    error_response = MagicMock()
    error_response.status_code = 401
    error_response.text = "Unauthorized"
    mock_session.get.return_value = error_response

    config = RestServiceConfig(session=mock_session, url=url)
    with pytest.raises(ConfigurationError) as exc_info:
        config.fetch_config()
    assert "authentication error" in str(exc_info.value)


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


@pytest.mark.parametrize(
    "server_env,expected",
    [
        ("oss", False),  # Open source environment
        ("cloud", True),  # Cloud environment
        ("managed", True),  # Managed environment
        ("enterprise", True),  # Enterprise environment
        (None, False),  # No environment specified
        ("", False),  # Empty string is not "oss"
    ],
)
def test_is_datahub_cloud(sample_config, server_env, expected):
    """Test checking if a configuration represents DataHub Cloud based on serverEnv."""
    modified_config = sample_config.copy()

    # Ensure datahub config exists
    if "datahub" not in modified_config:
        modified_config["datahub"] = {}

    # Set the serverEnv value
    modified_config["datahub"]["serverEnv"] = server_env

    config = RestServiceConfig(raw_config=modified_config)
    assert config.is_datahub_cloud is expected


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
    config.config["versions"]["acryldata/datahub"]["version"] = "v2.0.0"

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
    "version,feature,expected",
    [
        ("v1.0.0", ServiceFeature.IMPACT_ANALYSIS, True),  # Config-based feature
        (
            "v1.0.0-acryl",
            ServiceFeature.IMPACT_ANALYSIS,
            True,
        ),  # Config-based with acryl suffix
        (
            "v0.3.11",
            ServiceFeature.OPEN_API_SDK,
            False,
        ),  # Version-based (no suffix - not enough)
        (
            "v1.0.1",
            ServiceFeature.OPEN_API_SDK,
            True,
        ),  # Version-based (no suffix - enough)
        (
            "v0.3.11-acryl",
            ServiceFeature.OPEN_API_SDK,
            True,
        ),  # Version-based (acryl suffix - enough)
        (
            "v0.3.10-acryl",
            ServiceFeature.OPEN_API_SDK,
            False,
        ),  # Version-based (acryl suffix - not enough)
        ("v0.3.11-cloud", ServiceFeature.OPEN_API_SDK, True),  # Specific suffix
        ("v0.3.11-custom", ServiceFeature.OPEN_API_SDK, True),  # Any other suffix
    ],
)
def test_supports_feature(sample_config, version, feature, expected):
    """Test feature support detection based on version and config."""
    modified_config = sample_config.copy()
    modified_config["versions"]["acryldata/datahub"]["version"] = version

    config = RestServiceConfig(raw_config=modified_config)
    result = config.supports_feature(feature)
    assert result == expected


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
