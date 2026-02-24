"""Unit tests for Confluence configuration."""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.confluence.confluence_config import (
    ConfluenceSourceConfig,
)


def test_minimal_cloud_config_valid() -> None:
    """Test that minimal Cloud configuration is valid."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "cloud": True,
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://test.atlassian.net/wiki"
    assert config.username == "test@example.com"
    assert config.api_token is not None
    assert config.api_token.get_secret_value() == "test-token-123"
    assert config.cloud is True


def test_minimal_datacenter_config_valid() -> None:
    """Test that minimal Data Center configuration is valid."""
    config_dict = {
        "url": "https://confluence.company.com",
        "personal_access_token": "test-pat-123",
        "cloud": False,
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://confluence.company.com"
    assert config.personal_access_token is not None
    assert config.personal_access_token.get_secret_value() == "test-pat-123"
    assert config.cloud is False


def test_missing_auth_raises_error() -> None:
    """Test that missing authentication raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)

    assert "Either provide username + api_token" in str(exc_info.value)


def test_both_auth_methods_raises_error() -> None:
    """Test that providing both auth methods raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "personal_access_token": "test-pat-123",
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)

    assert "Cannot provide both Cloud authentication" in str(exc_info.value)


def test_cloud_without_username_raises_error() -> None:
    """Test that Cloud auth without username raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "api_token": "test-token-123",
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)

    assert "Either provide username + api_token" in str(exc_info.value)


def test_cloud_without_api_token_raises_error() -> None:
    """Test that Cloud auth without api_token raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)

    assert "Either provide username + api_token" in str(exc_info.value)


def test_hierarchy_strategy_defaults_to_confluence() -> None:
    """Test that hierarchy strategy defaults to 'confluence'."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    # Default is "folder", but validator should change it to "confluence"
    assert config.hierarchy.parent_strategy == "confluence"


def test_document_id_pattern_excludes_directory() -> None:
    """Test that document ID pattern excludes directory component."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    # Should use pattern without {directory}
    assert config.document_mapping.id_pattern == "{source_type}-{basename}"
    assert "{directory}" not in config.document_mapping.id_pattern


def test_max_spaces_default() -> None:
    """Test that max_spaces has sensible default."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.max_spaces == 100


def test_max_pages_per_space_default() -> None:
    """Test that max_pages_per_space has sensible default."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.max_pages_per_space == 1000


def test_recursive_default() -> None:
    """Test that recursive defaults to True."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.recursive is True


def test_advanced_configuration() -> None:
    """Test advanced configuration with all options."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "max_spaces": 50,
        "max_pages_per_space": 500,
        "recursive": False,
        "filtering": {
            "min_text_length": 200,
        },
        "chunking": {
            "strategy": "by_title",
            "max_characters": 1000,
        },
        "embedding": {
            "provider": "cohere",
            "model": "embed-english-v3.0",
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.max_spaces == 50
    assert config.max_pages_per_space == 500
    assert config.recursive is False
    assert config.filtering.min_text_length == 200
    assert config.chunking.strategy == "by_title"
    assert config.chunking.max_characters == 1000
    assert config.embedding.provider == "cohere"
    assert config.embedding.model == "embed-english-v3.0"


# ============================================================================
# URL Normalization Tests
# ============================================================================


def test_url_normalization_strips_home() -> None:
    """Test that /home suffix is automatically stripped from URL."""
    config_dict = {
        "url": "https://datahub-integration-testing.atlassian.net/wiki/home",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://datahub-integration-testing.atlassian.net/wiki"


def test_url_normalization_strips_spaces() -> None:
    """Test that /spaces suffix is automatically stripped from URL."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki/spaces",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://test.atlassian.net/wiki"


def test_url_normalization_strips_pages() -> None:
    """Test that /pages suffix is automatically stripped from URL."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki/pages",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://test.atlassian.net/wiki"


def test_url_normalization_strips_trailing_slash() -> None:
    """Test that trailing slashes are stripped."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki/",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://test.atlassian.net/wiki"


def test_url_normalization_adds_wiki_for_cloud() -> None:
    """Test that /wiki is added for Cloud URLs if missing."""
    config_dict = {
        "url": "https://test.atlassian.net",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://test.atlassian.net/wiki"


def test_url_normalization_datacenter_unchanged() -> None:
    """Test that Data Center URLs are not modified."""
    config_dict = {
        "url": "https://confluence.company.com",
        "personal_access_token": "test-pat-123",
        "cloud": False,
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://confluence.company.com"


def test_url_normalization_datacenter_with_context_path() -> None:
    """Test that Data Center URLs with context paths are preserved."""
    config_dict = {
        "url": "https://company.com/confluence",
        "personal_access_token": "test-pat-123",
        "cloud": False,
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.url == "https://company.com/confluence"


# ============================================================================
# Space Allow/Deny List Tests
# ============================================================================


def test_space_allow_with_space_keys() -> None:
    """Test spaces.allow with space keys."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": ["ENGINEERING", "PRODUCT", "DESIGN"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.spaces.allow == ["ENGINEERING", "PRODUCT", "DESIGN"]
    assert config._parsed_space_allow == ["ENGINEERING", "PRODUCT", "DESIGN"]


def test_space_allow_with_urls() -> None:
    """Test spaces.allow with space URLs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": [
                "https://test.atlassian.net/wiki/spaces/TEAM",
                "https://test.atlassian.net/wiki/spaces/DOCS",
            ],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_space_allow == ["TEAM", "DOCS"]


def test_space_allow_mixed_keys_and_urls() -> None:
    """Test spaces.allow with mix of space keys and URLs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": [
                "ENGINEERING",
                "https://test.atlassian.net/wiki/spaces/PRODUCT",
                "DESIGN",
            ],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_space_allow == ["ENGINEERING", "PRODUCT", "DESIGN"]


def test_space_deny_with_space_keys() -> None:
    """Test spaces.deny with space keys."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "deny": ["~john.doe", "~jane.smith", "ARCHIVE"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.spaces.deny == ["~john.doe", "~jane.smith", "ARCHIVE"]
    assert config._parsed_space_deny == ["~john.doe", "~jane.smith", "ARCHIVE"]


def test_space_allow_and_deny_together() -> None:
    """Test using both spaces.allow and spaces.deny."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": ["ENGINEERING", "PRODUCT"],
            "deny": ["~admin"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_space_allow == ["ENGINEERING", "PRODUCT"]
    assert config._parsed_space_deny == ["~admin"]


def test_space_allow_empty_list_raises_error() -> None:
    """Test that empty spaces.allow list raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": [],
        },
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)
    assert "spaces.allow must be a non-empty list" in str(exc_info.value)


def test_space_allow_invalid_url_raises_error() -> None:
    """Test that invalid space URL in spaces.allow raises error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": ["https://test.atlassian.net/wiki/invalid/path"],
        },
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)
    assert "Could not extract space key from URL" in str(exc_info.value)


# ============================================================================
# Page Allow/Deny List Tests
# ============================================================================


def test_page_allow_with_page_ids() -> None:
    """Test pages.allow with page IDs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": ["123456", "789012", "345678"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.pages.allow == ["123456", "789012", "345678"]
    assert config._parsed_page_allow == ["123456", "789012", "345678"]


def test_page_allow_with_cloud_urls() -> None:
    """Test pages.allow with Cloud page URLs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": [
                "https://test.atlassian.net/wiki/spaces/ENG/pages/123456/API-Docs",
                "https://test.atlassian.net/wiki/spaces/TEAM/pages/789012/Guide",
            ],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_page_allow == ["123456", "789012"]


def test_page_allow_with_datacenter_urls() -> None:
    """Test pages.allow with Data Center page URLs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": [
                "https://confluence.company.com/pages/viewpage.action?pageId=123456",
                "https://confluence.company.com/pages/viewpage.action?pageId=789012",
            ],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_page_allow == ["123456", "789012"]


def test_page_allow_mixed_ids_and_urls() -> None:
    """Test pages.allow with mix of page IDs and URLs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": [
                "123456",
                "https://test.atlassian.net/wiki/spaces/TEAM/pages/789012/Guide",
                "345678",
            ],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_page_allow == ["123456", "789012", "345678"]


def test_page_deny_with_page_ids() -> None:
    """Test pages.deny with page IDs."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "deny": ["999999", "888888"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config.pages.deny == ["999999", "888888"]
    assert config._parsed_page_deny == ["999999", "888888"]


def test_page_allow_and_deny_together() -> None:
    """Test using both pages.allow and pages.deny."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": ["123456", "789012"],
            "deny": ["888888"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_page_allow == ["123456", "789012"]
    assert config._parsed_page_deny == ["888888"]


def test_page_allow_empty_list_raises_error() -> None:
    """Test that empty pages.allow list raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": [],
        },
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)
    assert "pages.allow must be a non-empty list" in str(exc_info.value)


def test_page_allow_non_numeric_id_raises_error() -> None:
    """Test that non-numeric page ID in pages.allow raises error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": ["NOTANID"],
        },
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)
    assert "Page ID must be numeric" in str(exc_info.value)


def test_page_allow_invalid_url_raises_error() -> None:
    """Test that invalid page URL in pages.allow raises error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "pages": {
            "allow": ["https://test.atlassian.net/wiki/spaces/TEAM"],
        },
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)
    assert "Could not extract page ID from URL" in str(exc_info.value)


# ============================================================================
# Combined Allow/Deny List Tests
# ============================================================================


def test_combined_space_and_page_filtering() -> None:
    """Test using space and page filtering together."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {
            "allow": ["ENGINEERING", "PRODUCT"],
            "deny": ["~admin"],
        },
        "pages": {
            "deny": ["999999", "888888"],
        },
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_space_allow == ["ENGINEERING", "PRODUCT"]
    assert config._parsed_space_deny == ["~admin"]
    assert config._parsed_page_deny == ["999999", "888888"]


def test_no_filtering_is_valid() -> None:
    """Test that configuration with no filtering is valid (auto-discover mode)."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)
    assert config._parsed_space_allow is None
    assert config._parsed_space_deny is None
    assert config._parsed_page_allow is None
    assert config._parsed_page_deny is None


# ============================================================================
# Platform Instance Validation Tests
# ============================================================================


def test_platform_instance_valid_values() -> None:
    """Test that valid platform_instance values are accepted."""
    base_config = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token",
    }

    # Alphanumeric
    config = ConfluenceSourceConfig.model_validate(
        {**base_config, "platform_instance": "myinstance"}
    )
    assert config.platform_instance == "myinstance"

    # With hyphens
    config = ConfluenceSourceConfig.model_validate(
        {**base_config, "platform_instance": "my-instance"}
    )
    assert config.platform_instance == "my-instance"

    # With underscores
    config = ConfluenceSourceConfig.model_validate(
        {**base_config, "platform_instance": "my_instance"}
    )
    assert config.platform_instance == "my_instance"

    # Mixed alphanumeric, hyphens, underscores
    config = ConfluenceSourceConfig.model_validate(
        {**base_config, "platform_instance": "mycompany-prod-2024_v1"}
    )
    assert config.platform_instance == "mycompany-prod-2024_v1"


def test_platform_instance_empty_string_raises_error() -> None:
    """Test that empty platform_instance raises validation error."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token",
        "platform_instance": "",
    }
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(config_dict)

    assert "platform_instance cannot be empty string" in str(exc_info.value)


def test_platform_instance_invalid_characters_raises_error() -> None:
    """Test that platform_instance with invalid characters raises validation error."""
    base_config = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token",
    }

    # Special characters
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(
            {**base_config, "platform_instance": "my@instance"}
        )
    assert "must contain only alphanumeric" in str(exc_info.value)

    # Spaces
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(
            {**base_config, "platform_instance": "my instance"}
        )
    assert "must contain only alphanumeric" in str(exc_info.value)

    # Dots
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(
            {**base_config, "platform_instance": "my.instance"}
        )
    assert "must contain only alphanumeric" in str(exc_info.value)

    # Forward slash
    with pytest.raises(ValidationError) as exc_info:
        ConfluenceSourceConfig.model_validate(
            {**base_config, "platform_instance": "my/instance"}
        )
    assert "must contain only alphanumeric" in str(exc_info.value)


def test_platform_instance_none_is_valid() -> None:
    """Test that platform_instance can be None (uses auto-generated hash)."""
    config = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://test.atlassian.net/wiki",
            "username": "test@example.com",
            "api_token": "test-token",
        }
    )
    assert config.platform_instance is None
