"""Tests for NotionSourceConfig."""

import pytest
from pydantic import SecretStr, ValidationError

from datahub.ingestion.source.notion.notion_config import NotionSourceConfig


def test_valid_config_with_page_ids():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
    )
    assert config.api_key.get_secret_value() == "secret_test_key"
    assert config.page_ids == ["2bffc6a6-4277-8024-97c9-d0f26faa4480"]
    assert config.recursive is True


def test_valid_config_with_database_ids():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        database_ids=["abcdef01234567890123456789012345"],
    )
    # IDs are normalized to include hyphens (8-4-4-4-12 format)
    assert config.database_ids == ["abcdef01-2345-6789-0123-456789012345"]
    assert config.recursive is True


def test_valid_config_with_both_ids():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        database_ids=["abcdef01234567890123456789012345"],
    )
    assert len(config.page_ids) == 1
    assert len(config.database_ids) == 1


def test_valid_config_with_hyphens():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
    )
    assert config.page_ids == ["2bffc6a6-4277-8024-97c9-d0f26faa4480"]


def test_valid_config_without_hyphens():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a642778024971c9d0f26faa480"],
    )
    # IDs without hyphens are normalized to include hyphens (8-4-4-4-12 format)
    assert config.page_ids == ["2bffc6a6-4277-8024-971c-9d0f26faa480"]


def test_missing_api_key():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"])  # type: ignore[call-arg]

    assert "api_key" in str(exc_info.value)


def test_empty_api_key():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(
            api_key=SecretStr(""),
            page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        )

    assert "cannot be empty" in str(exc_info.value)


def test_whitespace_api_key():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(
            api_key=SecretStr("   "),
            page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        )

    assert "cannot be empty" in str(exc_info.value)


def test_missing_both_page_and_database_ids():
    # Auto-discovery is now supported - no validation error when both are empty
    config = NotionSourceConfig(api_key=SecretStr("secret_test_key"))
    assert config.page_ids == []
    assert config.database_ids == []


def test_invalid_page_id_too_short():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(
            api_key=SecretStr("secret_test_key"),
            page_ids=["abc123"],
        )

    assert "Invalid Notion page ID format" in str(exc_info.value)
    assert "32 hex characters" in str(exc_info.value)


def test_invalid_page_id_too_long():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(
            api_key=SecretStr("secret_test_key"),
            page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480-extra"],
        )

    assert "Invalid Notion page ID format" in str(exc_info.value)


def test_invalid_page_id_non_hex():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(
            api_key=SecretStr("secret_test_key"),
            page_ids=["zzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"],
        )

    assert "32 hex characters" in str(exc_info.value)


def test_invalid_database_id_format():
    with pytest.raises(ValidationError) as exc_info:
        NotionSourceConfig(
            api_key=SecretStr("secret_test_key"),
            database_ids=["invalid"],
        )

    assert "Invalid Notion database ID format" in str(exc_info.value)


def test_hierarchy_defaults_to_notion_strategy():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
    )

    assert config.hierarchy.enabled is True
    assert config.hierarchy.parent_strategy == "notion"


def test_recursive_default():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
    )

    assert config.recursive is True


def test_recursive_false():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        recursive=False,
    )

    assert config.recursive is False


def test_processing_config_defaults():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
    )

    assert config.processing.partition.strategy == "auto"
    assert config.processing.partition.partition_by_api is False
    assert config.processing.parallelism.num_processes == 2


def test_filtering_config_defaults():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
    )

    assert config.filtering.skip_empty_documents is True
    assert config.filtering.min_text_length == 50


def test_multiple_page_ids():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=[
            "2bffc6a6-4277-8024-97c9-d0f26faa4480",
            "abcdef01234567890123456789012345",
        ],
    )

    assert len(config.page_ids) == 2
