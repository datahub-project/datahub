import pytest
from pydantic import ValidationError

from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.quip.quip_config import QuipSourceConfig


def test_minimal_config_valid() -> None:
    config = QuipSourceConfig.model_validate({"access_token": "secret-token"})
    assert config.access_token.get_secret_value() == "secret-token"
    assert config.base_url == "https://platform.quip.com"
    assert config.recursive is True
    # EXTERNAL is the default import mode and must flow into document_mapping.
    assert config.document_import_mode == DocumentImportMode.EXTERNAL
    assert config.document_mapping.source.type == "EXTERNAL"


def test_empty_access_token_rejected() -> None:
    with pytest.raises(ValidationError):
        QuipSourceConfig.model_validate({"access_token": "   "})


def test_base_url_normalized_and_validated() -> None:
    config = QuipSourceConfig.model_validate(
        {"access_token": "t", "base_url": "https://platform.quip-amazon.com/"}
    )
    assert config.base_url == "https://platform.quip-amazon.com"

    with pytest.raises(ValidationError):
        QuipSourceConfig.model_validate(
            {"access_token": "t", "base_url": "platform.quip.com"}
        )


def test_platform_instance_must_be_safe() -> None:
    config = QuipSourceConfig.model_validate(
        {"access_token": "t", "platform_instance": "my-company_1"}
    )
    assert config.platform_instance == "my-company_1"

    with pytest.raises(ValidationError):
        QuipSourceConfig.model_validate(
            {"access_token": "t", "platform_instance": "bad instance!"}
        )


def test_native_import_mode_propagates() -> None:
    config = QuipSourceConfig.model_validate(
        {"access_token": "t", "document_import_mode": "NATIVE"}
    )
    assert config.document_mapping.source.type == "NATIVE"


def test_parent_document_urn_sets_root_parent() -> None:
    config = QuipSourceConfig.model_validate(
        {"access_token": "t", "parent_document_urn": "urn:li:document:root"}
    )
    assert config.hierarchy.folder_mapping.root_parent == "urn:li:document:root"


def test_id_pattern_drops_directory_component() -> None:
    config = QuipSourceConfig.model_validate({"access_token": "t"})
    assert config.document_mapping.id_pattern == "{source_type}-{basename}"
