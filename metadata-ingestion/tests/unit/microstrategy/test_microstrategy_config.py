import pytest
from pydantic import ValidationError

from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig


def test_base_url_normalization() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary/api/",
        }
    )

    assert config.base_url == "https://mstr.example.com/MicroStrategyLibrary"


@pytest.mark.parametrize("username", ["", "   "])
def test_password_auth_rejects_blank_username(username: str) -> None:
    with pytest.raises(ValidationError):
        MicroStrategyConfig.model_validate(
            {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "auth": {
                    "type": "password",
                    "username": username,
                    "password": "secret",
                },
            }
        )


def test_emit_semantic_model_entities_defaults_to_false() -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )

    assert config.emit_semantic_model_entities is False


def test_emit_semantic_model_entities_can_be_enabled() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "emit_semantic_model_entities": True,
        }
    )

    assert config.emit_semantic_model_entities is True


def test_personal_folders_excluded_by_default() -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )

    assert config.include_personal_folders is False
    assert MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "include_personal_folders": True,
        }
    ).include_personal_folders


def test_dataset_field_order_defaults_to_report() -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )

    assert config.dataset_field_order == "report"


def test_dataset_field_order_accepts_alphabetical() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "dataset_field_order": "alphabetical",
        }
    )

    assert config.dataset_field_order == "alphabetical"


def test_document_dataset_sql_lineage_defaults_to_false() -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )

    assert config.extract_document_dataset_sql_lineage is False


def test_document_dataset_sql_lineage_requires_warehouse_lineage() -> None:
    with pytest.raises(ValidationError, match="extract_warehouse_lineage"):
        MicroStrategyConfig.model_validate(
            {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "extract_document_dataset_sql_lineage": True,
            }
        )

    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "extract_warehouse_lineage": True,
            "extract_document_dataset_sql_lineage": True,
        }
    )

    assert config.extract_document_dataset_sql_lineage is True


def test_dataset_field_order_rejects_unknown_values() -> None:
    with pytest.raises(ValidationError):
        MicroStrategyConfig.model_validate(
            {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "dataset_field_order": "grid",
            }
        )


def test_folder_path_pattern_defaults_to_allow_all() -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )

    assert config.folder_path_pattern.allowed("Adhoc_RMA/Shared Reports/Anything")
    assert not MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "folder_path_pattern": {"deny": ["^Adhoc_RMA/Shared Reports/Archive"]},
        }
    ).folder_path_pattern.allowed("Adhoc_RMA/Shared Reports/Archive/Old")
