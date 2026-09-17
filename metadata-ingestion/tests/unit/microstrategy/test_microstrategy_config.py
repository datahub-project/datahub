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


def test_dataset_field_order_rejects_unknown_values() -> None:
    with pytest.raises(ValidationError):
        MicroStrategyConfig.model_validate(
            {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "dataset_field_order": "grid",
            }
        )
