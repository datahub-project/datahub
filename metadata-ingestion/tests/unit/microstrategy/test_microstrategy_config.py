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
