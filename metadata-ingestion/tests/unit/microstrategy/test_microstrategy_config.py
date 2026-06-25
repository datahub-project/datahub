from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConfig,
    MicroStrategyGuestAuth,
    MicroStrategyPasswordAuth,
)


def test_base_url_normalization() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary/api/",
        }
    )

    assert config.base_url == "https://mstr.example.com/MicroStrategyLibrary"
    assert isinstance(config.auth, MicroStrategyGuestAuth)
    assert config.extract_warehouse_lineage is False
    assert config.warehouse_lineage_sql_timeout_seconds == 180


def test_password_auth_config() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "auth": {
                "type": "password",
                "username": "metadata-reader",
                "password": "secret",
            },
        }
    )

    assert isinstance(config.auth, MicroStrategyPasswordAuth)
    assert config.auth.username == "metadata-reader"
    assert config.auth.password.get_secret_value() == "secret"
