import pytest
from pydantic import ValidationError

from datahub.ingestion.source.quicksight.quicksight_config import (
    ExternalDataSourceConfig,
    QuickSightSourceConfig,
)


def test_region_required_by_model_validator():
    # `validate_region_present` rejects configs without a region...
    with pytest.raises(ValidationError):
        QuickSightSourceConfig.model_validate({"aws_profile": "my-profile"})

    # ...and accepts them once a region is supplied.
    config = QuickSightSourceConfig.model_validate(
        {"aws_profile": "my-profile", "aws_region": "us-east-1"}
    )
    assert config.aws_region == "us-east-1"


def test_invalid_account_id_rejected():
    with pytest.raises(ValidationError):
        QuickSightSourceConfig.model_validate(
            {"aws_region": "us-east-1", "aws_account_id": "not-an-account"}
        )


def test_valid_account_id_accepted():
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", "aws_account_id": "064369473231"}
    )
    assert config.aws_account_id == "064369473231"


def test_external_data_sources_parsing():
    config = QuickSightSourceConfig.model_validate(
        {
            "aws_region": "us-east-1",
            "external_data_sources": {
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890": {
                    "platform_instance": "prod-snowflake",
                    "env": "PROD",
                    "convert_urns_to_lowercase": True,
                    "default_database": "prod_db",
                    "default_schema": "public",
                }
            },
        }
    )
    ds = config.external_data_sources["a1b2c3d4-e5f6-7890-abcd-ef1234567890"]
    assert isinstance(ds, ExternalDataSourceConfig)
    assert ds.platform_instance == "prod-snowflake"
    assert ds.default_database == "prod_db"
