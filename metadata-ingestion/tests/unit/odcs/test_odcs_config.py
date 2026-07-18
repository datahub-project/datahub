import pytest
from pydantic import ValidationError

from datahub.ingestion.source.odcs.odcs_config import (
    ODCSSourceConfig,
    SchemaAssertionCompatibility,
)


def test_schema_assertion_compatibility_defaults_to_superset() -> None:
    config = ODCSSourceConfig.model_validate({"path": "contracts/"})
    assert (
        config.schema_assertion_compatibility is SchemaAssertionCompatibility.SUPERSET
    )


def test_schema_assertion_compatibility_is_case_insensitive() -> None:
    config = ODCSSourceConfig.model_validate(
        {"path": "contracts/", "schema_assertion_compatibility": "subset"}
    )
    assert config.schema_assertion_compatibility is SchemaAssertionCompatibility.SUBSET


def test_schema_assertion_compatibility_rejects_unknown_mode() -> None:
    with pytest.raises(ValidationError):
        ODCSSourceConfig.model_validate(
            {"path": "contracts/", "schema_assertion_compatibility": "loose"}
        )
