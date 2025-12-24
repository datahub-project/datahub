"""Unit tests for stored procedure configuration mixin."""

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sql.stored_procedures.config import (
    StoredProcedureConfigMixin,
)


def test_config_defaults():
    """Test that StoredProcedureConfigMixin has correct defaults."""
    config = StoredProcedureConfigMixin()

    assert config.include_stored_procedures is True
    assert config.procedure_pattern == AllowDenyPattern.allow_all()
    assert config.include_stored_procedures_code is True


def test_config_override_include_stored_procedures():
    """Test overriding include_stored_procedures."""
    config = StoredProcedureConfigMixin(include_stored_procedures=False)

    assert config.include_stored_procedures is False
    assert config.include_stored_procedures_code is True  # Should remain default


def test_config_override_include_stored_procedures_code():
    """Test overriding include_stored_procedures_code."""
    config = StoredProcedureConfigMixin(include_stored_procedures_code=False)

    assert config.include_stored_procedures is True  # Should remain default
    assert config.include_stored_procedures_code is False


def test_config_override_procedure_pattern():
    """Test overriding procedure_pattern."""
    config = StoredProcedureConfigMixin(
        procedure_pattern={"allow": ["prod.*"], "deny": ["test.*"]}
    )

    assert config.procedure_pattern.allowed("prod.my_proc")
    assert not config.procedure_pattern.allowed("test.my_proc")


def test_config_all_overrides():
    """Test overriding all config fields."""
    config = StoredProcedureConfigMixin(
        include_stored_procedures=False,
        include_stored_procedures_code=False,
        procedure_pattern={"deny": ["internal.*"]},
    )

    assert config.include_stored_procedures is False
    assert config.include_stored_procedures_code is False
    assert not config.procedure_pattern.allowed("internal.secret_proc")
    assert config.procedure_pattern.allowed("public.my_proc")


def test_config_serialization():
    """Test that config can be serialized and deserialized."""
    original = StoredProcedureConfigMixin(
        include_stored_procedures=True,
        include_stored_procedures_code=False,
        procedure_pattern={"allow": ["finance.*"]},
    )

    # Convert to dict
    config_dict = original.dict()

    # Recreate from dict
    recreated = StoredProcedureConfigMixin(**config_dict)

    assert recreated.include_stored_procedures == original.include_stored_procedures
    assert (
        recreated.include_stored_procedures_code
        == original.include_stored_procedures_code
    )
    assert recreated.procedure_pattern.allowed("finance.revenue")
    assert not recreated.procedure_pattern.allowed("hr.employees")


def test_config_backward_compatibility():
    """Test that existing configs without new fields still work."""
    # Simulate old config that only has include_stored_procedures
    config_dict = {
        "include_stored_procedures": True,
        "procedure_pattern": {"allow": [".*"]},
        # include_stored_procedures_code is missing (old config)
    }

    # Should use default for missing field
    config = StoredProcedureConfigMixin(**config_dict)
    assert config.include_stored_procedures_code is True  # Default
