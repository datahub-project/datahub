"""Simplified unit tests for stored procedure base functionality."""

from datahub.emitter.mcp_builder import DatabaseKey
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    _generate_job_workunits,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataTransformLogicClass,
)


def test_include_stored_procedures_code_true():
    """Test that DataTransformLogicClass is emitted when include_stored_procedures_code=True."""
    procedure = BaseProcedure(
        name="test_proc",
        language="SQL",
        argument_signature=None,
        return_type=None,
        procedure_definition="SELECT 1",
        created=None,
        last_altered=None,
        comment=None,
        extra_properties=None,
    )

    database_key = DatabaseKey(
        database="testdb",
        platform="mysql",
        instance=None,
        env="PROD",
    )

    # Call with include_stored_procedures_code=True (default)
    workunits = list(
        _generate_job_workunits(
            database_key, None, procedure, include_stored_procedures_code=True
        )
    )

    # Check that DataTransformLogicClass is present
    transform_workunits = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataTransformLogicClass)
    ]
    assert len(transform_workunits) > 0, (
        "DataTransformLogicClass should be emitted when include_stored_procedures_code=True"
    )

    # Verify the SQL code is in the aspect
    wu = transform_workunits[0]
    assert hasattr(wu.metadata, "aspect")
    transform_aspect = wu.metadata.aspect
    assert isinstance(transform_aspect, DataTransformLogicClass)
    assert transform_aspect.transforms[0].queryStatement is not None
    assert transform_aspect.transforms[0].queryStatement.value == "SELECT 1"


def test_include_stored_procedures_code_false():
    """Test that DataTransformLogicClass is NOT emitted when include_stored_procedures_code=False."""
    procedure = BaseProcedure(
        name="test_proc",
        language="SQL",
        argument_signature=None,
        return_type=None,
        procedure_definition="SELECT 1",
        created=None,
        last_altered=None,
        comment=None,
        extra_properties=None,
    )

    database_key = DatabaseKey(
        database="testdb",
        platform="postgres",
        instance=None,
        env="PROD",
    )

    # Call with include_stored_procedures_code=False
    workunits = list(
        _generate_job_workunits(
            database_key, None, procedure, include_stored_procedures_code=False
        )
    )

    # Check that DataTransformLogicClass is NOT present
    transform_workunits = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataTransformLogicClass)
    ]
    assert len(transform_workunits) == 0, (
        "DataTransformLogicClass should NOT be emitted when include_stored_procedures_code=False"
    )

    # But DataJobInfoClass should still be present
    job_info_workunits = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataJobInfoClass)
    ]
    assert len(job_info_workunits) > 0, "DataJobInfoClass should still be emitted"


def test_include_stored_procedures_code_no_definition():
    """Test that DataTransformLogicClass is not emitted when procedure_definition is None."""
    procedure = BaseProcedure(
        name="test_proc",
        language="SQL",
        argument_signature=None,
        return_type=None,
        procedure_definition=None,  # ← No definition
        created=None,
        last_altered=None,
        comment=None,
        extra_properties=None,
    )

    database_key = DatabaseKey(
        database="testdb",
        platform="oracle",
        instance=None,
        env="PROD",
    )

    # Even with include_stored_procedures_code=True, no aspect should be emitted
    workunits = list(
        _generate_job_workunits(
            database_key, None, procedure, include_stored_procedures_code=True
        )
    )

    transform_workunits = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataTransformLogicClass)
    ]
    assert len(transform_workunits) == 0, (
        "DataTransformLogicClass should NOT be emitted when procedure_definition is None"
    )


def test_include_stored_procedures_code_default():
    """Test that the default value is True (backward compatible)."""
    procedure = BaseProcedure(
        name="test_proc",
        language="SQL",
        argument_signature=None,
        return_type=None,
        procedure_definition="SELECT 1",
        created=None,
        last_altered=None,
        comment=None,
        extra_properties=None,
    )

    database_key = DatabaseKey(
        database="testdb",
        platform="snowflake",
        instance=None,
        env="PROD",
    )

    # Call without specifying include_stored_procedures_code (uses default)
    workunits = list(_generate_job_workunits(database_key, None, procedure))

    # Default should be True, so DataTransformLogicClass should be present
    transform_workunits = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataTransformLogicClass)
    ]
    assert len(transform_workunits) > 0, (
        "DataTransformLogicClass should be emitted by default (include_stored_procedures_code defaults to True)"
    )
