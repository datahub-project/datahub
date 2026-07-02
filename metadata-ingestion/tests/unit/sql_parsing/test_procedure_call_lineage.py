"""Tests for stored-procedure CALL/EXEC lineage extraction.

These tests verify that `parse_procedure_code` recognises procedure invocations
inside a procedure body and emits them as `inputDatajobs` on the returned
`DataJobInputOutputClass`. The implementation is intentionally tolerant of the
two ways sqlglot models calls:

* TSQL ``EXEC``/``EXECUTE`` → structured ``Execute`` node
* All other dialects' ``CALL`` → unparseable ``Command`` literal, regex-extracted

The resolution rules being tested:

* Two-tier sources signal themselves with ``default_schema=None``. A bare
  ``CALL foo()`` resolves under the default database; a qualified
  ``CALL otherdb.foo()`` swaps in the explicit database.
* Three-tier sources fill the schema slot. Bare names use both defaults,
  two-part names override the schema, three-part names override both.

These are the same composition rules used by ``BaseProcedure.to_urn`` /
``get_procedure_flow_name`` so the resulting URNs join cleanly against the
DataJob URNs emitted for the called procedures elsewhere in ingestion.
"""

from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.sql_parsing.schema_resolver import SchemaResolver


def test_two_tier_unqualified_call_resolves_under_default_db():
    """``CALL foo()`` from a MySQL/MariaDB procedure must point at the same
    flow (``<db>.stored_procedures``) as the caller's flow URN."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """
    CALL process_customer_data();
    CALL generate_order_summaries(100.00);
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,  # two-tier
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),process_customer_data)",
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),generate_order_summaries)",
    ]
    # No DML in this body → no dataset lineage.
    assert not result.inputDatasets
    assert not result.outputDatasets


def test_two_tier_qualified_call_swaps_database():
    """``CALL otherdb.proc()`` in a two-tier source treats the leading qualifier
    as a database, not a schema."""
    schema_resolver = SchemaResolver(platform="mysql", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="caller_db",
        default_schema=None,
        code="CALL other_db.helper();",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mysql,other_db.stored_procedures,PROD),helper)"
    ]


def test_three_tier_unqualified_call_uses_default_db_and_schema():
    schema_resolver = SchemaResolver(platform="postgres", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="mydb",
        default_schema="public",
        code="CALL load_data();",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(postgres,mydb.public.stored_procedures,PROD),load_data)"
    ]


def test_three_tier_two_part_call_overrides_schema():
    schema_resolver = SchemaResolver(platform="postgres", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="mydb",
        default_schema="public",
        code="CALL etl.run_step();",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(postgres,mydb.etl.stored_procedures,PROD),run_step)"
    ]


def test_three_tier_three_part_call_overrides_both():
    schema_resolver = SchemaResolver(platform="postgres", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="mydb",
        default_schema="public",
        code="CALL warehouse.staging.compact();",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(postgres,warehouse.staging.stored_procedures,PROD),compact)"
    ]


def test_tsql_exec_resolves_via_structured_node():
    """TSQL ``EXEC`` parses as ``Execute`` with explicit db/schema/name."""
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="AdventureWorks",
        default_schema="dbo",
        code="EXEC dbo.usp_DoStuff @x = 1;",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mssql,AdventureWorks.dbo.stored_procedures,PROD),usp_DoStuff)"
    ]


def test_tsql_execute_with_db_schema_qualifier():
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="default_db",
        default_schema="dbo",
        code="EXECUTE OtherDB.audit.log_event;",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mssql,OtherDB.audit.stored_procedures,PROD),log_event)"
    ]


def test_tsql_bracketed_identifiers():
    """``EXEC [db].[schema].[proc]`` — bracket-quoted identifiers must have
    their brackets stripped for URN composition."""
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="default_db",
        default_schema="dbo",
        code="EXEC [Reporting].[etl].[refresh_all];",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mssql,Reporting.etl.stored_procedures,PROD),refresh_all)"
    ]


def test_call_lineage_combines_with_dml_lineage():
    """A body that both writes data AND calls another procedure must emit both
    table-level lineage and dataJob-level lineage in the same aspect.

    Two-tier procedure bodies typically reference tables by unqualified name
    (resolved against the procedure's home database), so the test mirrors that
    convention rather than fully qualifying.
    """
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """
    CALL upstream_loader();
    INSERT INTO target_table (id) SELECT id FROM source_table;
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),upstream_loader)"
    ]
    assert result.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,test_db.source_table,PROD)"
    ]
    assert result.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,test_db.target_table,PROD)"
    ]


def test_duplicate_calls_are_deduplicated():
    """Idempotent ``CALL`` of the same procedure twice must not produce
    duplicate ``inputDatajobs`` entries — the lineage relationship is the same."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """
    CALL helper();
    CALL helper();
    CALL helper();
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),helper)"
    ]


def test_call_with_no_default_context_is_skipped():
    """If we can't fill in even one part of the flow name, we silently drop the
    call rather than emit a malformed URN."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db=None,
        default_schema=None,
        code="CALL orphan();",
        is_temp_table=lambda _: False,
    )

    # Nothing usable in the body and no defaults to fall back to: parse_procedure_code
    # treats this as "no lineage" rather than emitting urn:li:dataFlow:(...,stored_procedures,...).
    assert result is None


def test_call_inside_view_or_function_does_not_match():
    """A SELECT that references a column literally named ``CALL`` must not be
    mistaken for a procedure invocation."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,
        code="INSERT INTO test_db.t (CALL) SELECT id FROM test_db.s;",
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert not result.inputDatajobs
