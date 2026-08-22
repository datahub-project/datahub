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

from datahub.ingestion.source.sql.stored_procedures.lineage import (
    ProcedureParseReport,
    _count_call_arguments,
    parse_procedure_code,
)
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


def test_unresolvable_call_dropped_but_dml_lineage_survives():
    """A CALL that can't be resolved (no default db/schema, no qualifier) is
    dropped from ``inputDatajobs`` without erroring or discarding the DML
    lineage that shares the body. Exercises ``_build_call_datajob_urn``
    returning ``None`` while the overall result is still non-``None``."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """
    INSERT INTO reportdb.target (id) SELECT id FROM reportdb.source;
    CALL orphan();
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db=None,
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    # The orphan CALL is silently dropped, not raised on.
    assert not result.inputDatajobs
    # Fully-qualified DML still yields table lineage.
    assert result.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,reportdb.source,PROD)"
    ]
    assert result.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,reportdb.target,PROD)"
    ]


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


def test_begin_end_wrapped_body_recovers_first_call():
    """Real MySQL/MariaDB bodies are wrapped in ``BEGIN ... END``. The opening
    ``BEGIN`` glued onto the first ``CALL`` used to drop it — both must appear."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """BEGIN
        CALL process_customer_data();
        CALL generate_order_summaries(100.00);
    END"""
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),process_customer_data)",
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),generate_order_summaries)",
    ]


def test_labeled_begin_end_block_handled():
    """MariaDB labeled blocks: ``my_label: BEGIN ... END my_label``. The labeled
    opener is stripped and the labeled closer skipped, leaving clean call lineage."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """proc_body: BEGIN
        CALL first_step();
        CALL second_step();
    END proc_body"""
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),first_step)",
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),second_step)",
    ]


def test_begin_end_wrapped_body_recovers_first_dml_and_call():
    """The glued-on BEGIN must not drop first-statement DML either: an opening
    INSERT...SELECT plus a CALL should emit both dataset and dataJob lineage."""
    schema_resolver = SchemaResolver(platform="mariadb", env="PROD")

    code = """BEGIN
        INSERT INTO target_table (id) SELECT id FROM source_table;
        CALL downstream_step();
    END"""
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema=None,
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(mariadb,test_db.stored_procedures,PROD),downstream_step)"
    ]
    assert result.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,test_db.source_table,PROD)"
    ]
    assert result.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,test_db.target_table,PROD)"
    ]


def test_execute_immediate_is_not_a_call_target():
    """``EXECUTE IMMEDIATE '<sql>'`` builds its SQL at runtime, so there is no
    static call target. Without the guard the regex reads ``IMMEDIATE`` as the
    procedure name, and sources that compose the target urn (rather than
    resolving it) emit an edge to a DataJob that cannot exist."""
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")

    code = """
    EXECUTE IMMEDIATE 'INSERT INTO tgt SELECT a FROM src';
    INSERT INTO real_target (id) SELECT id FROM real_source;
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema="test_schema",
        code=code,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    # The static INSERT still produces lineage; the dynamic one contributes nothing.
    assert result.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:oracle,test_db.test_schema.real_target,PROD)"
    ]
    assert not result.inputDatajobs
    assert not any("immediate" in urn.lower() for urn in result.inputDatajobs or [])


def test_unparseable_body_is_reported_as_a_failure_not_as_empty():
    """A body whose statements fail to parse returns None, exactly like a body
    with nothing lineage-bearing in it. The parse report is what tells a caller
    which of the two it got."""
    schema_resolver = SchemaResolver(platform="snowflake", env="PROD")

    parse_report = ProcedureParseReport()
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema="test_schema",
        code="INSERT INTO tgt SELECT FROM WHERE ((",
        is_temp_table=lambda _: False,
        parse_report=parse_report,
    )

    assert result is None
    assert parse_report.failed
    assert parse_report.first_error

    quiet_report = ProcedureParseReport()
    assert (
        parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db="test_db",
            default_schema="test_schema",
            code="TRUNCATE TABLE tgt",
            is_temp_table=lambda _: False,
            parse_report=quiet_report,
        )
        is None
    )
    assert not quiet_report.failed


def test_count_call_arguments_counts_top_level_arguments_only():
    """Overload disambiguation rests on this count, so pin the awkward cases:
    commas inside string literals and nested calls don't separate arguments."""
    assert _count_call_arguments("('a,b')") == 1
    assert _count_call_arguments("(f(a,b), c)") == 2
    assert _count_call_arguments("()") == 0
    assert _count_call_arguments("(  )") == 0
    assert _count_call_arguments("(a, b") is None
    assert _count_call_arguments("") is None


def test_catalogue_input_jobs_survive_a_body_with_no_lineage():
    """`additional_input_jobs` comes from a system catalogue (Oracle's
    ALL_DEPENDENCIES), not from the code. A body the parser finds nothing in —
    an `EXECUTE IMMEDIATE`, a bare `BEGIN NULL; END` — must not take those edges
    down with it, or the caller silently loses dependencies it knows about."""
    schema_resolver = SchemaResolver(platform="oracle", env="PROD")
    catalogue_edge = (
        "urn:li:dataJob:(urn:li:dataFlow:"
        "(oracle,test_db.test_schema.stored_procedures,PROD),upstream_proc)"
    )

    for code in (
        "EXECUTE IMMEDIATE 'INSERT INTO tgt SELECT a FROM src'",
        "BEGIN NULL; END;",
    ):
        result = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db="test_db",
            default_schema="test_schema",
            code=code,
            is_temp_table=lambda _: False,
            additional_input_jobs=[catalogue_edge],
        )
        assert result is not None, code
        assert result.inputDatajobs == [catalogue_edge], code

    # With nothing from either source there is still no aspect to emit.
    assert (
        parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db="test_db",
            default_schema="test_schema",
            code="BEGIN NULL; END;",
            is_temp_table=lambda _: False,
        )
        is None
    )


def test_tsql_execute_immediate_is_not_a_call_target():
    """TSQL parses `EXECUTE IMMEDIATE '<sql>'` into a structured Execute node,
    where the keyword lands in the target slot and reads as a procedure named
    IMMEDIATE. That is the same dynamic SQL as the Command-literal form, and the
    same non-existent target — guarding only one of the two paths left MSSQL and
    any other structured-Execute dialect emitting a bogus edge."""
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema="test_schema",
        code="""
        EXECUTE IMMEDIATE 'INSERT INTO tgt SELECT a FROM src';
        INSERT INTO real_target (id) SELECT id FROM real_source;
        """,
        is_temp_table=lambda _: False,
    )

    assert result is not None
    assert not result.inputDatajobs
    # An ordinary EXEC on the same path still resolves, so the guard is not
    # swallowing real calls.
    with_call = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema="test_schema",
        code="EXEC test_db.test_schema.immediate_refresh",
        is_temp_table=lambda _: False,
    )
    assert with_call is not None
    assert with_call.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:"
        "(mssql,test_db.test_schema.stored_procedures,PROD),immediate_refresh)"
    ]


def test_resolve_procedure_urn_callback_owns_the_target_urn():
    """The callback is the whole point of the hook: the source maps a call onto a
    procedure it actually ingested, because the urn's job id carries an
    argument-signature hash the call site cannot see. Cover all three paths —
    resolution, refusal, and what the reference carries."""
    schema_resolver = SchemaResolver(platform="snowflake", env="PROD")
    seen = []

    def resolver(ref):
        seen.append(ref)
        return (
            "urn:li:dataJob:(urn:li:dataFlow:"
            "(snowflake,test_db.test_schema.stored_procedures,PROD),"
            "my_proc_deadbeef)"
        )

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema="test_schema",
        code="CALL my_proc('a', 'b')",
        is_temp_table=lambda _: False,
        resolve_procedure_urn=resolver,
    )

    # The composed form would have been ...,my_proc) with no hash; the callback's
    # answer is used verbatim instead.
    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:"
        "(snowflake,test_db.test_schema.stored_procedures,PROD),my_proc_deadbeef)"
    ]

    # The reference arrives with the caller's defaults already applied, and with
    # the call-site arity that lets a source narrow overloads.
    assert len(seen) == 1
    assert (seen[0].database, seen[0].db_schema, seen[0].name) == (
        "test_db",
        "test_schema",
        "my_proc",
    )
    assert seen[0].argument_count == 2

    # Refusal means "not ingested here": no edge at all, rather than a composed
    # urn pointing at a DataJob that was never emitted.
    refused = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="test_db",
        default_schema="test_schema",
        code="CALL my_proc('a')",
        is_temp_table=lambda _: False,
        resolve_procedure_urn=lambda ref: None,
    )
    assert refused is None
