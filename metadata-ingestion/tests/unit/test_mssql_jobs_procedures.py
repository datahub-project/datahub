"""SQLServerSource behaviour around SQL Agent jobs, stored procedures and the
per-database inspector walk, against a stubbed SQLAlchemy connection."""

import logging
from typing import Any, Dict, List, Optional, Set, Union
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import (
    DatabaseError,
    DBAPIError,
    OperationalError,
    ProgrammingError,
    ResourceClosedError,
)
from sqlalchemy.sql import quoted_name

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    FlowContainerSubTypes,
    JobContainerSubTypes,
)
from datahub.ingestion.source.sql.mssql.job_models import (
    MSSQLProceduresContainer,
    StoredProcedure,
)
from datahub.ingestion.source.sql.mssql.source import (
    SQLServerConfig,
    SQLServerSource,
    _handle_sql_variant_as_string,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DataTransformLogicClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from tests.unit.conftest import (
    as_mock,
    aspects_of_type,
    log_contexts,
    log_messages,
    log_titles,
    single_aspect,
)
from tests.unit.sql_test_helpers import FakeConnection, FakeResult

Rows = List[Dict[str, Any]]
Outcome = Union[Rows, Exception]


def _source(**overrides: Any) -> SQLServerSource:
    config = SQLServerConfig.model_validate(
        {
            "host_port": "localhost:1433",
            "username": "sa",
            "password": "pw",
            "database": "TestDB",
            "include_descriptions": False,
            **overrides,
        }
    )
    return SQLServerSource(config, PipelineContext(run_id="test"))


def _inspector(database: str = "TestDB") -> MagicMock:
    inspector = MagicMock()
    inspector.engine.url = make_url(f"mssql+pytds://sa:pw@localhost:1433/{database}")
    return inspector


def _conn(inspector: MagicMock) -> MagicMock:
    return inspector.engine.connect.return_value.__enter__.return_value


def _workunit(name: str) -> MagicMock:
    return MagicMock(spec=MetadataWorkUnit, name=name)


def _runtime_warnings(source: SQLServerSource) -> Set[str]:
    """Contexts of the run-time warnings, without the lineage-casing
    advisories __init__ emits (titled "Potential issue ...")."""
    return log_contexts(
        w
        for w in source.report.warnings
        if not str(w.title or "").startswith("Potential issue")
    )


# --- SQL Agent jobs ---


def _step(**overrides: Any) -> Dict[str, Any]:
    return {
        "job_id": "job-1",
        "job_name": "Nightly, Load",
        "description": "loads orders",
        "date_created": "2024-01-01",
        "date_modified": "2024-02-01",
        "step_id": 1,
        "step_name": "Load Orders",
        "subsystem": "TSQL",
        "command": "EXEC dbo.LoadOrders",
        "database_name": "TestDB",
        **overrides,
    }


def test_loop_jobs_emits_a_flow_per_job_and_a_task_per_step() -> None:
    source = _source(platform_instance="prod", include_containers_for_pipelines=True)
    jobs = {"Nightly, Load": {1: _step()}}

    with patch.object(source, "_get_jobs", return_value=jobs):
        workunits = list(source.loop_jobs(_inspector(), source.config))

    flow = single_aspect(workunits, DataFlowInfoClass)
    # Commas would break the URN, so the name is sanitised.
    assert flow.name == "Nightly- Load"
    job = single_aspect(workunits, DataJobInfoClass)
    assert job.name == "Nightly- Load"
    assert job.type == "MSSQL_JOB_STEP"
    assert job.customProperties["command"] == "EXEC dbo.LoadOrders"
    assert job.customProperties["step_name"] == "Load Orders"
    assert job.customProperties["database_name"] == "TestDB"
    assert [s.typeNames for s in aspects_of_type(workunits, SubTypesClass)] == [
        [FlowContainerSubTypes.MSSQL_JOB],
        [JobContainerSubTypes.MSSQL_JOBSTEP],
    ]
    # Flow and step both carry the platform instance and database container.
    assert len(aspects_of_type(workunits, DataPlatformInstanceClass)) == 2
    assert len(aspects_of_type(workunits, ContainerClass)) == 2
    # Job steps carry no parsed lineage, but the aspect is still emitted.
    assert len(aspects_of_type(workunits, DataJobInputOutputClass)) == 1


@pytest.mark.xfail(
    strict=True,
    reason=(
        "loop_job_steps stringifies every step field with str(), so a NULL "
        "description/command lands in customProperties as the literal 'None'"
    ),
)
def test_null_job_step_fields_are_not_stringified_into_properties() -> None:
    source = _source()
    with patch.object(
        source,
        "_get_jobs",
        return_value={"J": {1: _step(description=None, command=None)}},
    ):
        workunits = list(source.loop_jobs(_inspector(), source.config))

    job = single_aspect(workunits, DataJobInfoClass)
    assert "None" not in job.customProperties.values()


def test_loop_jobs_without_containers_or_platform_instance() -> None:
    source = _source()
    with patch.object(source, "_get_jobs", return_value={"J": {1: _step()}}):
        workunits = list(source.loop_jobs(_inspector(), source.config))

    assert not aspects_of_type(workunits, DataPlatformInstanceClass)
    assert not aspects_of_type(workunits, ContainerClass)


def test_loop_jobs_reports_a_failing_job_and_continues() -> None:
    """One job whose flow cannot be built must not take the other jobs with it."""
    source = _source()
    jobs = {"BadJob": {1: _step(job_name="BadJob")}, "GoodJob": {1: _step()}}
    flow_wu = _workunit("flow")
    step_wu = _workunit("step")

    with (
        patch.object(source, "_get_jobs", return_value=jobs),
        patch.object(
            source,
            "construct_flow_workunits",
            side_effect=[RuntimeError("bad flow"), iter([flow_wu])],
        ),
        patch.object(source, "loop_job_steps", side_effect=lambda *_: iter([step_wu])),
    ):
        workunits = list(source.loop_jobs(_inspector(), source.config))

    assert workunits == [flow_wu, step_wu]
    assert "BadJob" in _runtime_warnings(source)


def test_loop_jobs_yields_nothing_when_there_are_no_jobs() -> None:
    source = _source()
    with patch.object(source, "_get_jobs", return_value={}):
        assert list(source.loop_jobs(_inspector(), source.config)) == []


def test_loop_jobs_propagates_a_failed_listing() -> None:
    source = _source()
    with (
        patch.object(
            source,
            "_get_jobs",
            side_effect=ProgrammingError(
                "EXECUTE permission denied", None, Exception()
            ),
        ),
        pytest.raises(ProgrammingError),
    ):
        list(source.loop_jobs(_inspector(), source.config))


def test_database_level_workunits_report_job_failures_without_aborting() -> None:
    """msdb access is commonly denied; the tables and views of the database
    must still be emitted, with the job listing recorded as a failure."""
    source = _source()
    base_wu = _workunit("base")

    with (
        patch.object(
            SQLAlchemySource,
            "get_database_level_workunits",
            return_value=iter([base_wu]),
        ),
        patch.object(source, "loop_jobs", side_effect=RuntimeError("msdb denied")),
    ):
        workunits = list(source.get_database_level_workunits(_inspector(), "TestDB"))

    assert workunits == [base_wu]
    assert "Failed to list jobs" in log_messages(source.report.failures)


def test_job_fallback_reports_structure_and_unexpected_errors() -> None:
    """Each listing strategy that fails is reported under its own context
    before the next one is tried; the first that works wins."""
    source = _source()
    jobs = {"J": {1: _step()}}

    def malformed(conn: Any, db: str) -> Dict[str, Any]:
        raise KeyError("step_id")

    def exploded(conn: Any, db: str) -> Dict[str, Any]:
        raise RuntimeError("driver crashed")

    def works(conn: Any, db: str) -> Dict[str, Any]:
        return jobs

    result = source._get_jobs_with_fallback(
        MagicMock(), "TestDB", [malformed, exploded, works], is_rds=False
    )

    assert result == jobs
    assert {"job_structure_error_on_prem", "job_extraction_error_on_prem"} <= (
        log_contexts(source.report.failures)
    )


def test_jobs_via_stored_procedures_keep_steps_in_this_database_and_skip_unreadable_jobs() -> (
    None
):
    """sp_help_jobstep is read per job: steps targeting another database are
    dropped, and a job whose steps cannot be read is skipped rather than fatal."""
    source = _source()
    job_rows = [
        {"job_id": "1", "name": "JobA", "description": "a"},
        {"job_id": "2", "name": "JobB"},
        {"job_id": "3", "name": "JobC"},
        {"job_id": "4", "name": "JobD"},
    ]
    steps: Dict[str, Outcome] = {
        "1": [
            {"step_id": 1, "step_name": "here", "database_name": "testdb"},
            {"step_id": 2, "step_name": "elsewhere", "database_name": "OtherDB"},
            {"step_id": 3, "step_name": "unscoped", "database_name": ""},
        ],
        "2": DatabaseError("job deleted", None, Exception()),
        "3": KeyError("step_id"),
        "4": RuntimeError("unexpected"),
    }

    def steps_for(params: Optional[Dict[str, Any]]) -> Outcome:
        assert params is not None
        return steps[params["job_id"]]

    conn = MagicMock(spec=Connection)
    conn.execute.side_effect = FakeConnection(
        {"sp_help_jobstep": steps_for, "": job_rows}
    ).execute

    jobs = source._get_jobs_via_stored_procedures(conn, "TestDB")

    assert list(jobs) == ["JobA"]
    # Keyed by step_id, an int, although the annotation says str.
    steps_found: Dict[Any, Any] = jobs["JobA"]
    assert sorted(steps_found) == [1, 3]
    assert steps_found[1]["job_name"] == "JobA"
    assert steps_found[1]["description"] == "a"
    assert steps_found[1]["command"] == ""


# --- stored procedures ---


def _procedure_catalog(**overrides: Outcome) -> Dict[str, Outcome]:
    """The catalog reads loop_stored_procedures makes, keyed by a needle in each statement."""
    handlers: Dict[str, Outcome] = {
        "[sys].[procedures]": [
            {"procedure_name": "LoadOrders", "schema_name": "dbo"},
            {"procedure_name": "Skipped", "schema_name": "dbo"},
        ],
        "referenced_schema_name": [
            {"db": "testdb", "schema": "dbo", "name": "staging", "type": "USER_TABLE"}
        ],
        "OBJECT_SCHEMA_NAME ( referencing_id )": [
            {"schema": "dbo", "name": "orders_view", "type": "VIEW"}
        ],
        "sys.parameters": [{"name": "@since", "type": "datetime"}],
        "sp_helptext": [
            {"Text": "-- header\n"},
            {"Text": "CREATE  PROCEDURE dbo.LoadOrders @since datetime AS\n"},
            {"Text": "INSERT INTO dbo.orders SELECT * FROM dbo.staging\n"},
        ],
        "FROM sys.procedures": [
            {"date_created": "2024-01-01", "date_modified": "2024-02-01"}
        ],
    }
    handlers.update(overrides)
    return handlers


def _inspector_with_catalog(**overrides: Outcome) -> MagicMock:
    inspector = _inspector()
    _conn(inspector).execute.side_effect = FakeConnection(
        _procedure_catalog(**overrides)
    ).execute
    return inspector


def test_loop_stored_procedures_emits_jobs_with_dependencies_code_and_parameters() -> (
    None
):
    source = _source(
        platform_instance="prod",
        include_containers_for_pipelines=True,
        procedure_pattern={"deny": [r"TestDB\.dbo\.Skipped"]},
    )

    workunits = list(
        source.loop_stored_procedures(_inspector_with_catalog(), "dbo", source.config)
    )

    flow = single_aspect(workunits, DataFlowInfoClass)
    assert flow.name == "TestDB.dbo.stored_procedures"
    job = single_aspect(workunits, DataJobInfoClass)
    assert job.name == "TestDB.dbo.LoadOrders"
    props = job.customProperties
    assert props["procedure_depends_on"] == str({"testdb.dbo.staging": "USER_TABLE"})
    assert props["depending_on_procedure"] == str({"TestDB.dbo.orders_view": "VIEW"})
    # sp_helptext rows are split at the CREATE PROCEDURE line.
    assert props["definition"] == "-- header\n"
    assert props["code"].startswith("CREATE  PROCEDURE dbo.LoadOrders")
    assert props["input parameters"] == "['@since']"
    assert props["parameter @since"] == str({"type": "datetime"})
    assert props["date_created"] == "2024-01-01"
    statement = single_aspect(workunits, DataTransformLogicClass).transforms[0]
    assert statement.queryStatement is not None
    assert statement.queryStatement.value == props["code"]
    assert [s.typeNames for s in aspects_of_type(workunits, SubTypesClass)] == [
        [FlowContainerSubTypes.PROCEDURE_CONTAINER],
        [JobContainerSubTypes.STORED_PROCEDURE],
    ]
    assert len(aspects_of_type(workunits, ContainerClass)) == 2
    # Lineage is parsed at the end of the run, not here.
    assert not aspects_of_type(workunits, DataJobInputOutputClass)
    assert list(source.report.filtered) == ["TestDB.dbo.Skipped"]
    assert [p.name for p in source.stored_procedures] == ["LoadOrders"]
    assert source.stored_procedures[0].code == props["code"]


def test_loop_stored_procedures_emits_no_flow_when_every_procedure_is_denied() -> None:
    source = _source(procedure_pattern={"deny": [".*"]})

    assert (
        list(
            source.loop_stored_procedures(
                _inspector_with_catalog(), "dbo", source.config
            )
        )
        == []
    )
    assert set(source.report.filtered) == {
        "TestDB.dbo.LoadOrders",
        "TestDB.dbo.Skipped",
    }


def test_procedure_code_is_omitted_when_sp_helptext_is_denied() -> None:
    flow = MSSQLProceduresContainer(
        name="TestDB.dbo.stored_procedures",
        env="PROD",
        db="TestDB",
        platform_instance=None,
    )
    procedure = StoredProcedure(db="TestDB", schema="dbo", name="P", flow=flow)

    denied = MagicMock()
    denied.execute.side_effect = ProgrammingError("denied", None, Exception())
    assert SQLServerSource._get_procedure_code(denied, procedure) == (None, None)

    closed = MagicMock()
    closed.execute.return_value.mappings.return_value.__iter__.side_effect = (
        ResourceClosedError("closed")
    )
    assert SQLServerSource._get_procedure_code(closed, procedure) == (None, None)


def test_a_failing_procedure_is_reported_and_the_rest_are_processed() -> None:
    """Recorded as a failure, not a warning: stale-entity removal suppresses
    soft-deletion only on a failure, and a skipped procedure must not be deleted."""
    source = _source()
    ok_wu = _workunit("ok")

    with patch.object(
        source,
        "_process_stored_procedure",
        side_effect=[RuntimeError("bad definition"), iter([ok_wu])],
    ):
        workunits = list(
            source.loop_stored_procedures(
                _inspector_with_catalog(), "dbo", source.config
            )
        )

    assert workunits[-1] is ok_wu
    assert "Failed to process stored procedure" in log_titles(source.report.failures)
    assert "TestDB.dbo.LoadOrders" in log_contexts(source.report.failures)


@pytest.mark.parametrize(
    "fatal",
    [
        DBAPIError("stmt", None, Exception(), connection_invalidated=True),
        ConnectionError("socket closed"),
    ],
)
def test_a_dead_connection_stops_the_schema(fatal: Exception) -> None:
    """A dead connection would fail every remaining procedure the same way,
    so it propagates instead of being reported once per procedure."""
    source = _source()

    with (
        patch.object(source, "_process_stored_procedure", side_effect=fatal),
        pytest.raises(type(fatal)),
    ):
        list(
            source.loop_stored_procedures(
                _inspector_with_catalog(), "dbo", source.config
            )
        )


def test_stored_procedure_lineage_is_parsed_after_the_schema_walk() -> None:
    """Procedure bodies are parsed once every dataset is known, so the lineage
    can be resolved against the discovered (here: lowercased) dataset names."""
    source = _source(
        convert_urns_to_lowercase=True, convert_column_urns_to_lowercase=True
    )
    flow = MSSQLProceduresContainer(
        name="testdb.dbo.stored_procedures",
        env="PROD",
        db="testdb",
        platform_instance=None,
    )
    source.stored_procedures.append(
        StoredProcedure(
            db="testdb",
            schema="dbo",
            name="LoadOrders",
            flow=flow,
            code=(
                "CREATE PROCEDURE dbo.LoadOrders AS BEGIN "
                "INSERT INTO dbo.orders SELECT * FROM dbo.staging END"
            ),
        )
    )
    source.discovered_datasets = {"testdb.dbo.orders", "testdb.dbo.staging"}

    with patch.object(
        SQLAlchemySource, "get_workunits_internal", return_value=iter([])
    ):
        workunits = list(source.get_workunits_internal())

    lineage = single_aspect(workunits, DataJobInputOutputClass)
    assert lineage.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.dbo.staging,PROD)"
    ]
    assert lineage.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.dbo.orders,PROD)"
    ]
    assert (
        workunits[0].get_urn()
        == "urn:li:dataJob:(urn:li:dataFlow:(mssql,testdb.dbo.stored_procedures,PROD),LoadOrders)"
    )


def test_report_procedure_failure_counts_on_the_aggregator_report() -> None:
    source = _source()
    source._report_procedure_failure("dbo.Broken")
    assert source.aggregator.report.num_procedures_failed == 1
    assert "dbo.Broken" in source.aggregator.report.procedure_parse_failures


# --- inspectors, identifiers and reflection overrides ---


@patch("datahub.ingestion.source.sql.mssql.source.inspect")
@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_get_inspectors_walks_allowed_databases_and_skips_failed_logins(
    create_engine: MagicMock, inspect_mock: MagicMock
) -> None:
    """A database the login cannot open is warned about and skipped; denied
    databases are never even connected to."""
    source = _source(database=None, database_pattern={"deny": ["Denied"]})

    def make_engine(url: str, **_: Any) -> MagicMock:
        engine = MagicMock()
        engine.url_text = url
        listing = engine.begin.return_value.__enter__.return_value
        listing.execute.return_value = FakeResult(
            [{"name": "Alpha"}, {"name": "Broken"}, {"name": "Denied"}]
        )
        return engine

    def inspect_engine(engine: MagicMock) -> MagicMock:
        if "Broken" in engine.url_text:
            raise OperationalError("Login failed for user 'sa'", None, Exception())
        return MagicMock(name=engine.url_text)

    create_engine.side_effect = make_engine
    inspect_mock.side_effect = inspect_engine

    inspectors = list(source.get_inspectors())

    assert len(inspectors) == 1
    assert source.current_database == "Alpha"
    assert "Broken" in _runtime_warnings(source)
    opened = [c.args[0] for c in create_engine.call_args_list]
    assert not any("Denied" in url for url in opened)


@patch("datahub.ingestion.source.sql.mssql.source.inspect")
@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_get_inspectors_propagates_connection_errors_other_than_login(
    create_engine: MagicMock, inspect_mock: MagicMock
) -> None:
    source = _source(database=None)
    listing = create_engine.return_value.begin.return_value.__enter__.return_value
    listing.execute.return_value = FakeResult([{"name": "Alpha"}])
    inspect_mock.side_effect = OperationalError("timeout", None, Exception())

    with pytest.raises(OperationalError):
        list(source.get_inspectors())


def test_get_identifier_follows_the_database_being_walked() -> None:
    source = _source(database=None, convert_urns_to_lowercase=True)
    source.current_database = "Sales"
    assert (
        source.get_identifier(schema="dbo", entity="Orders", inspector=MagicMock())
        == "sales.dbo.orders"
    )


def test_table_description_prefers_the_extended_property() -> None:
    source = _source()
    source.table_descriptions = {"TestDB.dbo.Orders": "from MS_Description"}

    with patch.object(
        SQLAlchemySource,
        "get_table_properties",
        return_value=("reflected", {"k": "v"}, None),
    ):
        description, properties, _ = source.get_table_properties(
            _inspector(), "dbo", "Orders"
        )
        _, other_props, _ = source.get_table_properties(_inspector(), "dbo", "Other")

    assert description == "from MS_Description"
    assert properties == {"k": "v"}
    assert other_props == {"k": "v"}


def test_descriptions_are_keyed_by_three_part_name() -> None:
    source = _source()
    conn = MagicMock()
    conn.execute.return_value.mappings.side_effect = [
        [{"schema_name": "dbo", "table_name": "Orders", "table_description": "t"}],
        [
            {
                "schema_name": "dbo",
                "table_name": "Orders",
                "column_name": "Id",
                "column_description": "c",
            }
        ],
    ]

    source._populate_table_descriptions(conn, "TestDB")
    source._populate_column_descriptions(conn, "TestDB")

    assert source.table_descriptions == {"TestDB.dbo.Orders": "t"}
    assert source.column_descriptions == {"TestDB.dbo.Orders.Id": "c"}


def test_schema_fields_follow_the_column_lowercase_flag() -> None:
    field = SchemaFieldClass(
        fieldPath="AccountID",
        type=SchemaFieldDataTypeClass(StringTypeClass()),
        nativeDataType="varchar",
    )
    with patch.object(SQLAlchemySource, "get_schema_fields", return_value=[field]):
        lowered = _source(convert_column_urns_to_lowercase=True).get_schema_fields(
            "testdb.dbo.t", [], MagicMock()
        )
        assert [f.fieldPath for f in lowered] == ["accountid"]

        field.fieldPath = "AccountID"
        kept = _source().get_schema_fields("testdb.dbo.t", [], MagicMock())
        assert [f.fieldPath for f in kept] == ["AccountID"]


def test_foreign_key_columns_follow_the_column_lowercase_flag() -> None:
    source = _source(convert_column_urns_to_lowercase=True)
    fk = {
        "name": "fk_orders_customers",
        "constrained_columns": ["CustomerID"],
        "referred_schema": "dbo",
        "referred_table": "Customers",
        "referred_columns": ["ID"],
    }
    with patch.object(
        SQLAlchemySource, "get_foreign_key_metadata", return_value=MagicMock()
    ) as base:
        source.get_foreign_key_metadata("urn:li:dataset:x", "dbo", fk, MagicMock())

    passed: Dict[str, Any] = base.call_args.args[2]
    assert passed["constrained_columns"] == ["customerid"]
    assert passed["referred_columns"] == ["id"]
    # The caller's dict is left alone.
    assert fk["constrained_columns"] == ["CustomerID"]


@pytest.mark.parametrize("quote", [True, False])
def test_allowed_schemas_are_quoted_only_when_configured(quote: bool) -> None:
    source = _source(quote_schemas=quote)
    with patch.object(
        SQLAlchemySource, "get_allowed_schemas", return_value=iter(["sales.archive"])
    ):
        (schema,) = list(source.get_allowed_schemas(_inspector(), "TestDB"))

    assert schema == "sales.archive"
    assert isinstance(schema, quoted_name) == quote


def test_get_db_name_reads_the_engine_url_or_fails_loudly() -> None:
    source = _source()
    assert source.get_db_name(_inspector("Sales")) == "Sales"

    no_engine = MagicMock()
    no_engine.engine = None
    assert source.get_db_name(no_engine) == ""

    broken = MagicMock()
    type(broken.engine).url = PropertyMock(side_effect=ValueError("no url"))
    with pytest.raises(RuntimeError):
        source.get_db_name(broken)


def test_close_closes_the_aggregator() -> None:
    source = _source()
    source.aggregator = create_autospec(SqlParsingAggregator, instance=True)
    source.close()
    as_mock(source.aggregator).close.assert_called_once()


def test_query_log_source_fails_when_no_database_had_a_log() -> None:
    source = _source(include_query_lineage=True, query_history_source="audit_log")
    with patch.object(source, "get_inspectors", return_value=[]):
        source._populate_aggregator_with_query_history()

    assert "No query log available" in log_titles(source.report.failures)


# --- config and driver helpers ---


def test_query_history_config_limits() -> None:
    with pytest.raises(ValidationError, match="<= 10000"):
        _source(max_queries_to_extract=20000)
    with pytest.raises(ValidationError, match="empty or whitespace"):
        _source(query_exclude_patterns=["%ok%", "   "])
    with pytest.raises(ValidationError, match="cannot exceed 100"):
        _source(query_exclude_patterns=[f"p{i}" for i in range(101)])
    with pytest.raises(ValidationError, match="exceeds 500 characters"):
        _source(query_exclude_patterns=["x" * 501])


def test_odbc_recipes_must_name_a_driver() -> None:
    with pytest.raises(ValidationError, match="driver"):
        SQLServerConfig.model_validate(
            {
                "host_port": "localhost:1433",
                "username": "sa",
                "password": "pw",
                "uri_args": {"TrustServerCertificate": "yes"},
            },
            context={"is_odbc": True},
        )


def test_per_query_usage_without_query_lineage_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        _source(include_query_usage_statistics=True, include_query_lineage=False)
    assert any("include_query_usage_statistics" in r.message for r in caplog.records)


def test_sql_variant_values_decode_as_utf16_then_windows_1251() -> None:
    assert _handle_sql_variant_as_string("héllo".encode("utf-16le")) == "héllo"
    # An odd byte count cannot be UTF-16; the legacy single-byte fallback applies.
    assert _handle_sql_variant_as_string(b"\xe9") == "\xe9".encode("latin-1").decode(
        "windows-1251"
    )
