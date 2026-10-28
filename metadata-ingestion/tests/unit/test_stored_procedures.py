"""The shared stored-procedure emitters: the flow/job containers every SQL
source hangs procedures under, and the call-target parsing behind
procedure-to-procedure lineage."""

from typing import Dict, List, Optional, Type, TypeVar

from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import FlowContainerSubTypes
from datahub.ingestion.source.sql.stored_procedures.base import (
    generate_procedure_container_workunits,
    generate_procedure_lineage,
    generate_procedure_workunits,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import (
    parse_procedure_code,
)
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DataTransformLogicClass,
    _Aspect,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver

T = TypeVar("T", bound=_Aspect)


def _procedure(definition: Optional[str], name: str = "proc") -> BaseProcedure:
    return BaseProcedure(
        name=name,
        procedure_definition=definition,
        created=None,
        last_altered=None,
        comment="does things",
        argument_signature=None,
        return_type=None,
        language="SQL",
        extra_properties={"owner": "me"},
    )


def _aspect(workunits: List[MetadataWorkUnit], aspect_type: Type[T]) -> Optional[T]:
    found = [wu.get_aspect_of_type(aspect_type) for wu in workunits]
    present = [a for a in found if a is not None]
    assert len(present) <= 1
    return present[0] if present else None


INSTANCE_URN = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,prod)"


def test_platform_instance_is_stamped_on_flow_and_job() -> None:
    database_key = DatabaseKey(
        platform="mysql", database="db", instance="prod", env="PROD"
    )

    flow = list(
        generate_procedure_container_workunits(
            database_key, None, FlowContainerSubTypes.PROCEDURE_CONTAINER
        )
    )
    job = list(
        generate_procedure_workunits(_procedure("SELECT 1"), database_key, None, None)
    )

    flow_info = _aspect(flow, DataFlowInfoClass)
    assert flow_info is not None and flow_info.name == "db.stored_procedures"
    flow_dpi = _aspect(flow, DataPlatformInstanceClass)
    assert flow_dpi is not None and flow_dpi.instance == INSTANCE_URN
    job_dpi = _aspect(job, DataPlatformInstanceClass)
    assert job_dpi is not None and job_dpi.instance == INSTANCE_URN
    flow_container = _aspect(flow, ContainerClass)
    assert flow_container is not None
    assert flow_container.container == database_key.as_urn()
    # With no schema layer the job is parented by the database.
    job_container = _aspect(job, ContainerClass)
    assert (
        job_container is not None and job_container.container == database_key.as_urn()
    )
    logic = _aspect(job, DataTransformLogicClass)
    assert logic is not None
    statement = logic.transforms[0].queryStatement
    assert statement is not None and statement.value == "SELECT 1"


def test_a_flow_without_a_database_is_top_level() -> None:
    flow = list(
        generate_procedure_container_workunits(
            DatabaseKey(platform="mysql", database="", env="PROD"),
            None,
            FlowContainerSubTypes.PROCEDURE_CONTAINER,
        )
    )
    assert _aspect(flow, ContainerClass) is None
    flow_info = _aspect(flow, DataFlowInfoClass)
    assert flow_info is not None and flow_info.name == "stored_procedures"


def test_procedure_without_code_emits_no_transform_logic() -> None:
    job = list(
        generate_procedure_workunits(
            _procedure(None),
            DatabaseKey(platform="mysql", database="db", env="PROD"),
            None,
            None,
        )
    )
    assert _aspect(job, DataTransformLogicClass) is None
    assert _aspect(job, DataJobInputOutputClass) is None


def test_lineage_failure_is_reported_to_the_caller() -> None:
    failed: List[str] = []

    mcps = list(
        generate_procedure_lineage(
            schema_resolver=SchemaResolver(platform="mysql"),
            procedure=_procedure("SET @x = 1", name="no_lineage"),
            procedure_job_urn="urn:li:dataJob:(urn:li:dataFlow:(mysql,db.stored_procedures,PROD),no_lineage)",
            default_db="db",
            report_failure=failed.append,
        )
    )

    assert mcps == []
    assert failed == ["no_lineage"]


def test_catalogue_dependencies_are_merged_with_parsed_calls() -> None:
    parsed_call = "urn:li:dataJob:(urn:li:dataFlow:(postgres,db.sch.stored_procedures,PROD),helper)"
    from_catalogue = "urn:li:dataJob:(urn:li:dataFlow:(postgres,db.sch.stored_procedures,PROD),other)"

    mcps = list(
        generate_procedure_lineage(
            schema_resolver=SchemaResolver(platform="postgres"),
            procedure=_procedure("CALL helper(); INSERT INTO t SELECT * FROM s;"),
            procedure_job_urn="urn:li:dataJob:x",
            default_db="db",
            default_schema="sch",
            additional_input_jobs=[from_catalogue, parsed_call],
        )
    )

    assert len(mcps) == 1
    io = mcps[0].aspect
    assert isinstance(io, DataJobInputOutputClass)
    assert io.inputDatajobs == [parsed_call, from_catalogue]
    assert io.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.s,PROD)"
    ]


def _called_jobs(platform: str, code: str, **defaults: Optional[str]) -> List[str]:
    result = parse_procedure_code(
        schema_resolver=SchemaResolver(platform=platform),
        default_db=defaults.get("default_db", "db"),
        default_schema=defaults.get("default_schema", "sch"),
        code=code,
        is_temp_table=lambda _: False,
    )
    assert result is not None
    return list(result.inputDatajobs or [])


def test_quoted_call_targets_are_unquoted() -> None:
    expected = [
        "urn:li:dataJob:(urn:li:dataFlow:(postgres,db.sch.stored_procedures,PROD),helper)"
    ]
    assert _called_jobs("postgres", 'CALL "sch"."helper"();') == expected
    assert _called_jobs("postgres", "CALL db.sch.helper();") == expected
    assert _called_jobs("mssql", "EXEC [db].[sch].[helper]", default_db="db") == [
        "urn:li:dataJob:(urn:li:dataFlow:(mssql,db.sch.stored_procedures,PROD),helper)"
    ]


def test_two_tier_call_qualifier_is_a_database() -> None:
    # MySQL has no schema layer: `CALL other_db.helper()` names a database.
    assert _called_jobs("mysql", "CALL other_db.helper();", default_schema=None) == [
        "urn:li:dataJob:(urn:li:dataFlow:(mysql,other_db.stored_procedures,PROD),helper)"
    ]


def test_repeated_calls_and_blank_statements_collapse() -> None:
    code = "BEGIN\n CALL helper(); ; CALL helper();\nEND"
    assert len(_called_jobs("mysql", code, default_schema=None)) == 1


def test_procedure_with_schema_defaults_from_keys() -> None:
    database_key = DatabaseKey(platform="postgres", database="db", env="PROD")
    schema_key = SchemaKey(platform="postgres", database="db", schema="sch", env="PROD")
    dependencies: Dict[str, List[str]] = {}

    job = list(
        generate_procedure_workunits(
            _procedure("INSERT INTO target SELECT * FROM source"),
            database_key,
            schema_key,
            SchemaResolver(platform="postgres"),
            additional_input_jobs=dependencies.get("proc"),
        )
    )

    io = _aspect(job, DataJobInputOutputClass)
    assert io is not None
    assert io.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.source,PROD)"
    ]
    assert io.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.target,PROD)"
    ]
