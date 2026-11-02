"""The shared stored-procedure emitters: the flow/job containers every SQL
source hangs procedures under, and the procedure-to-procedure lineage merge.

CALL/EXEC target resolution itself is covered by
tests/unit/sql_parsing/test_procedure_call_lineage.py."""

from typing import List, Optional

from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
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
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.unit.conftest import aspects_of_type, single_aspect
from tests.unit.sql_test_helpers import make_procedure


def _procedure(definition: Optional[str], name: str = "proc") -> BaseProcedure:
    return make_procedure(
        name, definition, comment="does things", extra_properties={"owner": "me"}
    )


INSTANCE_URN = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,prod)"


def test_platform_instance_is_stamped_on_flow_and_job() -> None:
    """With a platform instance and no schema layer (two-tier), the flow and
    job both carry the instance and hang off the database container, and the
    job keeps its code as transform logic."""
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

    assert single_aspect(flow, DataFlowInfoClass).name == "db.stored_procedures"
    assert single_aspect(flow, DataPlatformInstanceClass).instance == INSTANCE_URN
    assert single_aspect(job, DataPlatformInstanceClass).instance == INSTANCE_URN
    assert single_aspect(flow, ContainerClass).container == database_key.as_urn()
    assert single_aspect(job, ContainerClass).container == database_key.as_urn()
    statement = single_aspect(job, DataTransformLogicClass).transforms[0].queryStatement
    assert statement is not None and statement.value == "SELECT 1"


def test_a_flow_without_a_database_is_top_level() -> None:
    flow = list(
        generate_procedure_container_workunits(
            DatabaseKey(platform="mysql", database="", env="PROD"),
            None,
            FlowContainerSubTypes.PROCEDURE_CONTAINER,
        )
    )
    assert not aspects_of_type(flow, ContainerClass)
    assert single_aspect(flow, DataFlowInfoClass).name == "stored_procedures"


def test_procedure_without_code_emits_no_transform_logic() -> None:
    job = list(
        generate_procedure_workunits(
            _procedure(None),
            DatabaseKey(platform="mysql", database="db", env="PROD"),
            None,
            None,
        )
    )
    assert not aspects_of_type(job, DataTransformLogicClass)
    assert not aspects_of_type(job, DataJobInputOutputClass)


def test_lineage_failure_is_reported_to_the_caller() -> None:
    """A body with no lineage to extract is reported through the caller's
    callback (which sources count as a parse failure) and emits nothing."""
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
    """Dependencies a source reads from its catalogue are appended after the
    calls parsed from the body, without duplicating one the parser found."""
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


def test_double_quoted_call_targets_are_unquoted() -> None:
    result = parse_procedure_code(
        schema_resolver=SchemaResolver(platform="postgres"),
        default_db="db",
        default_schema="sch",
        code='CALL "sch"."helper"();',
        is_temp_table=lambda _: False,
    )
    assert result is not None
    assert result.inputDatajobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(postgres,db.sch.stored_procedures,PROD),helper)"
    ]


def test_procedure_table_lineage_defaults_from_the_container_keys() -> None:
    """Unqualified tables in the body resolve under the database and schema of
    the keys the procedure is emitted under."""
    database_key = DatabaseKey(platform="postgres", database="db", env="PROD")
    schema_key = SchemaKey(platform="postgres", database="db", schema="sch", env="PROD")

    job = list(
        generate_procedure_workunits(
            _procedure("INSERT INTO target SELECT * FROM source"),
            database_key,
            schema_key,
            SchemaResolver(platform="postgres"),
        )
    )

    io = single_aspect(job, DataJobInputOutputClass)
    assert io.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.source,PROD)"
    ]
    assert io.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sch.target,PROD)"
    ]
