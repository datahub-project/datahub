import pathlib
from pathlib import Path
from typing import List, Optional

import pytest

from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.source.sql.stored_procedures.base import (
    generate_procedure_lineage,
    generate_procedure_workunits,
)
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.testing import mce_helpers

PROCEDURE_SQLS_DIR = pathlib.Path(__file__).parent / "procedures"
PROCEDURES_GOLDEN_DIR = pathlib.Path(__file__).parent / "golden_files"
platforms = [folder.name for folder in PROCEDURE_SQLS_DIR.iterdir() if folder.is_dir()]
procedure_sqls = [
    (platform, sql_file.name, f"{platform}/{sql_file.name}")
    for platform in platforms
    for sql_file in PROCEDURE_SQLS_DIR.glob(f"{platform}/*.sql")
]


@pytest.mark.parametrize("platform, sql_file_name, procedure_sql_file", procedure_sqls)
@pytest.mark.integration
def test_stored_procedure_lineage(
    platform: str, sql_file_name: str, procedure_sql_file: str
) -> None:
    sql_file_path = PROCEDURE_SQLS_DIR / procedure_sql_file
    procedure_code = sql_file_path.read_text()

    name = sql_file_name
    db = "default_db"
    schema = "default_schema"

    procedure = BaseProcedure(
        name=name,
        procedure_definition=procedure_code,
        created=None,
        last_altered=None,
        comment=None,
        argument_signature=None,
        return_type=None,
        language="SQL",
        extra_properties=None,
    )
    data_job_urn = f"urn:li:dataJob:(urn:li:dataFlow:({platform},{db}.{schema}.stored_procedures,PROD),{name})"

    schema_resolver = SchemaResolver(platform=platform)

    mcps = list(
        generate_procedure_lineage(
            schema_resolver=schema_resolver,
            procedure=procedure,
            procedure_job_urn=data_job_urn,
            default_db=db,
            default_schema=schema,
        )
    )
    mce_helpers.check_goldens_stream(
        outputs=mcps,
        golden_path=(
            PROCEDURES_GOLDEN_DIR / Path(procedure_sql_file).with_suffix(".json")
        ),
    )


def _call_lineage_datajobs(
    *, default_db: Optional[str], default_schema: Optional[str]
) -> List[str]:
    """Run a three-tier procedure whose body ``CALL``s another procedure through
    ``generate_procedure_workunits`` and return the resolved ``inputDatajobs``.

    The container keys always carry real ``realdb``/``realschema`` names, so the
    only thing that changes the outcome is the procedure's own
    ``default_db``/``default_schema`` and the three-value logic in
    ``generate_procedure_workunits`` that interprets them.
    """
    procedure = BaseProcedure(
        name="caller",
        procedure_definition="CALL helper();",
        created=None,
        last_altered=None,
        comment=None,
        argument_signature=None,
        return_type=None,
        language="SQL",
        extra_properties=None,
        default_db=default_db,
        default_schema=default_schema,
    )
    database_key = DatabaseKey(platform="postgres", database="realdb", env="PROD")
    schema_key = SchemaKey(
        platform="postgres", database="realdb", schema="realschema", env="PROD"
    )

    workunits = generate_procedure_workunits(
        procedure=procedure,
        database_key=database_key,
        schema_key=schema_key,
        schema_resolver=SchemaResolver(platform="postgres", env="PROD"),
    )

    datajobs: List[str] = []
    for wu in workunits:
        aspect = wu.get_aspect_of_type(DataJobInputOutputClass)
        if aspect:
            datajobs.extend(aspect.inputDatajobs or [])
    return datajobs


def test_none_default_db_schema_falls_back_to_container_keys() -> None:
    """``default_db``/``default_schema`` of ``None`` means "fall back to the
    database/schema container keys" — so an unqualified ``CALL`` resolves under
    ``realdb.realschema``."""
    assert _call_lineage_datajobs(default_db=None, default_schema=None) == [
        "urn:li:dataJob:(urn:li:dataFlow:(postgres,realdb.realschema.stored_procedures,PROD),helper)"
    ]


def test_empty_string_default_db_schema_suppresses_container_keys() -> None:
    """Empty-string ``default_db``/``default_schema`` means "explicitly no
    database/schema segment", NOT "fall back to the container keys". With no
    db/schema to compose and no qualifier on the call, the call is unresolvable
    and dropped — proving ``""`` overrides the ``realdb``/``realschema`` fallback
    that ``None`` would have used above."""
    assert _call_lineage_datajobs(default_db="", default_schema="") == []
