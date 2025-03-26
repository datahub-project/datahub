import pathlib
from pathlib import Path

import pytest

from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    generate_procedure_lineage,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.test_helpers import mce_helpers

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
