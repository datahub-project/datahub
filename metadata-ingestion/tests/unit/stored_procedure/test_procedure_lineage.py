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
procedure_sqls = [
    sql_file.name for sql_file in PROCEDURE_SQLS_DIR.iterdir() if sql_file.is_file()
]


@pytest.mark.parametrize("procedure_sql_file", procedure_sqls)
@pytest.mark.integration
def test_stored_procedure_lineage(procedure_sql_file: str) -> None:
    sql_file_path = PROCEDURE_SQLS_DIR / procedure_sql_file
    procedure_code = sql_file_path.read_text()

    # Procedure file is named as <platform>_<db>.<schema>.<procedure_name>
    platform_split = procedure_sql_file.split("_", 1)
    platform = platform_split[0]

    splits = platform_split[1].split(".")
    db = splits[0]
    schema = splits[1]
    name = splits[2]

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
