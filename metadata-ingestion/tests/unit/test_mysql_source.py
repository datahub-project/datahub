from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure


def _source() -> MySQLSource:
    config = MySQLConfig(host_port="localhost:3306")
    return MySQLSource(config, PipelineContext(run_id="mysql-source-test"))


def _mock_inspector_returning(rows: list) -> MagicMock:
    inspector = MagicMock()
    conn = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value = rows
    return inspector


def test_get_procedures_for_schema_maps_rows_to_base_procedure() -> None:
    """``get_procedures_for_schema`` maps information_schema.ROUTINES rows to
    ``BaseProcedure``.

    The non-trivial bit is the language mapping: MySQL/MariaDB leave
    ``EXTERNAL_LANGUAGE`` NULL for natively-written SQL procedures (the common
    case), and ``generate_procedure_lineage`` only parses bodies flagged
    ``SQL`` — so a NULL must map to ``SQL`` or every native procedure would be
    silently skipped for lineage. A populated language (MLE procedures) must be
    preserved verbatim.
    """
    native_row = MagicMock()
    native_row.name = "refresh_totals"
    native_row.definition = "BEGIN\n  CALL child_proc();\nEND"
    native_row.language = None  # native SQL procedure -> EXTERNAL_LANGUAGE is NULL

    mle_row = MagicMock()
    mle_row.name = "score_rows"
    mle_row.definition = "function score() {}"
    mle_row.language = "JAVASCRIPT"  # MLE procedure -> explicit language

    procedures = _source().get_procedures_for_schema(
        _mock_inspector_returning([native_row, mle_row]), "my_schema", "my_db"
    )

    assert len(procedures) == 2
    assert all(isinstance(p, BaseProcedure) for p in procedures)

    native = procedures[0]
    assert native.name == "refresh_totals"
    assert native.procedure_definition == "BEGIN\n  CALL child_proc();\nEND"
    assert native.language == "SQL"  # NULL EXTERNAL_LANGUAGE defaulted to SQL

    mle = procedures[1]
    assert mle.name == "score_rows"
    assert mle.language == "JAVASCRIPT"  # explicit language preserved
