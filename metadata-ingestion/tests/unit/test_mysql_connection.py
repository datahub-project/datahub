from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


def _source(**overrides: object) -> MySQLSource:
    config = MySQLConfig(host_port="localhost:3306", **overrides)
    return MySQLSource(config, PipelineContext(run_id="mysql-conn-test"))


@patch("datahub.ingestion.source.sql.mysql.inspect")
@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_get_inspectors_disposes_every_engine(
    mock_create_engine: MagicMock, mock_inspect: MagicMock
) -> None:
    # Undisposed per-database engines leak one pooled connection each, exhausting
    # servers with a low max_user_connections limit. Every engine must be disposed.
    engines: list = []

    def _make_engine(*_args: object, **_kwargs: object) -> MagicMock:
        engine = MagicMock()
        engines.append(engine)
        return engine

    mock_create_engine.side_effect = _make_engine

    inspector = MagicMock()
    inspector.get_schema_names.return_value = ["db_one", "db_two"]
    mock_inspect.return_value = inspector

    source = _source()  # database unset -> enumerate all visible databases

    inspectors = list(source.get_inspectors())

    assert len(inspectors) == 2
    assert len(engines) == 3  # one discovery engine + one per database
    for engine in engines:
        engine.dispose.assert_called_once()
