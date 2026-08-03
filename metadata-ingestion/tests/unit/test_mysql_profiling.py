from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


def _source() -> MySQLSource:
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True},
    )
    return MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))


def _inspector_returning(rows: list) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value = rows
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    return inspector


def test_add_profile_metadata_reads_storage_bytes_positionally() -> None:
    source = _source()
    inspector = _inspector_returning(
        [
            ("my_db", "orders", 4096),
            ("my_db", "customers", 8192),
        ]
    )

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {
        "my_db.orders": 4096,
        "my_db.customers": 8192,
    }
