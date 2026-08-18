from typing import List, Tuple, Type
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig, DorisSource
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


def _inspector_returning(rows: List[Tuple[str, str, int]]) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value = rows
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    return inspector


@pytest.mark.parametrize(
    "source_cls,config_cls,host_port",
    [
        (MySQLSource, MySQLConfig, "localhost:3306"),
        # Doris inherits add_profile_metadata, so an override there has to keep
        # reading positionally too.
        (DorisSource, DorisConfig, "localhost:9030"),
    ],
)
def test_add_profile_metadata_reads_storage_bytes_positionally(
    source_cls: Type[MySQLSource],
    config_cls: Type[MySQLConfig],
    host_port: str,
) -> None:
    # Tuple rows (no named attributes) prove access is positional, not by the
    # label whose case differs across MySQL/MariaDB/Doris/TiDB.
    source = source_cls(
        config_cls(host_port=host_port, profiling={"enabled": True}),
        PipelineContext(run_id="mysql-family-profiling-test"),
    )
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
