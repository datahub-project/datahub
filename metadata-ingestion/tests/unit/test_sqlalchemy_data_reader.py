from typing import Dict, List

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from datahub.ingestion.source.sql.sqlalchemy_data_reader import (
    SqlAlchemyTableDataReader,
)


@pytest.fixture
def engine(sqlite_engine: Engine) -> Engine:
    """A ``people`` table with five rows, ``(i, 'name<i>')``."""
    with sqlite_engine.begin() as conn:
        conn.execute(text("CREATE TABLE people (id INTEGER, name TEXT)"))
        for i in range(5):
            conn.execute(text(f"INSERT INTO people VALUES ({i}, 'name{i}')"))
    return sqlite_engine


def test_sample_is_returned_per_column_and_bounded_by_sample_size(
    engine: Engine,
) -> None:
    with engine.connect() as conn:
        reader = SqlAlchemyTableDataReader.create(inspect(conn))
        with reader:
            sample: Dict[str, List[object]] = reader.get_sample_data_for_table(
                ["main", "people"], sample_size=3
            )

    assert set(sample) == {"id", "name"}
    assert sample["id"] == [0, 1, 2]
    assert sample["name"] == ["name0", "name1", "name2"]


def test_unqualified_table_id_reads_the_default_schema(engine: Engine) -> None:
    with SqlAlchemyTableDataReader(engine) as reader:
        assert reader.get_sample_data_for_table(["people"], sample_size=1) == {
            "id": [0],
            "name": ["name0"],
        }
