from typing import Dict, List

from sqlalchemy import create_engine, inspect, text

from datahub.ingestion.source.sql.sqlalchemy_data_reader import (
    SqlAlchemyTableDataReader,
)


def _engine_with_rows(rows: int):  # type: ignore[no-untyped-def]
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE people (id INTEGER, name TEXT)"))
        for i in range(rows):
            conn.execute(text(f"INSERT INTO people VALUES ({i}, 'name{i}')"))
    return engine


def test_sample_is_returned_per_column_and_bounded_by_sample_size() -> None:
    engine = _engine_with_rows(5)
    with engine.connect() as conn:
        reader = SqlAlchemyTableDataReader.create(inspect(conn))
        with reader:
            sample: Dict[str, List[object]] = reader.get_sample_data_for_table(
                ["main", "people"], sample_size=3
            )

    assert set(sample) == {"id", "name"}
    assert sample["id"] == [0, 1, 2]
    assert sample["name"] == ["name0", "name1", "name2"]


def test_unqualified_table_id_reads_the_default_schema() -> None:
    engine = _engine_with_rows(1)
    with SqlAlchemyTableDataReader(engine) as reader:
        assert reader.get_sample_data_for_table(["people"], sample_size=10) == {
            "id": [0],
            "name": ["name0"],
        }
