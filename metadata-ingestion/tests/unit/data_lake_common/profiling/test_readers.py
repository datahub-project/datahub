import io
from datetime import datetime, timezone

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

from datahub.ingestion.source.data_lake_common.profiling.accumulators import ColumnKind
from datahub.ingestion.source.data_lake_common.profiling.readers import (
    read_avro,
    read_csv,
    read_json,
    read_parquet,
)

AVRO_SCHEMA = {
    "type": "record",
    "name": "Test",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": ["null", "double"], "default": None},
        {"name": "name", "type": "string"},
        {
            "name": "created",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
    ],
}


def test_read_parquet_exposes_schema_and_streams_batches() -> None:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"]),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    source = read_parquet(buf)

    assert source.columns == ["id", "name"]
    assert source.column_kinds == {"id": ColumnKind.NUMERIC, "name": ColumnKind.STRING}
    assert sum(batch.num_rows for batch in source.batches) == 3


def test_read_csv_exposes_schema_and_streams_batches() -> None:
    csv_bytes = b"id,name\n1,a\n2,b\n3,c\n"

    source = read_csv(io.BytesIO(csv_bytes))

    assert source.columns == ["id", "name"]
    assert source.column_kinds["id"] == ColumnKind.NUMERIC
    assert sum(batch.num_rows for batch in source.batches) == 3


def test_read_csv_detects_date_columns_as_temporal() -> None:
    csv_bytes = b"id,event_date\n1,2023-01-01\n2,2023-01-02\n"

    source = read_csv(io.BytesIO(csv_bytes))

    assert source.column_kinds["event_date"] == ColumnKind.TEMPORAL


def test_read_csv_respects_custom_delimiter() -> None:
    tsv_bytes = b"id\tname\n1\ta\n2\tb\n"

    source = read_csv(io.BytesIO(tsv_bytes), delimiter="\t")

    assert source.columns == ["id", "name"]
    assert sum(batch.num_rows for batch in source.batches) == 2


def test_read_json_exposes_schema_and_streams_batches() -> None:
    json_bytes = b'{"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n'

    source = read_json(io.BytesIO(json_bytes))

    assert source.columns == ["id", "name"]
    assert source.column_kinds["id"] == ColumnKind.NUMERIC
    assert sum(batch.num_rows for batch in source.batches) == 2


def test_read_avro_classifies_columns_including_unions_and_logical_types() -> None:
    buf = io.BytesIO()
    fastavro.writer(
        buf,
        AVRO_SCHEMA,
        [
            {"id": 1, "amount": 9.5, "name": "a", "created": 1700000000000},
            {"id": 2, "amount": None, "name": "b", "created": 1700000001000},
        ],
    )
    buf.seek(0)

    source = read_avro(buf)

    assert source.columns == ["id", "amount", "name", "created"]
    assert source.column_kinds == {
        "id": ColumnKind.NUMERIC,
        "amount": ColumnKind.NUMERIC,
        "name": ColumnKind.STRING,
        "created": ColumnKind.TEMPORAL,
    }
    assert list(source.rows) == [
        {
            "id": 1,
            "amount": 9.5,
            "name": "a",
            "created": datetime.fromtimestamp(1700000000, tz=timezone.utc),
        },
        {
            "id": 2,
            "amount": None,
            "name": "b",
            "created": datetime.fromtimestamp(1700000001, tz=timezone.utc),
        },
    ]


def test_read_csv_reflows_ragged_rows_with_trailing_delimiter() -> None:
    # Every data row has a stray trailing comma (an extra empty field). Spark
    # silently drops it; pyarrow raises, so read_csv must reflow and recover.
    csv_bytes = b"id,name\n1,a,\n2,b,\n3,c,\n"

    source = read_csv(io.BytesIO(csv_bytes))

    assert source.columns == ["id", "name"]
    assert sum(batch.num_rows for batch in source.batches) == 3


def test_avro_field_kinds_cover_other_and_nested_types() -> None:
    schema = {
        "type": "record",
        "name": "T",
        "fields": [
            {"name": "flag", "type": "boolean"},  # -> OTHER
            {
                "name": "raw",
                "type": ["null", "bytes"],
            },  # union, all non-numeric -> OTHER
            {
                "name": "price",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 9,
                    "scale": 2,
                },
            },  # decimal logical type -> NUMERIC
            {
                "name": "nested",
                "type": {"type": "record", "name": "Inner", "fields": []},
            },  # nested record dict, no logicalType -> OTHER
        ],
    }
    from decimal import Decimal

    buf = io.BytesIO()
    fastavro.writer(
        buf,
        schema,
        [{"flag": True, "raw": None, "price": Decimal("1.50"), "nested": {}}],
    )
    buf.seek(0)

    source = read_avro(buf)

    assert source.column_kinds == {
        "flag": ColumnKind.OTHER,
        "raw": ColumnKind.OTHER,
        "price": ColumnKind.NUMERIC,
        "nested": ColumnKind.OTHER,
    }
