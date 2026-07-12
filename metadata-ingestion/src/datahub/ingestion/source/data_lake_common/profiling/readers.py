import csv as csv_module
import io
from dataclasses import dataclass
from typing import IO, Any, Dict, Iterable, List, Union, cast

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.parquet as pa_parquet
from fastavro import reader as avro_reader

from datahub.ingestion.source.data_lake_common.profiling.accumulators import (
    ColumnKind,
    classify_arrow_type,
)

# Matches the previous Spark-based profiler's read batch semantics: stream the
# file rather than loading it fully into memory.
PARQUET_BATCH_SIZE = 10_000

AVRO_NUMERIC_PRIMITIVES = {"int", "long", "float", "double"}
AVRO_TEMPORAL_LOGICAL_TYPES = {
    "date",
    "timestamp-millis",
    "timestamp-micros",
    "local-timestamp-millis",
    "local-timestamp-micros",
}

AvroFieldType = Union[str, Dict[str, Any], List[Any]]


@dataclass
class ColumnarSource:
    """A parquet/CSV file, exposed as a stream of Arrow record batches."""

    columns: List[str]
    column_kinds: Dict[str, ColumnKind]
    batches: Iterable[pa.RecordBatch]


@dataclass
class AvroSource:
    """An Avro file, exposed row-at-a-time (fastavro has no batch/vectorized API)."""

    columns: List[str]
    column_kinds: Dict[str, ColumnKind]
    rows: Iterable[Dict[str, Any]]


def _column_kinds_from_schema(schema: pa.Schema) -> Dict[str, ColumnKind]:
    return {field.name: classify_arrow_type(field.type) for field in schema}


def read_parquet(file_obj: IO[bytes]) -> ColumnarSource:
    parquet_file = pa_parquet.ParquetFile(file_obj)
    schema = parquet_file.schema_arrow
    return ColumnarSource(
        columns=schema.names,
        column_kinds=_column_kinds_from_schema(schema),
        batches=parquet_file.iter_batches(batch_size=PARQUET_BATCH_SIZE),
    )


def _reflow_ragged_rows(file_obj: IO[bytes], delimiter: str) -> IO[bytes]:
    """Truncate/pad rows to the header's width.

    Spark's CSV reader silently drops unnamed trailing fields (e.g. a stray
    trailing delimiter on every data row); pyarrow's parser raises instead.
    Re-serializing with consistent row width lets pyarrow parse (and still
    type-infer) the rest of the file normally.
    """
    file_obj.seek(0)
    text = io.TextIOWrapper(file_obj, encoding="utf-8", newline="")
    rows = list(csv_module.reader(text, delimiter=delimiter))
    if not rows:
        raise ValueError("empty CSV")
    width = len(rows[0])

    out = io.StringIO()
    writer = csv_module.writer(out, delimiter=delimiter)
    for row in rows:
        if len(row) > width:
            row = row[:width]
        elif len(row) < width:
            row = row + [""] * (width - len(row))
        writer.writerow(row)
    return io.BytesIO(out.getvalue().encode("utf-8"))


def read_csv(file_obj: IO[bytes], delimiter: str = ",") -> ColumnarSource:
    parse_options = pa_csv.ParseOptions(delimiter=delimiter)
    convert_options = pa_csv.ConvertOptions(strings_can_be_null=True)
    try:
        reader = pa_csv.open_csv(
            file_obj, parse_options=parse_options, convert_options=convert_options
        )
    except pa.ArrowInvalid:
        reader = pa_csv.open_csv(
            _reflow_ragged_rows(file_obj, delimiter),
            parse_options=parse_options,
            convert_options=convert_options,
        )
    return ColumnarSource(
        columns=reader.schema.names,
        column_kinds=_column_kinds_from_schema(reader.schema),
        batches=reader,
    )


def read_json(file_obj: IO[bytes]) -> ColumnarSource:
    # pyarrow.json has no streaming/batch API like parquet/csv (unlike Spark's
    # read.json(), which is also a full parse under the hood); lake JSON
    # files are typically small enough for this to be fine.
    table = pa_json.read_json(file_obj)
    return ColumnarSource(
        columns=table.schema.names,
        column_kinds=_column_kinds_from_schema(table.schema),
        batches=table.to_batches(),
    )


def _avro_field_kind(field_type: AvroFieldType) -> ColumnKind:
    if isinstance(field_type, str):
        if field_type in AVRO_NUMERIC_PRIMITIVES:
            return ColumnKind.NUMERIC
        if field_type == "string":
            return ColumnKind.STRING
        return ColumnKind.OTHER
    if isinstance(field_type, list):
        for member in field_type:
            if member == "null":
                continue
            kind = _avro_field_kind(member)
            if kind != ColumnKind.OTHER:
                return kind
        return ColumnKind.OTHER
    if isinstance(field_type, dict):
        logical_type = field_type.get("logicalType")
        if logical_type == "decimal":
            return ColumnKind.NUMERIC
        if logical_type in AVRO_TEMPORAL_LOGICAL_TYPES:
            return ColumnKind.TEMPORAL
        return _avro_field_kind(field_type.get("type", ""))
    # Avro field types are always str/list/dict.
    return ColumnKind.OTHER  # pragma: no cover


def read_avro(file_obj: IO[bytes]) -> AvroSource:
    rows = avro_reader(file_obj)
    # Data files always have a "record" schema (a dict), never a bare
    # primitive/union at the top level.
    assert isinstance(rows.writer_schema, dict)
    fields = rows.writer_schema.get("fields", [])
    return AvroSource(
        columns=[field["name"] for field in fields],
        column_kinds={
            field["name"]: _avro_field_kind(field["type"]) for field in fields
        },
        # fastavro's reader is typed to yield any Avro value; a "record"
        # schema guarantees dicts at runtime.
        rows=cast(Iterable[Dict[str, Any]], rows),
    )
