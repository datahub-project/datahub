import io
import logging
from typing import List, Optional

import openpyxl

from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.ingestion.source.sharepoint.sharepoint_client import SharePointItem
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)

# Extensions for which schema inference is supported. Other extensions (e.g. pdf)
# are still tracked as Dataset entities but no download or inference is attempted.
SCHEMA_INFERRABLE_EXTENSIONS = frozenset(
    ["csv", "tsv", "json", "jsonl", "parquet", "avro", "xlsx"]
)

# openpyxl data_type codes: "n" numeric, "s" string, "b" boolean, "d" date,
# "f" formula (treated as string), None → null.
_EXCEL_TYPE_MAP = {
    "n": NumberTypeClass,
    "s": StringTypeClass,
    "b": BooleanTypeClass,
    "d": DateTypeClass,
    "f": StringTypeClass,
    None: NullTypeClass,
}


def infer_schema(
    item: SharePointItem, file_bytes: bytes, max_rows: int
) -> Optional[List[SchemaFieldClass]]:
    """Infer schema from the content of a SharePoint file.

    Returns a list of SchemaFieldClass instances, or None if the extension is
    not supported or inference fails.
    """
    ext = item.extension
    file_obj = io.BytesIO(file_bytes)

    try:
        if ext == "parquet":
            return parquet.ParquetInferrer().infer_schema(file_obj)
        if ext == "csv":
            return csv_tsv.CsvInferrer(max_rows=max_rows).infer_schema(file_obj)
        if ext == "tsv":
            return csv_tsv.TsvInferrer(max_rows=max_rows).infer_schema(file_obj)
        if ext == "json":
            return json.JsonInferrer().infer_schema(file_obj)
        if ext == "jsonl":
            return json.JsonInferrer(max_rows=max_rows, format="jsonl").infer_schema(
                file_obj
            )
        if ext == "avro":
            return avro.AvroInferrer().infer_schema(file_obj)
        if ext == "xlsx":
            return _infer_excel_schema(file_bytes, max_rows)
    except Exception as exc:
        logger.warning(
            f"Schema inference failed for '{item.name}' ({ext}): {exc}",
            exc_info=True,
        )
        return None

    logger.debug(f"No inferrer available for extension '{ext}'; skipping schema.")
    return None


def _infer_excel_schema(
    file_bytes: bytes, max_rows: int
) -> Optional[List[SchemaFieldClass]]:
    """Infer schema from the first sheet of an Excel (.xlsx) workbook.

    Reads the header row for column names and samples up to *max_rows* data
    rows to determine the dominant data type for each column.
    """
    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(file_bytes),
            read_only=True,
            data_only=True,
        )
    except Exception as exc:
        logger.warning(f"Failed to open Excel workbook: {exc}")
        return None

    try:
        ws = wb.active
        if ws is None:
            logger.warning("Excel workbook has no active sheet.")
            return None

        rows = list(ws.iter_rows(max_row=max_rows + 1, values_only=False))
        if not rows:
            return []

        header_row = rows[0]
        column_names: List[str] = []
        for cell in header_row:
            value = cell.value
            col_name = str(value) if value is not None else f"column_{cell.column}"
            column_names.append(col_name)

        if not column_names:
            return []

        num_cols = len(column_names)
        type_votes: List[dict] = [{} for _ in range(num_cols)]
        for row in rows[1 : max_rows + 1]:
            for col_idx, cell in enumerate(row):
                if col_idx >= num_cols:
                    break
                dtype = cell.data_type
                type_votes[col_idx][dtype] = type_votes[col_idx].get(dtype, 0) + 1

        fields: List[SchemaFieldClass] = []
        for col_idx, col_name in enumerate(column_names):
            votes = type_votes[col_idx]
            best_type = None
            best_count = 0
            for dtype, count in votes.items():
                if dtype is None:
                    continue
                if count > best_count:
                    best_count = count
                    best_type = dtype

            type_class = _EXCEL_TYPE_MAP.get(best_type, StringTypeClass)
            fields.append(
                SchemaFieldClass(
                    fieldPath=col_name,
                    nativeDataType=_native_type_name(best_type),
                    type=SchemaFieldDataTypeClass(type=type_class()),  # type: ignore[arg-type]
                    nullable=True,
                    recursive=False,
                )
            )

        return fields
    finally:
        wb.close()


def _native_type_name(openpyxl_type: Optional[str]) -> str:
    mapping = {
        "n": "number",
        "s": "string",
        "b": "boolean",
        "d": "date",
        "f": "formula",
    }
    return mapping.get(openpyxl_type or "", "string")
