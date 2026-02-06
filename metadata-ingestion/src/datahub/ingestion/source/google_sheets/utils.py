from typing import Any, List

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.google_sheets.config import GoogleSheetsSourceConfig
from datahub.ingestion.source.google_sheets.constants import (
    ARRAY_DETECTION_SAMPLE_SIZE,
    BOOLEAN_VALUES,
    DATE_CONFIDENCE_THRESHOLD,
    DATE_PATTERNS,
    PURE_NUMBER_PATTERN,
    NativeDataType,
)
from datahub.ingestion.source.google_sheets.models import (
    DriveFile,
    DriveLabelInfo,
    InferredType,
    NamedRangeInfo,
    Spreadsheet,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TagAssociationClass,
)


def extract_tags_from_drive_metadata(
    sheet: DriveFile, config: GoogleSheetsSourceConfig
) -> List[TagAssociationClass]:
    """Extract tags from Google Drive file labels and custom properties.

    Parses the labelInfo field from Drive API v3 to extract applied labels.
    See: https://developers.google.com/drive/api/v3/reference/files#labelInfo

    Args:
        sheet: Drive file metadata (Pydantic model)
        config: Source configuration

    Returns:
        List of tag associations from Drive labels
    """
    if not config.extract_tags:
        return []

    tags: List[TagAssociationClass] = []

    if not sheet.labelInfo:
        return tags

    try:
        label_info = DriveLabelInfo.from_dict(sheet.labelInfo)
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(
            f"Failed to parse Drive labelInfo: {e}. Skipping label extraction."
        )
        return tags

    for label in label_info.labels:
        if label.id:
            tags.append(
                TagAssociationClass(tag=builder.make_tag_urn(f"drive-label:{label.id}"))
            )

        for field_value in label.fields.values():
            for text in field_value.text:
                if text:
                    tags.append(
                        TagAssociationClass(
                            tag=builder.make_tag_urn(f"drive-label-field:{text}")
                        )
                    )

            for selection in field_value.selection:
                if selection:
                    tags.append(
                        TagAssociationClass(
                            tag=builder.make_tag_urn(
                                f"drive-label-selection:{selection}"
                            )
                        )
                    )

    return tags


def _format_grid_range_to_a1(grid_range: Any, sheets: List[Any]) -> str:
    """Convert a GridRange to A1 notation.

    Args:
        grid_range: GridRange object with sheetId, row/column indices
        sheets: List of sheets to lookup sheet name

    Returns:
        A1 notation string like "Sheet1!A1:B10" or "Sheet1" if full sheet
    """
    # Find sheet name by sheetId
    sheet_name = None
    for sheet in sheets:
        if (
            hasattr(sheet, "properties")
            and sheet.properties.sheetId == grid_range.sheetId
        ):
            sheet_name = sheet.properties.title
            break

    if not sheet_name:
        sheet_name = f"Sheet{grid_range.sheetId}"

    # If no specific range, return sheet name only
    if (
        grid_range.startRowIndex == 0
        and grid_range.endRowIndex == 0
        and grid_range.startColumnIndex == 0
        and grid_range.endColumnIndex == 0
    ):
        return sheet_name

    # Convert column indices to letters
    def col_to_letter(col_idx: int) -> str:
        result = ""
        while col_idx >= 0:
            result = chr(ord("A") + (col_idx % 26)) + result
            col_idx = col_idx // 26 - 1
        return result

    start_col = col_to_letter(grid_range.startColumnIndex)
    end_col = (
        col_to_letter(grid_range.endColumnIndex - 1)
        if grid_range.endColumnIndex > 0
        else start_col
    )
    start_row = grid_range.startRowIndex + 1  # Convert to 1-based
    end_row = grid_range.endRowIndex if grid_range.endRowIndex > 0 else start_row

    return f"{sheet_name}!{start_col}{start_row}:{end_col}{end_row}"


def get_named_ranges(spreadsheet: Spreadsheet) -> List[NamedRangeInfo]:
    """Extract named ranges from spreadsheet metadata.

    Args:
        spreadsheet: Spreadsheet metadata

    Returns:
        List of named range information
    """
    named_ranges: List[NamedRangeInfo] = []

    if not spreadsheet.namedRanges:
        return named_ranges

    for named_range in spreadsheet.namedRanges:
        if not named_range.name:
            continue

        # Convert GridRange to A1 notation using Pydantic attribute access
        range_notation = _format_grid_range_to_a1(named_range.range, spreadsheet.sheets)

        range_info = NamedRangeInfo(
            name=named_range.name,
            range_notation=range_notation,
        )
        named_ranges.append(range_info)

    return named_ranges


def infer_column_type(data_rows: List[List[Any]], column_index: int) -> InferredType:
    """Infer the data type of a column from sample data.

    Args:
        data_rows: List of data rows
        column_index: Index of the column to analyze

    Returns:
        InferredType with data_type and native_type
    """
    if not data_rows:
        return InferredType(
            data_type=StringTypeClass(), native_type=NativeDataType.STRING.value
        )

    values = []
    for row in data_rows:
        if column_index < len(row):
            value = row[column_index]
            if value and str(value).strip():
                values.append(str(value).strip())

    if not values:
        return InferredType(
            data_type=StringTypeClass(), native_type=NativeDataType.STRING.value
        )

    if all(v.lower() in BOOLEAN_VALUES for v in values):
        return InferredType(
            data_type=BooleanTypeClass(), native_type=NativeDataType.BOOLEAN.value
        )

    if all(is_number(v) for v in values):
        return InferredType(
            data_type=NumberTypeClass(), native_type=NativeDataType.NUMBER.value
        )

    if all(is_date(v) for v in values):
        return InferredType(
            data_type=DateTypeClass(), native_type=NativeDataType.DATE.value
        )

    if any("[" in v or "," in v for v in values[:ARRAY_DETECTION_SAMPLE_SIZE]):
        return InferredType(
            data_type=ArrayTypeClass(nestedType=["string"]),
            native_type=NativeDataType.ARRAY.value,
        )

    return InferredType(
        data_type=StringTypeClass(), native_type=NativeDataType.STRING.value
    )


def is_number(value: str) -> bool:
    """Check if a string value represents a number."""
    if not value:
        return False

    clean_value = value.replace(",", "").replace("$", "").replace("%", "").strip()
    return bool(PURE_NUMBER_PATTERN.match(clean_value))


def is_date(value: str) -> bool:
    """Check if a string value represents a date."""
    if not value or len(value) < 6:
        return False

    matches = sum(1 for pattern in DATE_PATTERNS if pattern.search(value))
    return matches >= DATE_CONFIDENCE_THRESHOLD


def column_letter_to_index(column_letter: str) -> int:
    """Convert Excel-style column letter to zero-based index.

    Args:
        column_letter: Column letter (e.g., "A", "Z", "AA")

    Returns:
        Zero-based column index
    """
    index = 0
    for char in column_letter.upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def index_to_column_letter(index: int) -> str:
    """Convert zero-based column index to Excel-style column letter.

    Args:
        index: Zero-based column index

    Returns:
        Column letter (e.g., "A", "Z", "AA")
    """
    letter = ""
    index += 1
    while index > 0:
        index -= 1
        letter = chr(index % 26 + ord("A")) + letter
        index //= 26
    return letter
