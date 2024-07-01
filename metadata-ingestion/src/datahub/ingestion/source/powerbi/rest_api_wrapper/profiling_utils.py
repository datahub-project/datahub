import re
from typing import Dict, List, Optional


def get_column_name(table_and_col: str) -> Optional[str]:
    regex = re.compile(".*\\[(.*)\\]$")
    m = regex.match(table_and_col)
    if m:
        return m.group(1)
    return None


def process_sample_result(result_data: dict) -> dict:
    sample_data_by_column: Dict[str, List[str]] = {}
    rows = result_data["results"][0]["tables"][0]["rows"]
    for sample in rows:
        for key, value in sample.items():
            if not value:
                continue
            column_name = get_column_name(key)

            if not column_name:
                continue

            if column_name not in sample_data_by_column:
                sample_data_by_column[column_name] = []
            sample_data_by_column[column_name].append(str(value))
    return sample_data_by_column


def process_column_result(result_data: dict) -> dict:
    sample_data_by_column: Dict[str, str] = {}
    rows = result_data["results"][0]["tables"][0]["rows"]
    for sample in rows:
        for key, value in sample.items():
            if not value:
                continue
            column_name = get_column_name(key)

            if not column_name:
                continue

            if column_name != "unique_count":
                value = str(value)
            sample_data_by_column[column_name] = value
    return sample_data_by_column
