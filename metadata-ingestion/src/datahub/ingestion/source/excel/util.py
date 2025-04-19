import re


def gen_dataset_name(filename: str, sheet_name: str, lower_case: bool) -> str:
    sheet_name = re.sub(r"\s+", "_", sheet_name)
    if lower_case:
        sheet_name = sheet_name.lower()
        filename = filename.lower()
    return f"{filename}.{sheet_name}"
