import re


def gen_dataset_name(path: str, sheet_name: str, lower_case: bool) -> str:
    sheet_name = re.sub(r"\s+", "_", sheet_name)
    name = f"{path}:{sheet_name}"
    if lower_case:
        name = name.lower()
    return name
