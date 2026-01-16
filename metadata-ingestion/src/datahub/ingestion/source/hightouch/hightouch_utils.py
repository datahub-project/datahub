def normalize_column_name(name: str) -> str:
    return name.lower().replace("_", "").replace("-", "")
