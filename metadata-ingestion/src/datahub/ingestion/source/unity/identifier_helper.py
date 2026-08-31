from typing import List, Optional


def split_databricks_identifier(raw: str) -> Optional[List[str]]:
    """Split a Databricks qualified identifier on unquoted dots.

    Handles backtick-quoted parts that contain literal dots
    (e.g. ``catalog.`schema.with.dots`.table`` → ["catalog", "schema.with.dots", "table"])
    and double-quoted parts (e.g. ``"my.catalog".schema.table``).
    Returns None if backticks are unbalanced.
    """
    parts: List[str] = []
    buf: List[str] = []
    in_tick = False
    for ch in raw:
        if ch == "`":
            in_tick = not in_tick
            continue
        if ch == "." and not in_tick:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if in_tick:
        return None
    parts.append("".join(buf))
    cleaned: List[str] = []
    for p in parts:
        p = p.strip()
        # Only strip surrounding "..." so a quote inside a backtick-quoted part survives.
        if len(p) >= 2 and p.startswith('"') and p.endswith('"'):
            p = p[1:-1]
        cleaned.append(p)
    return cleaned
