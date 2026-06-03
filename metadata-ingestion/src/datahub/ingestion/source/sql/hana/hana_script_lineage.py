import re
from typing import List

from datahub.ingestion.source.sql.hana.constants import HANA_PSEUDO_TABLES
from datahub.ingestion.source.sql.hana.models import ScriptTableRef

# Matches ``FROM "schema"."name"`` / ``JOIN "schema"."name"``. HANA SQLScript
# always quotes schema-qualified references, so we ignore unquoted forms to
# avoid false positives on SQLScript-local table variables (``T_FREQ``) and
# column references.
_QUALIFIED_TABLE_RE = re.compile(
    r'\b(?:FROM|JOIN)\s+"([^"]+)"\."([^"]+)"',
    re.IGNORECASE,
)


def extract_table_references(sql: str) -> List[ScriptTableRef]:
    """Return the de-duplicated set of schema-qualified table references in ``sql``."""
    if not sql:
        return []

    seen: set[tuple[str, str]] = set()
    refs: List[ScriptTableRef] = []
    for match in _QUALIFIED_TABLE_RE.finditer(sql):
        schema, name = match.group(1), match.group(2)
        if not schema or not name:
            continue
        if name.upper() in HANA_PSEUDO_TABLES:
            continue
        key = (schema, name)
        if key in seen:
            continue
        seen.add(key)
        refs.append(ScriptTableRef(schema_name=schema, name=name))
    return refs
