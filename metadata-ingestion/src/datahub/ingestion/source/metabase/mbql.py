from dataclasses import dataclass, field
from typing import List

from datahub.ingestion.source.metabase.constants import _MBQL_REF_FIELD


@dataclass
class MBQLFieldRefs:
    """Field references extracted from an MBQL clause.

    ``ids`` holds numeric (id-based) refs we can resolve to columns; ``named``
    holds string (name-based) refs, which the source cannot resolve to an
    upstream column and therefore drops from column-level lineage.
    """

    ids: List[int] = field(default_factory=list)
    named: List[str] = field(default_factory=list)

    def extend(self, other: "MBQLFieldRefs") -> None:
        self.ids.extend(other.ids)
        self.named.extend(other.named)


def extract_mbql_field_refs(clause: object) -> MBQLFieldRefs:
    """
    Recursively extract Metabase field references from an MBQL clause.

    MBQL field refs look like ``["field", 100, null]`` (id-based) or
    ``["field", "name", {...}]`` (name-based). Only id-based refs can be
    resolved to a concrete upstream column; name-based refs are collected
    separately so the caller can report them rather than silently dropping
    column-level lineage.
    """
    refs = MBQLFieldRefs()
    if not isinstance(clause, list) or not clause:
        return refs
    if clause[0] == _MBQL_REF_FIELD and len(clause) >= 2:
        ref = clause[1]
        if isinstance(ref, int):
            refs.ids.append(ref)
        elif isinstance(ref, str):
            refs.named.append(ref)
    else:
        for item in clause:
            if isinstance(item, list):
                refs.extend(extract_mbql_field_refs(item))
    return refs


def _extract_field_ids_from_mbql(clause: object) -> List[int]:
    """Return only the id-based field refs from an MBQL clause."""
    return extract_mbql_field_refs(clause).ids
