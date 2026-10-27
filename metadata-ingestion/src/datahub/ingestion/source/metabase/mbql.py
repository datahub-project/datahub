from dataclasses import dataclass, field
from typing import List, Optional

from datahub.ingestion.source.metabase.constants import _MBQL_REF_FIELD


@dataclass
class MBQLFieldRefs:
    # ids: numeric refs resolvable to columns. named: string refs the source
    # cannot resolve to an upstream column, so they are dropped from CLL.
    ids: List[int] = field(default_factory=list)
    named: List[str] = field(default_factory=list)

    def extend(self, other: "MBQLFieldRefs") -> None:
        self.ids.extend(other.ids)
        self.named.extend(other.named)


def extract_mbql_field_refs(clause: object) -> MBQLFieldRefs:
    # MBQL field refs are ["field", 100, null] (id-based) or ["field", "name", {...}]
    # (name-based); recurse into nested clauses to collect both kinds.
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


def name_based_field_name(field_ref: object) -> Optional[str]:
    # Only a top-level name-based field ref (["field", "col", ...]) names a real
    # source column; aggregation/expression outputs return None so callers don't
    # invent lineage like film.count.
    if (
        isinstance(field_ref, list)
        and len(field_ref) >= 2
        and field_ref[0] == _MBQL_REF_FIELD
        and isinstance(field_ref[1], str)
    ):
        return field_ref[1]
    return None
