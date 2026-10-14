from dataclasses import dataclass, field
from typing import List, Sequence

from datahub.ingestion.source.external_dq.contract import ContractColumn
from datahub.ingestion.source.external_dq.types import TypeProfile


@dataclass(frozen=True)
class PhysicalColumn:
    name: str
    data_type: str
    position: int


@dataclass
class TableValidation:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def validate_table(
    physical: Sequence[PhysicalColumn],
    contract: Sequence[ContractColumn],
    profile: TypeProfile,
    *,
    strict_column_order: bool,
) -> TableValidation:
    result = TableValidation()
    if not physical:
        result.errors.append("table does not exist or has no readable columns")
        return result

    by_name = {c.name.lower(): c for c in physical}
    for column in contract:
        found = by_name.get(column.name)
        if found is None:
            result.errors.append(
                f"missing column '{column.name}' ({column.logical_type.value})"
            )
        elif not profile.accepts(column.logical_type, found.data_type):
            result.errors.append(
                f"column '{column.name}' has type '{found.data_type}', which "
                f"{profile.name} cannot read as {column.logical_type.value}"
            )
    if result.errors:
        return result

    # The reader selects columns by name, so order never affects correctness; it is
    # checked so producers relying on positional INSERTs notice drift.
    contract_names = [c.name for c in contract]
    contract_set = set(contract_names)
    ordered = [c.name.lower() for c in sorted(physical, key=lambda c: c.position)]
    order_issues: List[str] = []
    if [n for n in ordered if n in contract_set] != contract_names:
        order_issues.append(
            "contract columns are not in contract order: " + ", ".join(contract_names)
        )
    if any(n not in contract_set for n in ordered[: len(contract_names)]):
        order_issues.append("extension columns must come after all contract columns")
    (result.errors if strict_column_order else result.warnings).extend(order_issues)
    return result
