from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional


@dataclass(frozen=True)
class RunListing:
    """The names a `probe run` listed, ready for `probe filter` to judge."""

    kind: Optional[str]
    parent_path: List[str]
    names: List[str]
    # Aligned with names. Only scalar fields travel: a verdict compares
    # strings, and a nested value has no single string to match.
    attributes: List[Dict[str, str]]


def _as_attribute(value: object) -> Optional[str]:
    # bool before int: bool is an int subclass, and str(True) is "True"
    # where the JSON the caller read said `true`.
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    return None


def listing_from_run(envelope: Mapping[str, object]) -> RunListing:
    """Read a `probe run` result envelope as a listing to judge.

    Refuses rather than guesses: a result that is not a list (`sql` returns
    rows in an envelope of its own) holds no names, and judging its keys
    would answer a question nobody asked.
    """
    result = envelope.get("result")
    if not isinstance(result, list):
        raise ValueError(
            "that `probe run` output is not a listing: its `result` is not a "
            "list of names, so there is nothing to judge. Pass --name instead."
        )
    names: List[str] = []
    attributes: List[Dict[str, str]] = []
    for index, item in enumerate(result):
        if isinstance(item, str):
            names.append(item)
            attributes.append({})
            continue
        name = item.get("name") if isinstance(item, dict) else None
        if not isinstance(name, str) or not isinstance(item, dict):
            # The index, not the item: the item is caller data and may be large.
            raise ValueError(
                f"entry {index} of the listing has no string `name`, so it "
                f"cannot be judged"
            )
        names.append(name)
        attributes.append(
            {
                key: text
                for key, value in item.items()
                if key != "name" and (text := _as_attribute(value)) is not None
            }
        )
    kind = envelope.get("kind")
    parent = envelope.get("parent_path")
    return RunListing(
        kind=kind if isinstance(kind, str) and kind else None,
        parent_path=[str(p) for p in parent] if isinstance(parent, list) else [],
        names=names,
        attributes=attributes,
    )
