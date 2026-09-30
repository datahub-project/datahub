from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional


@dataclass(frozen=True)
class RunListing:
    """The names a `probe run` listed, ready for `probe filter` to judge."""

    kind: Optional[str]
    # The source that produced the listing, when the envelope says, so a
    # listing from one source is not judged against another's recipe.
    source_type: Optional[str]
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


def listing_from_run(envelope: object) -> RunListing:
    """Read a `probe run` result envelope as a listing to judge.

    Refuses rather than guesses: a result that is not a list (`sql` returns
    rows in an envelope of its own) holds no names, and judging its keys
    would answer a question nobody asked.
    """
    if not isinstance(envelope, Mapping):
        # A file of the wrong shape (a bare list of names, say) is a bad
        # argument, not a crash in the reader.
        raise ValueError(
            "that file is not a `probe run` output: it holds "
            f"{type(envelope).__name__}, not a result envelope. Pass the JSON "
            "`probe run` printed, or --name instead."
        )
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
    source_type = envelope.get("source_type")
    return RunListing(
        kind=kind if isinstance(kind, str) and kind else None,
        source_type=source_type
        if isinstance(source_type, str) and source_type
        else None,
        parent_path=[str(p) for p in parent] if isinstance(parent, list) else [],
        names=names,
        attributes=attributes,
    )
