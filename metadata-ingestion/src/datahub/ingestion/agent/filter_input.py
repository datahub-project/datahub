from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional

from datahub.ingestion.agent.redact import MASK


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
    # Entries left out because `probe run` redacted their name: a secret that
    # equals an identifier masks it, and "***" is not a name the source has.
    # Indexes, not names, since the name is exactly what is unreadable.
    skipped: List[int] = field(default_factory=list)
    # Attribute keys dropped from at least one entry for the same reason: a
    # verdict matching an id against "***" is a verdict about nothing.
    masked_attributes: List[str] = field(default_factory=list)
    # A parent_path segment was redacted. Unlike a masked name this taints
    # every verdict -- each is qualified by the parent -- so the CLI refuses
    # the listing's parent rather than judging against "***", unless the
    # caller passes --parent and the listing's is never used.
    parent_redacted: bool = False
    # The run stopped at its limit, so names beyond it were never listed.
    truncated: bool = False
    # The run recorded failures, so part of the source was not listed at all.
    incomplete: bool = False


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
    skipped: List[int] = []
    masked_keys: List[str] = []
    for index, item in enumerate(result):
        if isinstance(item, str):
            name: object = item
            fields: Mapping[str, object] = {}
        elif isinstance(item, dict):
            name = item.get("name")
            fields = item
        else:
            name = None
            fields = {}
        if not isinstance(name, str):
            # The index, not the item: the item is caller data and may be large.
            raise ValueError(
                f"entry {index} of the listing has no string `name`, so it "
                f"cannot be judged"
            )
        # `in`, not `==`: redact.py masks a secret as "***" but the registry
        # masker writes "***REDACTED:NAME***", and a secret embedded in a
        # longer identifier is masked as a substring.
        if MASK in name:
            skipped.append(index)
            continue
        kept: Dict[str, str] = {}
        for key, value in fields.items():
            text = _as_attribute(value) if key != "name" else None
            if text is None:
                continue
            if MASK in text:
                if key not in masked_keys:
                    masked_keys.append(key)
                continue
            kept[key] = text
        names.append(name)
        attributes.append(kept)
    kind = envelope.get("kind")
    source_type = envelope.get("source_type")
    failures = envelope.get("failures")
    parent_path = _parent_path(envelope.get("parent_path"))
    return RunListing(
        kind=kind if isinstance(kind, str) and kind else None,
        source_type=source_type
        if isinstance(source_type, str) and source_type
        else None,
        parent_path=parent_path,
        names=names,
        attributes=attributes,
        skipped=skipped,
        masked_attributes=masked_keys,
        parent_redacted=any(MASK in segment for segment in parent_path),
        truncated=envelope.get("truncated") is True,
        incomplete=isinstance(failures, list) and len(failures) > 0,
    )


def _parent_path(parent: object) -> List[str]:
    if parent is None:
        return []
    # Refused rather than read as no parent: a bare string used to become []
    # and judge the names as top-level, and str() over a nested entry judged
    # a repr no source has.
    if not isinstance(parent, list) or not all(isinstance(p, str) for p in parent):
        raise ValueError("not a `probe run` listing: its parent_path is malformed")
    return list(parent)
