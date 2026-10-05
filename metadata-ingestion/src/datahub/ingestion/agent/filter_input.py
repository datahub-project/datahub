from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, cast

from datahub.ingestion.agent.models import ProbeRunEnvelopeView
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
    # Indexes of entries whose name `probe run` redacted ("***" is no name).
    skipped: List[int] = field(default_factory=list)
    # Attribute keys dropped from at least one entry for the same reason.
    masked_attributes: List[str] = field(default_factory=list)
    # A parent_path segment was redacted, which taints every verdict: the CLI
    # refuses the listing's parent unless the caller passes --parent.
    parent_redacted: bool = False
    # The run stopped at its limit, so names beyond it were never listed.
    truncated: bool = False
    # The run recorded failures, so part of the source was not listed at all.
    incomplete: bool = False
    # The run's warnings: a degraded sub-fetch leaves the listing possibly partial.
    run_warnings: List[str] = field(default_factory=list)


def _as_attribute(value: object) -> Optional[str]:
    # bool before int: bool is an int subclass, and str(True) is "True"
    # where the JSON the caller read said `true`.
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    return None


def run_envelope_view(envelope: object) -> ProbeRunEnvelopeView:
    """Parsed `probe run` JSON as the envelope's keys, None for one it lacks.
    Refuses anything that is not a mapping; every value is left for the reader
    to check."""
    if not isinstance(envelope, Mapping):
        raise ValueError(
            "that file is not a `probe run` output: it holds "
            f"{type(envelope).__name__}, not a result envelope. Pass the JSON "
            "`probe run` printed, or --name instead."
        )
    # cast: mypy cannot type a comprehension over a TypedDict's keys as that
    # TypedDict. It claims only what is built here, every declared key with an
    # unchecked `object` value; the JSON is trusted for nothing more.
    return cast(
        ProbeRunEnvelopeView,
        {key: envelope.get(key) for key in ProbeRunEnvelopeView.__annotations__},
    )


def listing_from_run(envelope: object) -> RunListing:
    """Read a `probe run` result envelope as a listing to judge. Refuses
    rather than guesses: a result that is not a list holds no names."""
    run = run_envelope_view(envelope)
    result = run["result"]
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
        # `in`: maskers write "***" or "***REDACTED:NAME***", possibly as a
        # substring of a longer identifier.
        if MASK in name:
            skipped.append(index)
            continue
        kept: Dict[str, str] = {}
        for key, value in fields.items():
            text = _as_attribute(value) if key != "name" else None
            if text is None:
                continue
            # Keys are masked as well as values.
            if MASK in key or MASK in text:
                if key not in masked_keys:
                    masked_keys.append(key)
                continue
            kept[key] = text
        names.append(name)
        attributes.append(kept)
    kind = _unmasked(run["kind"])
    source_type = _unmasked(run["source_type"])
    failures = run["failures"]
    warnings = run["warnings"]
    parent_path = _parent_path(run["parent_path"])
    return RunListing(
        kind=kind,
        source_type=source_type,
        parent_path=parent_path,
        names=names,
        attributes=attributes,
        skipped=skipped,
        masked_attributes=masked_keys,
        parent_redacted=any(MASK in segment for segment in parent_path),
        truncated=run["truncated"] is True,
        incomplete=isinstance(failures, list) and len(failures) > 0,
        run_warnings=[w for w in warnings if isinstance(w, str)]
        if isinstance(warnings, list)
        else [],
    )


def _unmasked(value: object) -> Optional[str]:
    # A redacted kind or source_type reads as absent, so the CLI asks for --kind.
    if isinstance(value, str) and value and MASK not in value:
        return value
    return None


def _parent_path(parent: object) -> List[str]:
    if parent is None:
        return []
    # Refused rather than read as no parent or as a repr no source has.
    if not isinstance(parent, list) or not all(isinstance(p, str) for p in parent):
        raise ValueError("not a `probe run` listing: its parent_path is malformed")
    return list(parent)


def listing_warnings(listing: RunListing) -> List[str]:
    """What the caller must know about a listing judged as it stands.

    Each is a warning, not a refusal: the names that are there still get a
    correct verdict. What would be wrong is reading the result as covering
    every object the source has.
    """
    warnings: List[str] = []
    if listing.skipped:
        warnings.append(
            "not judged, because redaction masked their names: listing entries "
            f"{', '.join(str(i) for i in listing.skipped)}. A name that collides "
            "with a secret reads '***'; judge it with --name and the real name"
        )
    if listing.masked_attributes:
        keys = ", ".join(f"`{k}`" for k in listing.masked_attributes)
        warnings.append(
            f"redaction masked {keys} on some entries, so those values were "
            "left out of their verdicts; a source that filters on them judged "
            "those names without them"
        )
    if listing.truncated:
        warnings.append(
            "that listing was truncated, so names beyond it were not judged"
        )
    if listing.incomplete:
        warnings.append(
            "the run that wrote that listing recorded failures, so the listing "
            "is incomplete and names it could not read were not judged"
        )
    warnings.extend(
        f"the run that wrote that listing warned, so it may be partial: {w}"
        for w in listing.run_warnings
    )
    return warnings


@dataclass(frozen=True)
class FilterRequest:
    """What `probe filter` judges: its flags reconciled with a listing."""

    kind: str
    parent_path: List[str]
    names: List[str]
    # Aligned with names; None for names given bare, which carry no facts.
    attributes: Optional[List[Dict[str, str]]]
    # What the caller must know about the listing (listing_warnings).
    warnings: List[str]


def filter_request(
    *,
    source_type: str,
    kind: Optional[str],
    parents: Sequence[str],
    names: Sequence[str],
    listing: Optional[RunListing],
) -> FilterRequest:
    """The kind, parent and names `probe filter` judges, from --kind,
    --parent and either --name or a `probe run` listing (--from-run).

    A listing brings its own names, kind and parent: --kind may restate its
    kind but not contradict it, and --parent replaces its parent_path. The
    caller refuses --name alongside a listing.
    """
    # Before the listing branch, so a --parent replacing a redacted
    # parent_path is checked too.
    _refuse_masked("--parent", parents, what="container name")
    _refuse_masked("--name", names, what="object name")
    if listing is None:
        if not names:
            raise ValueError(
                "nothing to judge: pass --name, or --from-run with a `probe run` output"
            )
        if not kind:
            raise ValueError("pass --kind: it says what kind of object the names are")
        return FilterRequest(
            kind=kind,
            parent_path=list(parents),
            names=list(names),
            attributes=None,
            warnings=[],
        )
    return _listing_request(
        source_type=source_type, kind=kind, parents=parents, listing=listing
    )


def _refuse_masked(flag: str, values: Sequence[str], *, what: str) -> None:
    """Refuse a flag value copied from masked output: judged, `***` would get
    a confident verdict on a name the source does not have."""
    # `in`, as listing_from_run tests: the mask may sit inside a longer
    # identifier. Only the mask is echoed, never the rest of the value.
    if any(MASK in value for value in values):
        raise ValueError(
            f"a {flag} value holds '{MASK}', the mask a secret was replaced "
            f"with, so it cannot be judged; pass the real {what}. This happens "
            "when a secret's value equals an identifier: output that masks the "
            "secret masks the identifier too"
        )


def _listing_request(
    *,
    source_type: str,
    kind: Optional[str],
    parents: Sequence[str],
    listing: RunListing,
) -> FilterRequest:
    _refuse_other_listing(source_type=source_type, kind=kind, listing=listing)
    judged_kind = kind or listing.kind
    if not judged_kind:
        raise ValueError("the listing does not say what kind it holds; pass --kind")
    if not parents and listing.parent_redacted:
        raise ValueError(
            "that listing's parent_path was redacted, so its names "
            "cannot be judged against the right container; pass "
            "--parent with the real container names"
        )
    return FilterRequest(
        kind=judged_kind,
        parent_path=list(parents) if parents else list(listing.parent_path),
        names=list(listing.names),
        attributes=listing.attributes,
        warnings=listing_warnings(listing),
    )


def _refuse_other_listing(
    *, source_type: str, kind: Optional[str], listing: RunListing
) -> None:
    """Refuse a listing of another source's objects, or of another kind."""
    if listing.source_type and listing.source_type != source_type:
        raise ValueError(
            f"this listing came from {listing.source_type}, the recipe is {source_type}"
        )
    if kind and listing.kind and kind.lower() != listing.kind.lower():
        raise ValueError(
            f"--kind {kind} contradicts the listing, which holds {listing.kind} names"
        )
