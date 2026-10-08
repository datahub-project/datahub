"""One assertion for "the probe's verdicts are ingestion's".

Runs ingestion, lists the same fixture with the probe, judges every listing
the way `probe run --report-to` then `probe filter --from-run` would (through
the CLI's own envelope, redaction and listing-warning code), and
compares per kind in both directions. Every connector's hand-written parity
test re-implemented these steps: the redacted JSON round trip, the fan-out
under every parent (kept or not), the identity a listing record shares with
an emitted URN, and the guard against two empty sets agreeing about nothing.

The harness patches nothing. Wrap the call in the connector's own mocks so
that ingestion and the probe read the same content.
"""

import dataclasses
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Pattern,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

from datahub._codegen.aspect import _Aspect
from datahub.cli.recipe_cli import (
    probe_run_envelope,
    report_to_text,
    resolve_probe_recipe,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import (
    RunListing,
    listing_from_run,
    listing_warnings,
    run_envelope_view,
)
from datahub.ingestion.agent.models import ProbeRunEnvelopeView
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    SubTypesClass,
)
from datahub.metadata.urns import Urn

_Metadata = Union[
    MetadataChangeEventClass, MetadataChangeProposalClass, MetadataChangeProposalWrapper
]


@dataclass
class EmittedIndex:
    """What one ingestion run emitted, as aspects by entity URN.

    A PATCH proposal (or any MCPC that does not decode to a whole aspect)
    records its URN with no aspect, so `urns(with_aspect=...)` and
    `container_names` miss an aspect the run only ever patched.
    """

    aspects: Dict[str, List[_Aspect]] = field(default_factory=dict)

    @classmethod
    def from_workunits(cls, workunits: Iterable[MetadataWorkUnit]) -> "EmittedIndex":
        index = cls()
        for workunit in workunits:
            index.add(workunit.metadata)
        return index

    @classmethod
    def from_file(cls, path: Path) -> "EmittedIndex":
        index = cls()
        for item in read_metadata_file(path):
            index.add(item)
        return index

    def add(self, item: _Metadata) -> None:
        if isinstance(item, MetadataChangeEventClass):
            urn = item.proposedSnapshot.urn
            found: List[_Aspect] = [a for a in item.proposedSnapshot.aspects]
        else:
            if not item.entityUrn:
                return
            urn = item.entityUrn
            wrapper = (
                item
                if isinstance(item, MetadataChangeProposalWrapper)
                else MetadataChangeProposalWrapper.try_from_mcpc(item)
            )
            found = (
                [wrapper.aspect]
                if wrapper is not None and wrapper.aspect is not None
                else []
            )
        # setdefault even with no decodable aspect: the URN was still emitted.
        self.aspects.setdefault(urn, []).extend(found)

    def urns(
        self, entity_type: str, *, with_aspect: Optional[Type[_Aspect]] = None
    ) -> Set[str]:
        """Every emitted URN of `entity_type`. `with_aspect` keeps only those
        carrying that aspect. Some sources emit an aspect onto an entity they
        merely reference (a lineage upstream), and that entity is not one of
        their own listings."""
        return {
            urn
            for urn, aspects in self.aspects.items()
            if Urn.from_string(urn).entity_type == entity_type
            and (
                with_aspect is None or any(isinstance(a, with_aspect) for a in aspects)
            )
        }

    def container_names(self, sub_type: Optional[str] = None) -> Set[str]:
        """The containerProperties name of every emitted container, optionally
        only those whose subTypes include `sub_type`. Container URNs are
        GUIDs, so the name is the only thing a listing can be compared on.

        Bare names: two same-named containers under different parents
        collapse into one here. The listing side catches it, since a fanned
        out listing's records with one bare name under two parents fail the
        harness's identity check; qualify `emitted` and `identity` alike when
        the fixture has such a pair."""
        names: Set[str] = set()
        for aspects in self.aspects.values():
            properties = next(
                (a for a in aspects if isinstance(a, ContainerPropertiesClass)), None
            )
            if properties is None:
                continue
            if sub_type is not None and not any(
                isinstance(a, SubTypesClass) and sub_type in a.typeNames
                for a in aspects
            ):
                continue
            names.add(properties.name)
        return names


def pipeline_ingestion(
    source_type: str, tmp_path: Path, **pipeline: object
) -> Callable[[Dict[str, object]], EmittedIndex]:
    """Run ingestion as `datahub ingest` does: a Pipeline with a file sink.
    `pipeline` adds top-level keys such as pipeline_name."""

    def run(recipe: Dict[str, object]) -> EmittedIndex:
        out = tmp_path / f"{source_type.rsplit('.', 1)[-1]}-parity.json"
        ingestion = Pipeline.create(
            {
                "run_id": "probe-parity",
                **pipeline,
                "source": {"type": source_type, "config": recipe},
                "sink": {"type": "file", "config": {"filename": str(out)}},
            }
        )
        ingestion.run()
        # A failed run compared against the probe would report a drift that
        # is really an ingestion error, or agree about a half-empty run.
        ingestion.raise_from_status()
        return EmittedIndex.from_file(out)

    return run


@dataclass(frozen=True)
class JudgedRecord:
    name: str
    parent_path: Tuple[str, ...]
    attributes: Mapping[str, str]
    included: bool
    excluded_by: Optional[str]


def by_name(record: JudgedRecord) -> str:
    return record.name


def by_qualified_name(record: JudgedRecord) -> str:
    """`parent_path` and `name` joined with ".", outermost first: "g1.a"."""
    return ".".join((*record.parent_path, record.name))


@dataclass(frozen=True)
class FanOut:
    """Run the listing once per record of `parent_command`, every one of them
    kept or not, passing that record's name as `param`. A child of a dropped
    parent must read as excluded on its own listing's facts, so the excluded
    parents are the point."""

    parent_command: str
    param: str
    parent_kwargs: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ParityListing:
    # Names this kind in failures and in ParityReport.kinds.
    label: str
    command: str
    # The identities ingestion emitted for this kind.
    emitted: Callable[[EmittedIndex], Set[str]]
    kwargs: Mapping[str, object] = field(default_factory=dict)
    fan_out: Optional[FanOut] = None
    # Maps a judged record to the identity `emitted` returns. Left unset, it
    # is `by_name`, or `by_qualified_name` when `fan_out` is set, since the
    # same name under two parents is two objects.
    identity: Optional[Callable[[JudgedRecord], str]] = None
    # True when the recipe switches this kind off, so ingestion emitting none
    # is the expected answer rather than a vacuous one. The probe must still
    # list something: an empty listing fails even so, because a kind the
    # recipe switches off is still listed, and judged excluded, by the probe.
    expect_empty: bool = False
    # Warnings the source gives on every normal run of this listing, which do
    # not make it partial: a str matches a warning exactly, a Pattern by
    # fullmatch. Applied to every listing taken for this kind, fan-out parents
    # included. It never waives truncation, failures or redaction, and an
    # entry that matched nothing in the run fails, so the list cannot go stale.
    accept_warnings: Tuple[Union[str, Pattern[str]], ...] = ()

    def identity_of(self, record: JudgedRecord) -> str:
        if self.identity is not None:
            return self.identity(record)
        if self.fan_out is not None:
            return by_qualified_name(record)
        return by_name(record)


@dataclass(frozen=True)
class KindParity:
    emitted: FrozenSet[str]
    included: FrozenSet[str]
    excluded_by: Mapping[str, Optional[str]]
    warnings: Tuple[str, ...]
    # The run warnings accept_warnings let through, in the order first seen.
    accepted_warnings: Tuple[str, ...] = ()


@dataclass
class _Acceptance:
    """Which of a ParityListing's accept_warnings matched, across its run."""

    accept: Tuple[Union[str, Pattern[str]], ...]
    used: Set[int] = field(default_factory=set)
    accepted: List[str] = field(default_factory=list)

    def take(self, warning: str) -> bool:
        hits = {
            i
            for i, entry in enumerate(self.accept)
            if (
                warning == entry
                if isinstance(entry, str)
                else entry.fullmatch(warning) is not None
            )
        }
        if not hits:
            return False
        self.used |= hits
        if warning not in self.accepted:
            self.accepted.append(warning)
        return True

    def unused(self) -> List[str]:
        return [
            entry if isinstance(entry, str) else entry.pattern
            for i, entry in enumerate(self.accept)
            if i not in self.used
        ]


@dataclass(frozen=True)
class ParityReport:
    kinds: Mapping[str, KindParity]

    def excluded_by(self, label: str) -> Mapping[str, Optional[str]]:
        return self.kinds[label].excluded_by


def _resolve(
    source_type: str, recipe: Mapping[str, object]
) -> Tuple[Dict[str, object], Set[str]]:
    _type, resolved, secrets = resolve_probe_recipe(
        {"source": {"type": source_type, "config": dict(recipe)}}
    )
    return resolved, secrets


def _envelope(
    source_type: str,
    resolved: Dict[str, object],
    secrets: Set[str],
    command: str,
    kwargs: Mapping[str, object],
) -> ProbeRunEnvelopeView:
    run = run_probe_method(source_type, dict(resolved), command, dict(kwargs))
    return run_envelope_view(
        json.loads(report_to_text(probe_run_envelope(run, secrets)))
    )


def report_envelope(
    source_type: str,
    recipe: Mapping[str, object],
    command: str,
    kwargs: Mapping[str, object],
) -> ProbeRunEnvelopeView:
    """The JSON `probe run <command> --report-to` writes for this recipe, as
    the harness reads it."""
    resolved, secrets = _resolve(source_type, recipe)
    return _envelope(source_type, resolved, secrets, command, kwargs)


def _listing(
    source_type: str,
    resolved: Dict[str, object],
    secrets: Set[str],
    command: str,
    kwargs: Mapping[str, object],
    acceptance: _Acceptance,
) -> RunListing:
    envelope = _envelope(source_type, resolved, secrets, command, kwargs)
    listing = listing_from_run(envelope)
    where = f"`probe run {command}` with {dict(kwargs)}"
    if listing.truncated:
        raise AssertionError(
            f"{where} stopped at its limit, so the names past it were never "
            f"judged; pass a larger limit in the listing's kwargs"
        )
    if listing.incomplete:
        raise AssertionError(
            f"{where} recorded failures, so part of the fixture was never "
            f"listed: {envelope['failures']}"
        )
    if listing.skipped or listing.masked_attributes or listing.parent_redacted:
        raise AssertionError(
            f"{where} had values redacted because a fixture secret equals an "
            f"identifier; change the fixture's secret"
        )
    # Filtered raw, before listing_warnings words each one as "may be partial".
    listing = dataclasses.replace(
        listing,
        run_warnings=[w for w in listing.run_warnings if not acceptance.take(w)],
    )
    # Whatever else `probe filter --from-run` would warn about this listing,
    # chiefly a soft-degraded sub-fetch: the listing may be partial with no
    # failure recorded, and a partial listing proves nothing.
    said = listing_warnings(listing)
    if said:
        raise AssertionError(f"{where} cannot be compared: {said}")
    return listing


def _judge(
    source_type: str,
    resolved: Dict[str, object],
    secrets: Set[str],
    command: str,
    kwargs: Mapping[str, object],
    acceptance: _Acceptance,
) -> Tuple[List[JudgedRecord], List[str]]:
    listing = _listing(source_type, resolved, secrets, command, kwargs, acceptance)
    kind = listing.kind
    if kind is None:
        raise AssertionError(
            f"`probe run {command}` declares no kind, so probe filter cannot judge it"
        )
    result = check_filters(
        source_type=source_type,
        config_dict=dict(resolved),
        kind=kind,
        parent_path=listing.parent_path,
        names=listing.names,
        attributes=listing.attributes,
    )
    records = [
        JudgedRecord(
            name=verdict.name,
            parent_path=tuple(listing.parent_path),
            attributes=attributes,
            included=verdict.included,
            excluded_by=verdict.excluded_by,
        )
        for verdict, attributes in zip(result.results, listing.attributes, strict=True)
    ]
    return records, list(result.warnings)


def _judged(
    source_type: str,
    resolved: Dict[str, object],
    secrets: Set[str],
    listing: ParityListing,
    acceptance: _Acceptance,
) -> Tuple[List[JudgedRecord], List[str]]:
    if listing.fan_out is None:
        return _judge(
            source_type,
            resolved,
            secrets,
            listing.command,
            listing.kwargs,
            acceptance,
        )
    fan_out = listing.fan_out
    parents = _listing(
        source_type,
        resolved,
        secrets,
        fan_out.parent_command,
        fan_out.parent_kwargs,
        acceptance,
    ).names
    records: List[JudgedRecord] = []
    warnings: List[str] = []
    for parent in parents:
        found, said = _judge(
            source_type,
            resolved,
            secrets,
            listing.command,
            {**listing.kwargs, fan_out.param: parent},
            acceptance,
        )
        records.extend(found)
        warnings.extend(w for w in said if w not in warnings)
    return records, warnings


def _compare(
    listing: ParityListing,
    records: List[JudgedRecord],
    warnings: List[str],
    acceptance: _Acceptance,
    emitted: Set[str],
    problems: List[str],
) -> KindParity:
    label = listing.label
    for entry in acceptance.unused():
        problems.append(
            f"{label}: accept_warnings entry {entry!r} matched nothing in this "
            f"run; drop it, or the allow-list outlives the warning it named"
        )
    # Every record under each identity. Two distinct records merged into one
    # identity hide a drift even when their verdicts agree: ingestion dropping
    # one of them still emits the identity the other stands for.
    grouped: Dict[str, List[JudgedRecord]] = {}
    for record in records:
        grouped.setdefault(listing.identity_of(record), []).append(record)
    for identity, group in sorted(grouped.items()):
        keys = sorted({(*r.parent_path, r.name) for r in group})
        if len(keys) > 1:
            listed = ", ".join("/".join(key) for key in keys)
            problems.append(
                f"{label}: '{identity}' names {len(keys)} distinct listing "
                f"records ({listed}); its identity function must tell them apart"
            )
        elif len({r.included for r in group}) > 1:
            problems.append(
                f"{label}: '{identity}' was listed more than once with "
                f"different verdicts"
            )
    included = {i for i, group in grouped.items() if group[0].included}
    excluded_by = {
        i: group[0].excluded_by for i, group in grouped.items() if not group[0].included
    }
    if not records:
        problems.append(f"{label}: the probe listed nothing, so nothing was compared")
    elif not emitted and not listing.expect_empty:
        problems.append(
            f"{label}: ingestion emitted none, so agreement proves nothing; give "
            f"the fixture one the recipe keeps, or set expect_empty"
        )
    for identity in sorted(emitted - included):
        if identity in excluded_by:
            problems.append(
                f"{label}: ingestion emitted '{identity}', but probe filter "
                f"excludes it (excluded_by={excluded_by[identity]})"
            )
        else:
            problems.append(
                f"{label}: ingestion emitted '{identity}', but no probe listing "
                f"returned it"
            )
    for identity in sorted(included - emitted):
        problems.append(
            f"{label}: probe filter includes '{identity}', but ingestion did not "
            f"emit it"
        )
    return KindParity(
        emitted=frozenset(emitted),
        included=frozenset(included),
        excluded_by=excluded_by,
        warnings=tuple(warnings),
        accepted_warnings=tuple(acceptance.accepted),
    )


def assert_probe_parity(
    source_type: str,
    recipe: Mapping[str, object],
    run_ingestion: Callable[[Dict[str, object]], EmittedIndex],
    listings: Sequence[ParityListing],
) -> ParityReport:
    """Assert that ingestion and `probe filter` agree on every listed kind.

    For each listing, every identity ingestion emitted must be included by
    the probe, every identity the probe includes must have been emitted, and
    the comparison must not be vacuous. Raises one AssertionError naming every
    disagreement. Returns the per-kind verdicts so that a test can also pin
    which rule excluded what.
    """
    if not listings:
        # Before ingestion runs: with no listing, every kind would agree.
        raise AssertionError(
            "assert_probe_parity was given no listing, so it would compare "
            "nothing; pass a ParityListing per kind"
        )
    emitted = run_ingestion(dict(recipe))
    # The config and secrets `probe run` and `probe filter` work from, so the
    # probe side judges the recipe exactly as the CLI would.
    resolved, secrets = _resolve(source_type, recipe)
    problems: List[str] = []
    kinds: Dict[str, KindParity] = {}
    for listing in listings:
        acceptance = _Acceptance(listing.accept_warnings)
        records, warnings = _judged(source_type, resolved, secrets, listing, acceptance)
        kinds[listing.label] = _compare(
            listing, records, warnings, acceptance, listing.emitted(emitted), problems
        )
    if problems:
        raise AssertionError(
            "probe filter and ingestion disagree:\n  " + "\n  ".join(problems)
        )
    return ParityReport(kinds=kinds)
