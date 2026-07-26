import re
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from typing_extensions import TypeGuard

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import (
    ProbeLeafKind,
    ProbeNode,
    ProbeNodeKind,
    ProbeResult,
    ProbeShapeNode,
)
from datahub.ingestion.source.source_registry import source_registry


class ProbeCapableConfig(Protocol):
    """Contract a connector config implements to opt into live probing.

    The probe framework never references concrete sources — it discovers this
    capability by duck-typing on the config class. Each connector owns its probe
    logic in its own package and builds ProbeNodes with the helpers below.
    """

    @classmethod
    def probe_hierarchy(cls) -> List[ProbeNodeKind]:
        """Ordered container/leaf levels this source exposes, top-first.

        Structural metadata only — must not open a connection, so the framework
        (and CLI) can advertise support and compute required arguments without
        validating config or connecting.
        """
        ...

    def list_probe_children(self, parent_path: List[str], limit: int) -> ProbeResult:
        """List the children directly under parent_path (names/counts only)."""
        ...


# --- Reusable, source-agnostic ProbeNode builders --------------------------------
# Connectors compose these so every probe result speaks the same shape; the
# framework owns them precisely because they carry no source-specific knowledge.


@dataclass(frozen=True)
class Verdict:
    """A connector's verdict for one node: would it be ingested given the
    recipe's filters plus the source's built-in exclusions?

    excluded_by names the reason a node would be dropped (a *_pattern field,
    "default_schema", "system_object"), or None when included. The filtering
    logic itself lives in the connector (reusing its own ingestion filters);
    the framework only carries it.
    """

    included: bool
    excluded_by: Optional[str] = None

    @classmethod
    def include(cls) -> "Verdict":
        return cls(True, None)


_INCLUDED = Verdict.include()

# A level the source offers no filter for (e.g. Mode's datasets and queries).
# Distinct from pattern_field=None, which means "resolve the conventional
# <kind>_pattern field". Nodes at an UNFILTERED level report pattern_field=None
# and are always included.
UNFILTERED: str = "__unfiltered__"

# Stand-in for a node whose lister produced no usable name. Listers are declared
# to return Sequence[str], but real APIs break that contract — Mode hands back
# reports with name=null. Such a node can be neither filtered
# (AllowDenyPattern.allowed raises TypeError on a non-string) nor addressed as a
# --parent, but it does exist, and a probe is a diagnostic: "two unnamed reports
# live here" is more useful to a caller than a dropped row or a stack trace.
UNNAMED: str = "<unnamed>"

# Reported like the connectors' own structural exclusions ("system_object",
# "default_schema"): a statement about what the probe can address, not a
# prediction that ingestion would skip the object.
_UNNAMED_VERDICT = Verdict(False, "unnamed")


def _usable_name(name: object) -> TypeGuard[str]:
    """Whether a lister gave us a name we can filter on and descend into."""
    return isinstance(name, str) and bool(name.strip())


def _join_fqn(prefix: Optional[str], name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


# (name, kind, pattern_field) for one child at a level.
LevelItem = Tuple[str, ProbeNodeKind, Optional[str]]
# verdict_for(name, fqn, pattern_field) -> Verdict
VerdictFor = Callable[[str, str, Optional[str]], Verdict]


def pattern_verdict(config: Any, pattern_field: Optional[str], target: str) -> Verdict:
    """The standard allow/deny check: the config's *_pattern field against `target`.

    Exported so a custom level classifier can defer to it after its own
    structural exclusions.
    """
    if pattern_field is None:
        return _INCLUDED
    pattern = getattr(config, pattern_field)
    return _INCLUDED if pattern.allowed(target) else Verdict(False, pattern_field)


def level_nodes(
    items: Sequence[LevelItem],
    limit: int,
    fqn_prefix: Optional[str],
    verdict_for: VerdictFor,
) -> Tuple[List[ProbeNode], bool]:
    """Build one level's nodes from (name, kind, pattern_field) triples.

    The single node-construction path: fqn prefixing, per-node kind and pattern,
    the include/exclude verdict, and truncation all happen here.
    """
    nodes: List[ProbeNode] = []
    for name, kind, pattern_field in items[:limit]:
        if not _usable_name(name):
            nodes.append(
                ProbeNode(
                    UNNAMED,
                    kind,
                    _join_fqn(fqn_prefix, UNNAMED),
                    pattern_field,
                    _UNNAMED_VERDICT.included,
                    _UNNAMED_VERDICT.excluded_by,
                )
            )
            continue
        node_fqn = _join_fqn(fqn_prefix, name)
        verdict = verdict_for(name, node_fqn, pattern_field)
        nodes.append(
            ProbeNode(
                name,
                kind,
                node_fqn,
                pattern_field,
                verdict.included,
                verdict.excluded_by,
            )
        )
    return nodes, len(items) > limit


# --- ClientProbe: declarative helper for connector-based probes ------------------
# A source with a client that can be built from its config and lists entities by
# name reduces to a declaration: the client factory, and one ProbeLevel per level.
# ClientProbe reuses the node builders and the config's own *_pattern, so a new
# non-SQL source needs no bespoke list_probe_children plumbing.

# Given the connector's client, its config, and the parent path already descended,
# return the child names at the current level.
LevelLister = Callable[[Any, Any, List[str]], Sequence[str]]

# A lister for a level whose single listing yields several kinds (e.g. BigQuery's
# list_tables, which returns tables and views distinguished by table_type).
LevelItemLister = Callable[[Any, Any, List[str]], Sequence[LevelItem]]


@dataclass
class LevelSource:
    """One lister feeding a level, with the kind and pattern its nodes carry.

    Lets a single level be assembled from several listings that differ in kind
    and filter (e.g. tables + views).
    """

    list_names: LevelLister
    kind: ProbeNodeKind
    pattern_field: Optional[str] = None


@dataclass(frozen=True)
class ClassifyContext:
    """Everything a level classifier needs to judge one child node."""

    config: Any
    name: str
    fqn: str
    pattern_field: Optional[str]
    # The container names already descended, top-first — the parent of this node.
    parent_path: Tuple[str, ...]


# classify(ctx) -> Verdict
LevelClassifier = Callable[[ClassifyContext], Verdict]


@dataclass
class ProbeLevel:
    kind: ProbeNodeKind
    # The config *_pattern field that filters this level. None means one of two
    # things: for a leaf (column) level, there is no filter to carry; for any other
    # level, resolve the conventional <kind>_pattern/<kind>_patterns field instead
    # (against the live config instance, then its class), raising if neither has
    # one (see ClientProbe._resolved / pattern_field_for_config).
    pattern_field: Optional[str] = None
    list_names: Optional[LevelLister] = None
    # The exact string this level's pattern is matched against. Set it to the
    # connector's own identifier function — never to a reimplementation of it —
    # so the probe and ingestion cannot disagree about what is being filtered.
    # When None, the node's bare name is the match target.
    filter_target: Optional[Callable[["ClassifyContext"], str]] = None
    # A level fed by several listers, each contributing its own kind and pattern
    # (e.g. tables + views). Mutually exclusive with list_names/list_items.
    sources: Optional[List[LevelSource]] = None
    # Full verdict override, for structural drops the user's patterns don't
    # express (INFORMATION_SCHEMA, sys$…) or non-standard match semantics.
    # Defaults to pattern_verdict() when None.
    classify: Optional[LevelClassifier] = None
    # A single listing that itself yields several kinds (e.g. BigQuery's
    # list_tables returning both tables and views). Each item carries its own kind
    # and pattern_field, like `sources`, but without a second client call.
    # Mutually exclusive with list_names/sources.
    list_items: Optional[LevelItemLister] = None
    # The level this one hangs off, by kind; None marks the top level. The edges
    # — not this list's order — define the hierarchy, so a level is self-describing
    # and reordering the declaration cannot silently change the shape.
    parent: Optional[ProbeNodeKind] = None

    def __post_init__(self) -> None:
        modes = [self.list_names, self.sources, self.list_items]
        if sum(mode is not None for mode in modes) != 1:
            raise ValueError(
                "ProbeLevel requires exactly one of list_names, sources, or list_items"
            )
        if self.sources is not None or self.list_items is not None:
            # Both modes carry kind and pattern per item, so a level-wide value
            # would be silently ignored.
            if self.pattern_field is not None:
                raise ValueError(
                    "a sources/list_items level carries pattern_field per item; "
                    "remove the level-wide pattern_field"
                )


# A pattern field is conventionally named after the kind it filters:
# Schema -> schema_pattern, Topic -> topic_patterns.
_PATTERN_SUFFIXES = ("_pattern", "_patterns")


def _pattern_field_candidates(kind: ProbeNodeKind) -> List[str]:
    base = re.sub(r"[^a-z0-9]+", "_", str(kind).lower()).strip("_")
    return [base + suffix for suffix in _PATTERN_SUFFIXES]


@lru_cache(maxsize=None)
def _hinted_pattern_field(config_cls: type, kind: ProbeNodeKind) -> Optional[str]:
    """The field explicitly declaring Filters(kind), or None.

    Exact by construction: unlike the name convention, a hint cannot
    accidentally match, so a wrong result here is a declaration bug and is
    raised rather than guessed around.
    """
    # lazy, for the same reason as pattern_field_for_config_class below:
    # introspect imports source_registry at module level, and that must stay off
    # probe.py's import path. Filters is imported here too so the two stay together.
    from datahub.configuration.common import Filters
    from datahub.ingestion.agent.introspect import is_pattern_field

    wanted = str(kind)
    fields = getattr(config_cls, "model_fields", {})
    matches = sorted(
        name
        for name, field in fields.items()
        if any(
            isinstance(meta, Filters) and str(meta.kind) == wanted
            for meta in field.metadata
        )
    )
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"{config_cls.__name__} declares Filters({wanted!r}) on more than one "
            f"field ({', '.join(matches)}); a level must resolve to exactly one "
            f"AllowDenyPattern"
        )
    name = matches[0]
    if not is_pattern_field(fields[name].annotation):
        raise ValueError(
            f"{config_cls.__name__}.{name} declares Filters({wanted!r}) but is "
            f"not an AllowDenyPattern"
        )
    return name


@lru_cache(maxsize=None)
def pattern_field_for_config_class(
    config_cls: type, kind: ProbeNodeKind
) -> Optional[str]:
    """Find the config class's AllowDenyPattern field that filters `kind`, by
    convention, from its declared pydantic fields.

    Returns None when no such field exists, or when a same-named field is not an
    AllowDenyPattern. This is the class-level fallback for when an instance has an
    Optional pattern field left as None — see pattern_field_for_config for the
    instance-aware check that runs first. Memoized: resolution is per (config
    class, kind) and never changes at runtime.
    """
    # lazy: introspect imports source_registry at module level; keep that off
    # probe.py's import path.
    from datahub.ingestion.agent.introspect import is_pattern_field

    hinted = _hinted_pattern_field(config_cls, kind)
    if hinted is not None:
        return hinted

    fields = getattr(config_cls, "model_fields", {})
    for name in _pattern_field_candidates(kind):
        field = fields.get(name)
        if field is not None and is_pattern_field(field.annotation):
            return name
    return None


def pattern_field_for_config(config: Any, kind: ProbeNodeKind) -> Optional[str]:
    """Find the *live config object's* AllowDenyPattern field that filters `kind`.

    A declared hint (Filters(kind) on a field's Annotated metadata) wins over
    both the instance check below and pattern_field_for_config_class's
    convention, since it is exact by construction. Failing that, checks the
    instance's own attributes first — what pattern_verdict() actually reads via
    getattr(config, pattern_field) — before falling back to
    pattern_field_for_config_class's class-level introspection (which also
    catches an Optional pattern field the instance happens to hold as None).
    Deliberately not memoized: unlike pattern_field_for_config_class's (class,
    kind) cache, many distinct config instances (e.g. every test fixture built as
    a plain SimpleNamespace) can share the same type, so caching by type would
    leak one instance's resolved field onto an unrelated instance of that same
    type.
    """
    # Narrowed via an annotated local: passing `type(config)` inline infers as
    # type[Any], which mypy's lru_cache stub rejects as Hashable (a metaclass
    # __hash__ signature mismatch) even though it is hashable at runtime.
    config_cls: type = type(config)
    hinted = _hinted_pattern_field(config_cls, kind)
    if hinted is not None:
        return hinted
    for name in _pattern_field_candidates(kind):
        if isinstance(getattr(config, name, None), AllowDenyPattern):
            return name
    return pattern_field_for_config_class(config_cls, kind)


class ProbeBranchesError(ValueError):
    """Raised by ClientProbe.hierarchy() when the probe's levels form a tree,
    not a chain, so there is no single ordered hierarchy to report.

    A distinct subclass (rather than a plain ValueError) so callers -- notably
    probe_shape()'s framework-level derivation -- can react specifically to
    "this probe branches" without misreading an unrelated ValueError the same
    way. Still a ValueError, so existing `except ValueError` call sites (e.g.
    recipe_cli's EXIT_USER mapping) keep working unchanged.
    """


class ProbeSoftError(Exception):
    """A connector's list_names raises this to report that one endpoint
    couldn't be read cleanly -- a 404 on a resource deleted between listing
    and fetch, or a 403 on something this token can't read -- and that
    ClientProbe.list_children should treat the contribution as empty rather
    than either:

    - letting the exception propagate and kill the whole list_children call,
      discarding sibling levels (e.g. Reports vs Datasets under a Mode Space)
      that already succeeded, or
    - silently swallowing it and returning [], which is indistinguishable
      from the level genuinely having no children.

    list_children catches this per LEVEL (not per endpoint): for a plain
    list_names/list_items level, that is the same thing, since one lister
    call produces the level's entire contribution. But a level assembled
    from several LevelSources (ProbeLevel.sources, e.g. tables + views) is
    fed by more than one lister, and the catch is still level-wide -- if the
    second source raises, whatever the first source already produced for
    this level is discarded too, not just the second source's share. No
    connector's sources= level raises ProbeSoftError today, so this hasn't
    manifested, but it is a real limitation of the current per-level
    granularity, not per-lister-within-a-level.

    list_children records str(exc) on ProbeResult.warnings and continues
    with the remaining sibling levels. Source-agnostic: any connector's
    lister may raise it, not just Mode's.
    """


@contextmanager
def soft_on_status(*codes: int, context: str) -> Iterator[None]:
    """Treat the given HTTP statuses as expected absence, not failure.

    A probe must distinguish "nothing here" from "could not look" (see
    ProbeSoftError): the listed codes become a ProbeSoftError, anything else
    propagates. Duck-types on `.response.status_code` -- matches
    requests.HTTPError and similar shapes -- so the framework takes no
    HTTP-library dependency; any HTTP-based connector can reuse this instead
    of writing its own status-code split.
    """
    try:
        yield
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in codes:
            raise ProbeSoftError(
                f"{context} returned HTTP {status}; treating it as empty."
            ) from exc
        raise


def _level_tree(
    levels: List[ProbeLevel],
) -> Tuple[ProbeLevel, Dict[ProbeNodeKind, List[ProbeLevel]]]:
    """Validate the declared edges and return (root, children-by-parent-kind).

    Several levels may share a parent: that is a branching source (a BI workspace
    holding reports and dashboards). Path elements disambiguate siblings, so the
    chain restriction is gone — but kinds must still be unique, there must be
    exactly one root, every parent must be declared, and nothing may be orphaned
    or cyclic.
    """
    by_kind = {level.kind: level for level in levels}
    if len(by_kind) != len(levels):
        raise ValueError("ProbeLevel kinds must be unique within a probe")

    roots = [level for level in levels if level.parent is None]
    if len(roots) != 1:
        raise ValueError(
            f"a probe needs exactly one root level (parent=None); found {len(roots)}: "
            f"{[str(level.kind) for level in roots]}"
        )

    children: Dict[ProbeNodeKind, List[ProbeLevel]] = {}
    for level in levels:
        if level.parent is None:
            continue
        if level.parent not in by_kind:
            raise ValueError(
                f"level '{level.kind}' declares unknown parent '{level.parent}'; "
                f"declared kinds are {[str(k) for k in by_kind]}"
            )
        children.setdefault(level.parent, []).append(level)

    # Reachability catches orphans and cycles disjoint from the root.
    seen: List[ProbeLevel] = []
    queue = [roots[0]]
    while queue:
        level = queue.pop(0)
        seen.append(level)
        queue.extend(children.get(level.kind, []))
    if len(seen) != len(levels):
        reached = {id(level) for level in seen}
        raise ValueError(
            "levels unreachable from the root (orphaned or cyclic): "
            f"{[str(lvl.kind) for lvl in levels if id(lvl) not in reached]}"
        )
    return roots[0], children


class ClientProbe:
    def __init__(
        self,
        client_factory: Callable[[Any], Any],
        levels: List[ProbeLevel],
        close: Callable[[Any], None] = lambda client: None,
    ) -> None:
        self._client_factory = client_factory
        self._root, self._children = _level_tree(levels)
        self._all_levels = levels
        self._close = close

    @property
    def is_linear(self) -> bool:
        return all(len(kids) <= 1 for kids in self._children.values())

    def hierarchy(self) -> List[ProbeNodeKind]:
        # Structural only — never touches the client, so it is connection-free.
        if not self.is_linear:
            raise ProbeBranchesError(
                "this probe branches, so its shape is a tree, not a chain; "
                "use shape() instead of hierarchy()"
            )
        chain = [self._root]
        while self._children.get(chain[-1].kind):
            chain.append(self._children[chain[-1].kind][0])
        return [level.kind for level in chain]

    def shape(self) -> ProbeShapeNode:
        def build(level: ProbeLevel) -> ProbeShapeNode:
            return ProbeShapeNode(
                level.kind,
                [build(child) for child in self._children.get(level.kind, [])],
            )

        return build(self._root)

    def _levels_for(self, parent_path: List[str]) -> Tuple[List[ProbeLevel], List[str]]:
        """Walk the path, returning the level(s) to list and the BARE names.

        Element i names a node belonging to one of the candidate levels at depth i.
        When only one level is possible the element is a plain name; when siblings
        make it ambiguous it must be qualified 'Subtype:name'.
        """
        candidates = [self._root]
        names: List[str] = []
        for depth, element in enumerate(parent_path):
            if not candidates:
                # Past the declared depth: nothing to descend into. Callers treat
                # an empty level list as "no children", matching the pre-tree
                # behaviour for over-long paths.
                return [], names
            if len(candidates) == 1:
                level, name = candidates[0], element
            else:
                kinds = {str(lvl.kind): lvl for lvl in candidates}
                qualifier, sep, rest = element.partition(":")
                if not sep or qualifier not in kinds:
                    raise ValueError(
                        f"'{element}' is ambiguous at depth {depth}: it could be a "
                        f"{' or a '.join(sorted(kinds))}. Qualify it as "
                        f"'Subtype:name', e.g. '{sorted(kinds)[0]}:{element}'."
                    )
                level, name = kinds[qualifier], rest
            names.append(name)
            candidates = self._children.get(level.kind, [])
        return candidates, names

    def _resolved(self, config: Any, items: List[LevelItem]) -> List[LevelItem]:
        # Non-leaf items must end up with a real pattern field: either the one the
        # declaration gave, or the conventional one for their kind, resolved
        # against the live config instance (see pattern_field_for_config). A
        # Column item has no pattern field to resolve — e.g. a sources/list_items
        # level mixing Column with a classify= override — so it passes through
        # as-is. A level only ever spans one or two kinds, so resolve each kind
        # at most once here rather than once per item (items can number in the
        # tens of thousands before truncation).
        resolved_by_kind: Dict[ProbeNodeKind, Optional[str]] = {}
        out: List[LevelItem] = []
        for name, kind, pattern_field in items:
            if kind == ProbeLeafKind.COLUMN:
                out.append((name, kind, pattern_field))
                continue
            if pattern_field == UNFILTERED:
                out.append((name, kind, None))
                continue
            if pattern_field is None:
                if kind not in resolved_by_kind:
                    resolved_by_kind[kind] = pattern_field_for_config(config, kind)
                pattern_field = resolved_by_kind[kind]
                if pattern_field is None:
                    raise ValueError(
                        f"connector bug: {type(config).__name__} declares a "
                        f"probe level for kind '{kind}' but has no "
                        f"AllowDenyPattern field to filter it (looked for "
                        f"{_pattern_field_candidates(kind)}); annotate the "
                        f"intended field with Annotated[AllowDenyPattern, "
                        f"Filters({kind!r})] (see datahub.configuration.common.Filters), "
                        f"or set pattern_field= explicitly on that level/source "
                        f"if the field's name doesn't otherwise matter"
                    )
            out.append((name, kind, pattern_field))
        return out

    def _items(
        self, level: ProbeLevel, client: Any, config: Any, parent_path: List[str]
    ) -> List[LevelItem]:
        if level.list_items is not None:
            items: List[LevelItem] = []
            index: Dict[str, int] = {}
            for name, kind, pattern_field in level.list_items(
                client, config, parent_path
            ):
                if name in index:
                    items[index[name]] = (name, kind, pattern_field)
                    continue
                index[name] = len(items)
                items.append((name, kind, pattern_field))
            return self._resolved(config, items)
        if level.sources is not None:
            items = []
            index = {}
            for source in level.sources:
                for name in source.list_names(client, config, parent_path):
                    # A name reported by two listings keeps its first position but
                    # takes the LATER source's kind/pattern: a dialect that reports
                    # views inside its table listing (Hive) must still classify them
                    # as views.
                    if name in index:
                        items[index[name]] = (name, source.kind, source.pattern_field)
                        continue
                    index[name] = len(items)
                    items.append((name, source.kind, source.pattern_field))
            return self._resolved(config, items)
        assert level.list_names is not None  # guaranteed by ProbeLevel.__post_init__
        items = [
            (name, level.kind, level.pattern_field)
            for name in level.list_names(client, config, parent_path)
        ]
        return self._resolved(config, items)

    def _nodes_for_level(
        self,
        level: ProbeLevel,
        client: Any,
        config: Any,
        names: List[str],
        prefix: str,
        limit: int,
    ) -> Tuple[List[ProbeNode], bool]:
        custom = level.classify
        target = level.filter_target

        def verdict_for(
            name: str, node_fqn: str, pattern_field: Optional[str]
        ) -> Verdict:
            if custom is not None:
                return custom(
                    ClassifyContext(
                        config=config,
                        name=name,
                        fqn=node_fqn,
                        pattern_field=pattern_field,
                        parent_path=tuple(names),
                    )
                )
            if target is not None:
                ctx = ClassifyContext(
                    config=config,
                    name=name,
                    fqn=node_fqn,
                    pattern_field=pattern_field,
                    parent_path=tuple(names),
                )
                return pattern_verdict(config, pattern_field, target(ctx))
            return pattern_verdict(config, pattern_field, name)

        return level_nodes(
            self._items(level, client, config, names),
            limit,
            prefix or None,
            verdict_for,
        )

    def list_children(
        self, config: Any, parent_path: List[str], limit: int
    ) -> ProbeResult:
        levels, names = self._levels_for(parent_path)
        if not levels:
            # Past the declared depth there are no children.
            return ProbeResult(source_type="", supported=True, parent_path=parent_path)
        client = self._client_factory(config)
        try:
            prefix = ".".join(names)
            nodes: List[ProbeNode] = []
            truncated = False
            warnings: List[str] = []
            for level in levels:
                # Each sibling contributes its own kinds, patterns and classify.
                # A level that raises ProbeSoftError (see its docstring)
                # contributes zero nodes and a warning instead of aborting the
                # whole call -- so e.g. a Space whose Reports 403s still
                # reports its Datasets, rather than reporting nothing at all.
                try:
                    nodes_from_level, level_truncated = self._nodes_for_level(
                        level, client, config, names, prefix, limit
                    )
                except ProbeSoftError as exc:
                    warnings.append(str(exc))
                    continue
                nodes.extend(nodes_from_level)
                truncated = truncated or level_truncated
            if len(nodes) > limit:
                nodes, truncated = nodes[:limit], True
            return ProbeResult(
                source_type="",
                supported=True,
                parent_path=parent_path,
                nodes=nodes,
                truncated=truncated,
                warnings=warnings,
            )
        finally:
            self._close(client)


# --- Framework entry points ------------------------------------------------------


# Returns the connector's config class. Typed Any because get_config_class is
# injected by the @config_class decorator at runtime — mypy can't see it, nor the
# pydantic model API (model_validate) / probe contract methods on the result.
def _config_class(source_type: str) -> Any:
    source_cls = source_registry.get(source_type)
    get_config_class = getattr(source_cls, "get_config_class", None)
    return get_config_class() if get_config_class is not None else None


def probe_hierarchy(source_type: str) -> Optional[List[ProbeNodeKind]]:
    # Resolve the source's declared hierarchy without validating config or
    # connecting; None means the source does not support live probing.
    try:
        config_cls = _config_class(source_type)
    except Exception:
        return None
    hierarchy_fn = getattr(config_cls, "probe_hierarchy", None)
    if not callable(hierarchy_fn):
        return None
    return hierarchy_fn()


def probe_shape(source_type: str) -> Optional[ProbeShapeNode]:
    """The source's level tree, or None when it has no probe support.

    Connection-free, like probe_hierarchy: reads only the declared levels.
    Resolution order:

    1. A config class that declares its own probe_shape() classmethod wins --
       this is the hook a branching connector uses, delegating to its own
       X_PROBE.shape(). Mode is the first connector that needs one (a Space
       branches into both Reports and Datasets); most connectors don't (see 2).
    2. Otherwise, the connector's probe is linear -- its own probe_hierarchy()
       classmethod already proves it, since ClientProbe.hierarchy() (which
       every linear classmethod delegates to) raises ProbeBranchesError at
       import time for a branching declaration. So a chain reported by
       probe_hierarchy() and the tree reported by shape() are the same
       information; reshaping the former into ProbeShapeNode avoids adding a
       near-duplicate probe_shape() classmethod to every connector whose
       probe doesn't actually branch.
    3. A branching connector with no probe_shape() classmethod is a connector
       bug, not "no probe support": raising ValueError (rather than returning
       None) keeps the CLI from reporting a probe-capable source as
       unsupported just because its shape can't be derived generically.
    """
    try:
        config_cls = _config_class(source_type)
    except Exception:
        return None
    shape_fn = getattr(config_cls, "probe_shape", None)
    if callable(shape_fn):
        result: ProbeShapeNode = shape_fn()
        return result
    try:
        hierarchy = probe_hierarchy(source_type)
    except ProbeBranchesError:
        raise ValueError(
            f"source '{source_type}' declares a branching probe, so it has no "
            "single hierarchy; its config class must define a probe_shape() "
            "classmethod returning its ClientProbe's shape()"
        ) from None
    if not hierarchy:
        return None
    node = ProbeShapeNode(hierarchy[-1], [])
    for kind in reversed(hierarchy[:-1]):
        node = ProbeShapeNode(kind, [node])
    return node


def probe(
    source_type: str,
    config_dict: Dict[str, object],
    parent_path: List[str],
    limit: int,
) -> ProbeResult:
    try:
        config_cls = _config_class(source_type)
    except Exception:
        config_cls = None
    # A source opts into probing by implementing the ProbeCapableConfig contract.
    if config_cls is None or not hasattr(config_cls, "probe_hierarchy"):
        return ProbeResult(
            source_type=source_type,
            supported=False,
            parent_path=parent_path,
            fallback="No live-probe support for this source; use test-connection or Layer C.",
        )
    config = config_cls.model_validate(config_dict)
    result = config.list_probe_children(parent_path, limit)
    # The provider doesn't know its own registry name; stamp it here so results
    # are self-describing without every connector threading source_type through.
    result.source_type = source_type
    return result
