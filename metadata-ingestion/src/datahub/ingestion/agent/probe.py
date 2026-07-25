import re
from dataclasses import dataclass
from functools import lru_cache
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    runtime_checkable,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import (
    ProbeLeafKind,
    ProbeNode,
    ProbeNodeKind,
    ProbeResult,
)
from datahub.ingestion.source.source_registry import source_registry


@runtime_checkable
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

# (included, excluded_by) — a connector's verdict for one node. excluded_by names
# the reason a node would be dropped (a *_pattern field, "default_schema",
# "system_object"), or None when included. The filtering logic itself lives in the
# connector (reusing its own ingestion filters); the framework only carries it.
Verdict = Tuple[bool, Optional[str]]

_INCLUDED: Verdict = (True, None)


def fqn(prefix: Optional[str], name: str) -> str:
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
    return _INCLUDED if pattern.allowed(target) else (False, pattern_field)


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
        node_fqn = fqn(fqn_prefix, name)
        included, excluded_by = verdict_for(name, node_fqn, pattern_field)
        nodes.append(
            ProbeNode(name, kind, node_fqn, pattern_field, included, excluded_by)
        )
    return nodes, len(items) > limit


def column_nodes(
    cols: Sequence[Dict[str, object]], limit: int, fqn_prefix: str
) -> Tuple[List[ProbeNode], bool]:
    nodes = [
        ProbeNode(
            str(col["name"]),
            ProbeLeafKind.COLUMN,
            f"{fqn_prefix}.{col['name']}",
            None,
        )
        for col in cols[:limit]
    ]
    return nodes, len(cols) > limit


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
    # Optional: derive the node kind per-name when a level mixes kinds (e.g.
    # Salesforce custom vs standard objects). `kind` stays the nominal/structural
    # kind reported by hierarchy().
    kind_for: Optional[Callable[[str], ProbeNodeKind]] = None
    # Some connectors filter on the fully-qualified name (PARENT.CHILD), not the
    # bare child name (e.g. BigID's dataset_pattern). Set to test the pattern
    # against the node fqn instead of its name.
    classify_on_fqn: bool = False
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
            if self.kind_for is not None:
                raise ValueError(
                    "a sources/list_items level carries kind per item; remove kind_for"
                )


# A pattern field is conventionally named after the kind it filters:
# Schema -> schema_pattern, Topic -> topic_patterns.
_PATTERN_SUFFIXES = ("_pattern", "_patterns")


def _pattern_field_candidates(kind: ProbeNodeKind) -> List[str]:
    base = re.sub(r"[^a-z0-9]+", "_", str(kind).lower()).strip("_")
    return [base + suffix for suffix in _PATTERN_SUFFIXES]


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

    fields = getattr(config_cls, "model_fields", {})
    for name in _pattern_field_candidates(kind):
        field = fields.get(name)
        if field is not None and is_pattern_field(field.annotation):
            return name
    return None


def pattern_field_for_config(config: Any, kind: ProbeNodeKind) -> Optional[str]:
    """Find the *live config object's* AllowDenyPattern field that filters `kind`.

    Checks the instance's own attributes first — what pattern_verdict() actually
    reads via getattr(config, pattern_field) — before falling back to
    pattern_field_for_config_class's class-level introspection (which also
    catches an Optional pattern field the instance happens to hold as None).
    Deliberately not memoized: unlike pattern_field_for_config_class's (class,
    kind) cache, many distinct config instances (e.g. every test fixture built as
    a plain SimpleNamespace) can share the same type, so caching by type would
    leak one instance's resolved field onto an unrelated instance of that same
    type.
    """
    for name in _pattern_field_candidates(kind):
        if isinstance(getattr(config, name, None), AllowDenyPattern):
            return name
    # Narrowed via an annotated local: passing `type(config)` inline infers as
    # type[Any], which mypy's lru_cache stub rejects as Hashable (a metaclass
    # __hash__ signature mismatch) even though it is hashable at runtime.
    config_cls: type = type(config)
    return pattern_field_for_config_class(config_cls, kind)


def _ordered_levels(levels: List[ProbeLevel]) -> List[ProbeLevel]:
    """Order levels by their declared parent edges, validating the shape.

    Today's traversal is a single chain: parent_path is a list of bare names, so
    two levels sharing a parent could not be told apart. Branching is therefore
    detected and rejected here rather than mis-dispatched at probe time.
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

    children: Dict[ProbeNodeKind, ProbeLevel] = {}
    for level in levels:
        if level.parent is None:
            continue
        if level.parent not in by_kind:
            raise ValueError(
                f"level '{level.kind}' declares unknown parent '{level.parent}'; "
                f"declared kinds are {[str(k) for k in by_kind]}"
            )
        if level.parent in children:
            raise ValueError(
                f"levels '{children[level.parent].kind}' and '{level.kind}' both "
                f"branch off '{level.parent}'. Branching hierarchies are not "
                f"supported yet: parent_path carries bare names, so siblings "
                f"cannot be told apart."
            )
        children[level.parent] = level

    ordered = [roots[0]]
    while ordered[-1].kind in children:
        ordered.append(children[ordered[-1].kind])
    if len(ordered) != len(levels):
        unreachable = [str(lvl.kind) for lvl in levels if lvl not in ordered]
        raise ValueError(
            f"levels unreachable from the root (orphaned or cyclic): {unreachable}"
        )
    return ordered


class ClientProbe:
    def __init__(
        self,
        client_factory: Callable[[Any], Any],
        levels: List[ProbeLevel],
        close: Callable[[Any], None] = lambda client: None,
    ) -> None:
        self._client_factory = client_factory
        self._levels = _ordered_levels(levels)
        self._close = close

    def hierarchy(self) -> List[ProbeNodeKind]:
        # Structural only — never touches the client, so it is connection-free.
        return [level.kind for level in self._levels]

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
            if pattern_field is None:
                if kind not in resolved_by_kind:
                    resolved_by_kind[kind] = pattern_field_for_config(config, kind)
                pattern_field = resolved_by_kind[kind]
                if pattern_field is None:
                    raise ValueError(
                        f"connector bug: {type(config).__name__} declares a "
                        f"probe level for kind '{kind}' but has no "
                        f"AllowDenyPattern field to filter it (looked for "
                        f"{_pattern_field_candidates(kind)}); the connector must "
                        f"set pattern_field= explicitly on that level/source"
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
            (
                name,
                level.kind_for(name) if level.kind_for else level.kind,
                level.pattern_field,
            )
            for name in level.list_names(client, config, parent_path)
        ]
        return self._resolved(config, items)

    def list_children(
        self, config: Any, parent_path: List[str], limit: int
    ) -> ProbeResult:
        # Past the declared depth there are no children (e.g. a flat source's leaf).
        if len(parent_path) >= len(self._levels):
            return ProbeResult(source_type="", supported=True, parent_path=parent_path)
        level = self._levels[len(parent_path)]
        client = self._client_factory(config)
        try:
            prefix = ".".join(parent_path)
            if (
                level.kind == ProbeLeafKind.COLUMN
                and level.sources is None
                and level.list_items is None
                and level.pattern_field is None
                and level.classify is None
            ):
                # Leaf (column) level: no filter, no verdict.
                assert level.list_names is not None
                names = list(level.list_names(client, config, parent_path))
                nodes, truncated = column_nodes(
                    [{"name": n} for n in names], limit, fqn_prefix=prefix
                )
            else:
                custom = level.classify
                on_fqn = level.classify_on_fqn

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
                                parent_path=tuple(parent_path),
                            )
                        )
                    return pattern_verdict(
                        config, pattern_field, node_fqn if on_fqn else name
                    )

                nodes, truncated = level_nodes(
                    self._items(level, client, config, parent_path),
                    limit,
                    prefix or None,
                    verdict_for,
                )
            return ProbeResult(
                source_type="",
                supported=True,
                parent_path=parent_path,
                nodes=nodes,
                truncated=truncated,
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
