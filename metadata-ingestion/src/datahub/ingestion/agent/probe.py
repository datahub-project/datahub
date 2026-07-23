from dataclasses import dataclass
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

from datahub.ingestion.agent.models import (
    ProbeLeafKind,
    ProbeNode,
    ProbeNodeKind,
    ProbeResult,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
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
# classify(name, fqn) for containers; classify(name, fqn, is_view) for tables.
ContainerClassifier = Callable[[str, str], Verdict]
TableClassifier = Callable[[str, str, bool], Verdict]

_INCLUDED: Verdict = (True, None)


def fqn(prefix: Optional[str], name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


def container_nodes(
    names: Sequence[str],
    limit: int,
    kind: ProbeNodeKind,
    pattern_field: str,
    fqn_prefix: Optional[str] = None,
    classify: Optional[ContainerClassifier] = None,
    kind_for: Optional[Callable[[str], ProbeNodeKind]] = None,
) -> Tuple[List[ProbeNode], bool]:
    # fqn_prefix carries the parent container for 3-level sources (Snowflake
    # database, BigQuery project), so a schema/dataset fqn is PARENT.CHILD — the
    # form its pattern matches under match_fully_qualified_names. For 2-level
    # sources it stays bare. kind_for lets a single level carry per-node kinds
    # (e.g. Salesforce custom vs standard objects distinguished by name).
    nodes: List[ProbeNode] = []
    for n in names[:limit]:
        node_fqn = fqn(fqn_prefix, n)
        included, excluded_by = classify(n, node_fqn) if classify else _INCLUDED
        node_kind = kind_for(n) if kind_for else kind
        nodes.append(
            ProbeNode(n, node_kind, node_fqn, pattern_field, included, excluded_by)
        )
    return nodes, len(names) > limit


def table_nodes(
    tables: Sequence[str],
    views: Sequence[str],
    limit: int,
    fqn_prefix: str,
    classify: Optional[TableClassifier] = None,
) -> Tuple[List[ProbeNode], bool]:
    view_names = set(views)
    table_names = set(tables)
    # Table listings usually exclude views, so merge the two (tables first) to get
    # the full set of children without duplicating a name reported in both.
    combined = list(tables) + [v for v in views if v not in table_names]
    nodes: List[ProbeNode] = []
    for name in combined[:limit]:
        is_view = name in view_names
        kind = DatasetSubTypes.VIEW if is_view else DatasetSubTypes.TABLE
        pattern = "view_pattern" if is_view else "table_pattern"
        node_fqn = f"{fqn_prefix}.{name}"
        included, excluded_by = (
            classify(name, node_fqn, is_view) if classify else _INCLUDED
        )
        nodes.append(ProbeNode(name, kind, node_fqn, pattern, included, excluded_by))
    return nodes, len(combined) > limit


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


@dataclass
class ProbeLevel:
    kind: ProbeNodeKind
    # The config *_pattern field that filters this level, or None for a leaf
    # (column) level, which carries no filter.
    pattern_field: Optional[str]
    list_names: LevelLister
    # Optional: derive the node kind per-name when a level mixes kinds (e.g.
    # Salesforce custom vs standard objects). `kind` stays the nominal/structural
    # kind reported by hierarchy().
    kind_for: Optional[Callable[[str], ProbeNodeKind]] = None
    # Some connectors filter on the fully-qualified name (PARENT.CHILD), not the
    # bare child name (e.g. BigID's dataset_pattern). Set to test the pattern
    # against the node fqn instead of its name.
    classify_on_fqn: bool = False


class ClientProbe:
    def __init__(
        self,
        client_factory: Callable[[Any], Any],
        levels: List[ProbeLevel],
        close: Callable[[Any], None] = lambda client: None,
    ) -> None:
        self._client_factory = client_factory
        self._levels = levels
        self._close = close

    def hierarchy(self) -> List[ProbeNodeKind]:
        # Structural only — never touches the client, so it is connection-free.
        return [level.kind for level in self._levels]

    def list_children(
        self, config: Any, parent_path: List[str], limit: int
    ) -> ProbeResult:
        # Past the declared depth there are no children (e.g. a flat source's leaf).
        if len(parent_path) >= len(self._levels):
            return ProbeResult(source_type="", supported=True, parent_path=parent_path)
        level = self._levels[len(parent_path)]
        client = self._client_factory(config)
        try:
            names = list(level.list_names(client, config, parent_path))
            prefix = ".".join(parent_path)
            if level.pattern_field is None:
                nodes, truncated = column_nodes(
                    [{"name": n} for n in names], limit, fqn_prefix=prefix
                )
            else:
                pattern = getattr(config, level.pattern_field)
                field = level.pattern_field
                on_fqn = level.classify_on_fqn

                def classify(name: str, node_fqn: str) -> Verdict:
                    target = node_fqn if on_fqn else name
                    return (True, None) if pattern.allowed(target) else (False, field)

                nodes, truncated = container_nodes(
                    names,
                    limit,
                    level.kind,
                    field,
                    fqn_prefix=prefix or None,
                    classify=classify,
                    kind_for=level.kind_for,
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
