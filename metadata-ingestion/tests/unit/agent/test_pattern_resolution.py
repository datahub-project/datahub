import re
from types import SimpleNamespace
from typing import Annotated, Any, Dict, List, Optional, Set, Tuple

import pytest
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent.introspect import (
    _pattern_field_for_config_class,
    is_pattern_field,
    pattern_field_for_config,
)
from datahub.ingestion.agent.probe import ClientProbe
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _Cfg(ConfigModel):
    schema_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    view_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    topic_patterns: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    # A same-named field that is NOT a pattern — must never be resolved to.
    table_pattern: str = "not-an-allow-deny-pattern"


def test_resolves_by_convention():
    assert (
        _pattern_field_for_config_class(_Cfg, DatasetContainerSubTypes.SCHEMA)
        == "schema_pattern"
    )
    assert _pattern_field_for_config_class(_Cfg, DatasetSubTypes.VIEW) == "view_pattern"


def test_resolves_the_plural_form():
    assert (
        _pattern_field_for_config_class(_Cfg, DatasetSubTypes.TOPIC) == "topic_patterns"
    )


def test_ignores_a_same_named_non_pattern_field():
    # `table_pattern: str` exists but is not an AllowDenyPattern.
    assert _pattern_field_for_config_class(_Cfg, DatasetSubTypes.TABLE) is None


def test_returns_none_when_absent():
    assert (
        _pattern_field_for_config_class(_Cfg, DatasetContainerSubTypes.DATABASE) is None
    )


def test_multiword_kind_collapses_to_underscores():
    class _K(ConfigModel):
        flink_job_pattern: AllowDenyPattern = Field(
            default=AllowDenyPattern.allow_all()
        )

    assert _pattern_field_for_config_class(_K, "Flink Job") == "flink_job_pattern"


_LIST_CHILDREN_IMPORT_RE = re.compile(
    r"from ([\w\.]+) import\s*\(?\s*(list_\w+_children)"
)
_LIST_CHILDREN_CALL_RE = re.compile(r"(\w+)\.list_children\(")

# Configs whose list_probe_children delegates to another source's connection
# object (e.g. `self.connection.list_probe_children(...)`) rather than owning a
# module-level ClientProbe of its own. Named explicitly so a *different* config
# silently falling out of the mapping below fails the test instead of quietly
# losing pattern-resolution coverage.
_EXPECTED_UNMAPPED_SOURCE_TYPES = {"bigquery-queries", "snowflake-queries"}


def _probe_capable_configs() -> Dict[str, Any]:
    """source_type -> config class, for every source whose config declares a probe."""
    # function-scoped: importing the source registry pulls in every connector
    # package, which must not happen at collection time for this module's other
    # (fast, fixture-only) unit tests.
    from datahub.ingestion.agent.probe import _config_class
    from datahub.ingestion.source.source_registry import source_registry

    configs = {}
    for source_type in source_registry.mapping:
        try:
            config_cls = _config_class(source_type)
        except Exception:
            continue
        if callable(getattr(config_cls, "probe_hierarchy", None)):
            configs[source_type] = config_cls
    return configs


def _mapped_probes() -> Tuple[Dict[str, Tuple[Any, "ClientProbe"]], List[str]]:
    """(source_type -> (config_cls, probe)) for every config whose
    list_probe_children maps to its own ClientProbe, plus the source_types
    that don't (see _mapped_probe)."""
    mapped = {}
    unmapped = []
    for source_type, config_cls in _probe_capable_configs().items():
        probe = _mapped_probe(config_cls)
        if probe is None:
            unmapped.append(source_type)
        else:
            mapped[source_type] = (config_cls, probe)
    return mapped, unmapped


def _mapped_probe(config_cls: Any) -> Optional[ClientProbe]:
    """Resolve the single ClientProbe a config's list_probe_children delegates
    to, by reading its source for the `from <probe_module> import
    list_x_children` delegation, then that wrapper function's own
    `SOME_PROBE.list_children(...)` call. Returns None when the config doesn't
    delegate this way (i.e. isn't backed by its own ClientProbe).

    Typed Any because config_cls is a dynamically resolved connector config
    class (see _config_class in probe.py); mypy can't see its
    list_probe_children classmethod.
    """
    import importlib
    import inspect

    try:
        src = inspect.getsource(config_cls.list_probe_children)
    except (OSError, TypeError):
        return None
    m = _LIST_CHILDREN_IMPORT_RE.search(src)
    if not m:
        return None
    mod = importlib.import_module(m.group(1))
    func = getattr(mod, m.group(2), None)
    if func is None:
        return None
    call_match = _LIST_CHILDREN_CALL_RE.search(inspect.getsource(func))
    if not call_match:
        return None
    probe = getattr(mod, call_match.group(1), None)
    return probe if isinstance(probe, ClientProbe) else None


def test_every_probe_level_resolves_to_a_real_pattern_field():
    """Drift net: every declared level/source on a probe-capable config's OWN
    ClientProbe must reach an AllowDenyPattern field on that same config,
    explicitly or by convention.

    Limitations, so this test's guarantee isn't overstated:
    - A `list_items` level's per-item kinds are only known once its lister runs
      against a live client (e.g. BigQuery's table listing, which yields both
      Table and View from one call). A static walk of the declaration can't see
      those kinds, so `list_items` levels are skipped here rather than checked
      against a placeholder kind. Harmless today: every `list_items` lister in
      the codebase sets an explicit `pattern_field` on each item it yields.
    - Two configs (`bigquery-queries`, `snowflake-queries`) delegate
      `list_probe_children` to another source's connection object instead of
      declaring their own ClientProbe; `_mapped_probe` can't attribute a probe
      to them, so they're named explicitly in
      `_EXPECTED_UNMAPPED_SOURCE_TYPES` and asserted below rather than silently
      skipped.
    - A probe module that fails to import is a real regression, not a benign
      skip; the discovery loop below asserts nothing was skipped.
    """
    import importlib
    import pkgutil

    import datahub.ingestion.source as source_pkg
    from datahub.ingestion.agent.models import ProbeLeafKind

    assert _probe_capable_configs(), "no probe-capable sources discovered"

    # A probe module that fails to import would silently vanish from the net;
    # fail loudly instead of skipping it.
    skipped_modules = []
    for mod_info in pkgutil.walk_packages(
        source_pkg.__path__, "datahub.ingestion.source."
    ):
        if not mod_info.name.endswith("_probe"):
            continue
        try:
            importlib.import_module(mod_info.name)
        except Exception as e:
            skipped_modules.append((mod_info.name, str(e)))
    assert not skipped_modules, f"probe modules failed to import: {skipped_modules}"

    mapped, unmapped = _mapped_probes()

    assert set(unmapped) == _EXPECTED_UNMAPPED_SOURCE_TYPES, (
        "the set of configs whose list_probe_children doesn't map to their own "
        f"ClientProbe changed: {sorted(unmapped)}. If a source now legitimately "
        "delegates elsewhere, add it to _EXPECTED_UNMAPPED_SOURCE_TYPES only "
        "after confirming its pattern resolution is covered some other way; if "
        "one dropped out, `_mapped_probe`'s source-parsing heuristic likely "
        "needs updating to keep tracking it."
    )

    unresolved = []
    for source_type, (config_cls, probe) in mapped.items():
        for level in probe._all_levels:
            if level.sources:
                entries = [(s.kind, s.pattern_field) for s in level.sources]
            elif level.list_items is not None:
                # Per-item kinds aren't visible statically — see docstring.
                continue
            else:
                entries = [(level.kind, level.pattern_field)]
            for kind, declared in entries:
                if declared is not None or kind == ProbeLeafKind.COLUMN:
                    continue
                if _pattern_field_for_config_class(config_cls, kind) is None:
                    unresolved.append((source_type, str(kind)))

    assert not unresolved, (
        "these (config, probe level) pairs declare no pattern_field and the "
        f"config's own conventional field doesn't resolve it: {unresolved}"
    )


def test_every_declared_hint_matches_a_level_kind_on_its_own_probe():
    """The inverse guard: a Filters hint naming the wrong kind is otherwise
    silent. _hinted_pattern_field simply returns None for a kind no field
    hints, resolution falls back to the name convention, and the level ends
    up filtered by a DIFFERENT AllowDenyPattern than the one the author
    intended -- with every other test still green, since neither
    test_no_probe_capable_config_declares_conflicting_hints (checks
    uniqueness, not existence) nor
    test_every_probe_level_resolves_to_a_real_pattern_field (only fires when
    the convention resolves to None) catches it. Every Filters hint on a
    probe-capable config must name a kind that config's OWN probe actually
    declares a level for.

    Limitation: a level's list_items can produce a kind that isn't the level's
    own declared `.kind` and isn't visible in this static walk (e.g. a single
    listing that yields both Table and View, like BigQuery's table listing).
    A hint that would otherwise look dead is not flagged when the same probe
    has such a level, since the missing kind may simply be one of its
    runtime-only ones.
    """
    mapped, _ = _mapped_probes()

    dead: List[Tuple[str, str]] = []
    for source_type, (config_cls, probe) in mapped.items():
        level_kinds: Set[str] = set()
        has_dynamic_kinds = False
        for level in probe._all_levels:
            if level.sources:
                level_kinds.update(str(s.kind) for s in level.sources)
            else:
                level_kinds.add(str(level.kind))
            if level.list_items is not None:
                has_dynamic_kinds = True

        hinted_kinds = {
            str(meta.kind)
            for field in config_cls.model_fields.values()
            for meta in field.metadata
            if isinstance(meta, Filters)
        }
        missing = hinted_kinds - level_kinds
        if missing and has_dynamic_kinds:
            continue
        dead.extend((source_type, kind) for kind in missing)

    assert not dead, (
        "these (source, kind) Filters hints are declared on a probe-capable "
        f"config but no level on that config's OWN probe has that kind: {dead}. "
        "Fix the Filters(...) kind to match a real level."
    )


def test_a_declared_hint_beats_the_name_convention():
    """The convention would find `table_pattern`; the hint must win."""

    class _Hinted(ConfigModel):
        table_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
        collection_pattern: Annotated[
            AllowDenyPattern, Filters(DatasetSubTypes.TABLE)
        ] = Field(default=AllowDenyPattern.allow_all())

    assert (
        _pattern_field_for_config_class(_Hinted, DatasetSubTypes.TABLE)
        == "collection_pattern"
    )


def test_a_hint_does_not_remove_the_field_from_convention_matching():
    """A hint adds a binding; `dataset_pattern` still answers a Dataset lookup."""

    class _BigIdLike(ConfigModel):
        dataset_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = (
            Field(default=AllowDenyPattern.allow_all())
        )

    assert (
        _pattern_field_for_config_class(_BigIdLike, DatasetSubTypes.TABLE)
        == "dataset_pattern"
    )
    assert _pattern_field_for_config_class(_BigIdLike, "Dataset") == "dataset_pattern"


def test_a_field_can_hint_more_than_one_kind():
    """One field can filter two kinds (Salesforce's object_pattern filters
    both standard and custom objects) by stacking a Filters per kind."""

    class _SalesforceLike(ConfigModel):
        object_pattern: Annotated[
            AllowDenyPattern,
            Filters(DatasetSubTypes.SALESFORCE_STANDARD_OBJECT),
            Filters(DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT),
        ] = Field(default=AllowDenyPattern.allow_all())

    assert (
        _pattern_field_for_config_class(
            _SalesforceLike, DatasetSubTypes.SALESFORCE_STANDARD_OBJECT
        )
        == "object_pattern"
    )
    assert (
        _pattern_field_for_config_class(
            _SalesforceLike, DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT
        )
        == "object_pattern"
    )


def test_two_fields_hinting_the_same_kind_raise_naming_both():
    class _Conflict(ConfigModel):
        a_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
            default=AllowDenyPattern.allow_all()
        )
        b_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
            default=AllowDenyPattern.allow_all()
        )

    with pytest.raises(ValueError, match="a_pattern.*b_pattern|b_pattern.*a_pattern"):
        _pattern_field_for_config_class(_Conflict, DatasetSubTypes.TABLE)


def test_a_hint_on_a_non_pattern_field_raises():
    class _Bad(ConfigModel):
        thing: Annotated[str, Filters(DatasetSubTypes.TABLE)] = "nope"

    with pytest.raises(ValueError, match="not an AllowDenyPattern"):
        _pattern_field_for_config_class(_Bad, DatasetSubTypes.TABLE)


def test_annotating_a_field_keeps_it_recognised_as_a_pattern_field():
    """Guards the one way this change could silently break describe/scaffold."""

    class _Ann(ConfigModel):
        p: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
            default=AllowDenyPattern.allow_all()
        )

    assert is_pattern_field(_Ann.model_fields["p"].annotation)


def test_the_hint_wins_over_an_instance_level_convention_match():
    """pattern_field_for_config checks live attributes; the hint still wins."""

    class _Hinted(ConfigModel):
        table_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
        collection_pattern: Annotated[
            AllowDenyPattern, Filters(DatasetSubTypes.TABLE)
        ] = Field(default=AllowDenyPattern.allow_all())

    assert (
        pattern_field_for_config(_Hinted(), DatasetSubTypes.TABLE)
        == "collection_pattern"
    )


def test_an_object_without_model_fields_still_resolves_by_convention():
    """Test fixtures are plain SimpleNamespaces — they have no hints."""
    cfg = SimpleNamespace(schema_pattern=AllowDenyPattern.allow_all())
    assert (
        pattern_field_for_config(cfg, DatasetContainerSubTypes.SCHEMA)
        == "schema_pattern"
    )


def test_no_probe_capable_config_declares_conflicting_hints():
    """A second field claiming a kind would make resolution ambiguous, and the
    probe would silently filter on the wrong pattern."""
    # function-scoped: walking every connector package is expensive and must not
    # run at collection time for the rest of this module's fast unit tests.
    import importlib
    import pkgutil
    from collections import defaultdict

    import datahub.ingestion.source as srcpkg
    from datahub.configuration.common import Filters

    conflicts = {}
    for mod in pkgutil.walk_packages(srcpkg.__path__, srcpkg.__name__ + "."):
        try:
            module = importlib.import_module(mod.name)
        except Exception:
            continue  # optional connector dependency not installed
        for obj in vars(module).values():
            if not (isinstance(obj, type) and hasattr(obj, "model_fields")):
                continue
            by_kind = defaultdict(list)
            for fname, field in obj.model_fields.items():
                for meta in field.metadata:
                    if isinstance(meta, Filters):
                        by_kind[str(meta.kind)].append(fname)
            for kind, names in by_kind.items():
                if len(names) > 1:
                    conflicts[f"{obj.__name__}:{kind}"] = sorted(names)
    assert not conflicts, conflicts
