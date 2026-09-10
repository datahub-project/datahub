"""Contract checks that hold across every connector's probe, not just one.

The framework enforces what a getter *declares* (probe_methods._enforce_gates),
which leaves one thing unenforceable from inside: a getter that declares nothing.
`@probe_method def query(self, sql: str)` runs completely unchecked and is still
advertised by `probe methods`. Nothing at decoration time can tell that parameter
apart from a harmless one -- only the author knows, and until this file existed
nothing verified that they had thought about it.

These are tripwires, not boundaries. A name-based rule is defeatable by renaming
a parameter, which is exactly why it belongs here (visible, greppable, arguable
in review) rather than as a hard import-time failure that an author works around.
Each rule below is proved to fire against a deliberately-bad provider, because a
lint whose failure path is never exercised is a lint nobody can trust.
"""

from typing import Dict, List, Set, Tuple

import pytest

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    ProbeProvider,
    _iter_specs,
    _provider_class,
    config_class_for,
    probe_method,
)
from datahub.ingestion.source.source_registry import source_registry

# Parameter names that carry something a connector hands to an interpreter, a
# filesystem or the network. A getter taking one of these must declare the
# matching gate so the framework checks it first.
_SQLISH_PARAMS = frozenset({"query", "sql", "statement", "ddl", "script", "expression"})
_PATHISH_PARAMS = frozenset({"path", "url", "uri", "endpoint", "route"})

# Any parameter literally named `limit` bounds how much comes back, so it must be
# declared for the framework to clamp it (probe_methods._bounded_kwargs).
_LIMIT_PARAM = "limit"

# Sources whose probe support is expected to exist, asserted separately so this
# file cannot pass by scanning nothing. Both need only core dependencies, so they
# load in any environment that can run the unit suite at all.
_MUST_BE_SCANNED = ("postgres", "mysql")


def _spec_of(fn: object) -> ProbeMethodSpec:
    spec = getattr(fn, "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    return spec


def _violations(spec: ProbeMethodSpec) -> List[str]:
    """What this method takes without declaring how it should be checked."""
    found: List[str] = []
    for param in spec.params:
        name = param.name.lower()
        if name in _SQLISH_PARAMS and spec.scoped_sql_param != param.name:
            found.append(f"'{param.name}' looks like SQL but no scoped_sql_param")
        if name in _PATHISH_PARAMS and spec.scoped_path_param != param.name:
            found.append(f"'{param.name}' looks like a path but no scoped_path_param")
        if name == _LIMIT_PARAM and spec.row_limit_param != param.name:
            found.append(f"'{param.name}' bounds output but no row_limit_param")
    return found


def _scan() -> Tuple[Dict[str, List[str]], Set[str], List[str]]:
    """Walk every registered source's probe provider.

    Returns findings keyed by "source.command", the sources actually scanned, and
    the sources that could not be loaded -- the last of those is returned rather
    than swallowed so a shrinking scan shows up instead of looking like success.
    """
    findings: Dict[str, List[str]] = {}
    scanned: Set[str] = set()
    unloadable: List[str] = []
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception as exc:
            unloadable.append(f"{source_type}: {type(exc).__name__}")
            continue
        if provider_cls is None:
            continue
        scanned.add(source_type)
        for command, spec in _iter_specs(provider_cls):
            problems = _violations(spec)
            if problems:
                findings[f"{source_type}.{command}"] = problems
    return findings, scanned, unloadable


def test_no_probe_method_takes_a_dangerous_parameter_without_declaring_a_gate():
    findings, _, _ = _scan()
    assert findings == {}, (
        "these probe methods take a parameter the framework cannot check, because "
        "the method never declared it -- add the matching scoped_sql_param / "
        f"scoped_path_param / row_limit_param: {findings}"
    )


def test_the_scan_actually_reached_providers():
    # Guards the test above against passing vacuously: if plugin loading breaks,
    # _scan() finds nothing to check and every rule here trivially holds.
    _, scanned, unloadable = _scan()
    missing = [s for s in _MUST_BE_SCANNED if s not in scanned]
    assert not missing, (
        f"expected probe support on {missing} but the scan did not reach it; "
        f"unloadable sources: {unloadable}"
    )


def test_every_advertised_provider_satisfies_the_provider_protocol():
    """`probe methods` describes a provider; `probe run` builds and invokes it.

    Both go through the single class `probe_provider_class()` returns, so they cannot
    name different things -- the Snowflake/BigQuery bug, where discovery inherited the
    SQLAlchemy answer while execution used the connector's own client, is
    unrepresentable rather than merely tested for. What is still worth checking is
    that the class it names is usable.

    Checked with issubclass against ProbeProvider rather than by listing members here:
    the Protocol is where the contract is written, and a test that restated it would
    be a second copy to drift from. It covers for_config (constructible from a
    recipe), __enter__ and __exit__ (so whatever it opened gets closed).
    """
    broken: Dict[str, str] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception:
            continue  # covered by test_the_scan_actually_reached_providers
        if provider_cls is None:
            continue
        if not issubclass(provider_cls, ProbeProvider):
            missing = [
                member
                for member in ("for_config", "__enter__", "__exit__")
                if not hasattr(provider_cls, member)
            ]
            broken[source_type] = f"{provider_cls.__name__} lacks {missing}"
        elif not _iter_specs(provider_cls):
            broken[source_type] = f"{provider_cls.__name__} declares no probe methods"
    assert broken == {}, broken


def test_the_protocol_rejects_a_provider_that_cannot_be_built_or_closed():
    # Proving the check above can fail: it is the only thing standing between a
    # half-written provider and a `probe run` that dies inside the with-block.
    class NoBuilder:
        def __enter__(self) -> "NoBuilder":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    class NoExit:
        @classmethod
        def for_config(cls, config: object) -> "NoExit":
            return cls()

        def __enter__(self) -> "NoExit":
            return self

    assert not issubclass(NoBuilder, ProbeProvider)
    assert not issubclass(NoExit, ProbeProvider)


def test_a_provider_taking_sql_declares_the_dialect_it_will_be_parsed_as():
    """The gate refuses a query outright when the provider has no sql_dialect.

    That is the right runtime behaviour, but it only surfaces when someone runs a
    query. Checked here so a connector that adds a `sql` command and forgets the
    dialect fails in CI rather than on a customer's first probe.
    """
    missing: Dict[str, str] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception:
            continue
        if provider_cls is None:
            continue
        for command, spec in _iter_specs(provider_cls):
            if spec.scoped_sql_param and not hasattr(provider_cls, "sql_dialect"):
                missing[f"{source_type}.{command}"] = (
                    f"{provider_cls.__name__} takes SQL but declares no sql_dialect"
                )
    assert missing == {}, missing


# Every hook the framework reads off a config by name. A method that looks like one
# of these but is not exactly one is the failure this list exists to catch.
_CONFIG_HOOKS = frozenset(
    {
        "probe_provider_class",
        "probe_catalog_scope",
        "probe_container_kind",
        "probe_match_target",
        "probe_filter_target",
        "probe_schema_verdict_override",
        "probe_prepare_engine",
    }
)


def test_no_config_declares_a_probe_hook_the_framework_will_never_read():
    """A misspelled verdict hook is silence, and silence here is a wrong answer.

    The framework resolves these by name (`getattr(config, "probe_match_target")`),
    so `probe_match_targets` does not fail -- it is simply never called, the probe
    falls back to matching on the bare name, and it reports a verdict the
    connector's ingestion does not make. Nothing else in the stack can notice.

    Only covers the `probe_`-prefixed hooks. `default_schemas` and
    `default_databases` have base definitions on SQLCommonConfig, so a typo there
    shadows nothing and is not detectable this way; assert those against ingestion
    instead.
    """
    unknown: Dict[str, List[str]] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            config_cls = config_class_for(source_type)
        except Exception:
            continue
        if config_cls is None:
            continue
        for klass in config_cls.__mro__:
            suspects = [
                name
                for name in vars(klass)
                if name.startswith("probe_") and name not in _CONFIG_HOOKS
            ]
            if suspects:
                unknown[klass.__name__] = sorted(suspects)
    assert unknown == {}, (
        "these look like probe hooks but the framework reads none of them, so they "
        f"do nothing; expected one of {sorted(_CONFIG_HOOKS)}: {unknown}"
    )


def test_the_tripwire_fires_on_an_undeclared_query_parameter():
    class Forgetful:
        @probe_method(name="query")
        def query(self, sql: str) -> Dict[str, object]:
            """Run a query, gating nothing."""
            return {}

    spec = _spec_of(Forgetful.query)
    assert _violations(spec) == ["'sql' looks like SQL but no scoped_sql_param"]


def test_the_tripwire_fires_on_an_undeclared_path_parameter():
    class Forgetful:
        @probe_method(name="fetch")
        def fetch(self, path: str) -> Dict[str, object]:
            """Fetch a path, gating nothing."""
            return {}

    spec = _spec_of(Forgetful.fetch)
    assert _violations(spec) == ["'path' looks like a path but no scoped_path_param"]


def test_the_tripwire_fires_on_an_undeclared_limit():
    class Forgetful:
        @probe_method(name="things")
        def things(self, limit: int = 500) -> List[str]:
            """List things, unbounded."""
            return []

    spec = _spec_of(Forgetful.things)
    assert _violations(spec) == ["'limit' bounds output but no row_limit_param"]


def test_a_declared_parameter_is_not_flagged():
    class Careful:
        @probe_method(name="sql", scoped_sql_param="query", row_limit_param="limit")
        def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
            """Run a catalog query."""
            return {}

    assert _violations(_spec_of(Careful.sql)) == []


def test_two_methods_cannot_declare_the_same_command():
    """A duplicate command name was a gate bypass, not a cosmetic clash.

    _iter_specs kept whichever spec dir() yielded last while _bound_method
    returned the first matching attribute, so _enforce_gates could check one
    declaration and run_probe_method invoke a different method. With `sql`
    declared twice -- once with scoped_sql_param, once without -- the ungated
    spec won the check and the gated method ran the query, so the query
    executed with no scope check at all.
    """

    class _Clashing:
        @probe_method(name="sql", scoped_sql_param="query")
        def a_sql(self, query: str) -> object:
            """Gated: the framework scope-checks query."""
            return query

        @probe_method(name="sql")
        def z_sql(self, query: str) -> object:
            """Ungated: declares no scoped_sql_param."""
            return query

    with pytest.raises(ValueError, match="two different methods"):
        _iter_specs(_Clashing)


def test_overriding_an_inherited_command_is_still_allowed():
    """The refusal above must not break the normal way to specialise a command:
    redefine the same attribute name, which dir() yields once."""

    class _Base:
        @probe_method(name="thing")
        def thing(self) -> object:
            """Base implementation."""
            return "base"

    class _Sub(_Base):
        @probe_method(name="thing", row_limit_param="limit")
        def thing(self, limit: int = 10) -> object:
            """Overridden, same attribute name."""
            return "sub"

    specs = dict(_iter_specs(_Sub))
    assert sorted(specs) == ["thing"]
    assert specs["thing"].row_limit_param == "limit"


# --- every kind an agent can ask about must resolve, and both commands must
# --- agree about it ------------------------------------------------------------


def _probe_capable_configs():
    """(source_type, config_cls) for every source that declares any probe kind."""
    from datahub.ingestion.agent.introspect import declared_kinds_for_class

    out = []
    for source_type in sorted(source_registry.mapping):
        try:
            source_cls = source_registry.get(source_type)
            get_config_class = getattr(source_cls, "get_config_class", None)
            if get_config_class is None:
                continue
            config_cls = get_config_class()
        except Exception:
            # An uninstalled extra is not this test's business.
            continue
        if not getattr(config_cls, "model_fields", None):
            continue
        try:
            if declared_kinds_for_class(source_type, config_cls):
                out.append((source_type, config_cls))
        except Exception:
            continue
    return out


def test_which_declared_kinds_resolve_to_no_filter_field():
    """A kind an agent can ask about that resolves to no pattern field.

    `probe filter --kind X` then answers "everything included" for X. That is
    correct when the source genuinely filters nothing at that level -- Mode has
    no dataset or query filter -- and a confidently wrong verdict when the
    annotation merely rotted away, which is what happened to Teradata's
    database_pattern. **The two are indistinguishable from here**: UNFILTERED is
    a framework sentinel and no connector can declare it, so "deliberately
    unfiltered" and "the annotation evaporated" produce the same silence.

    Until a connector can say which it means, this list is the difference. Every
    entry has been looked at once; a new one cannot appear without someone
    adding it here and deciding which case it is.
    """
    from datahub.ingestion.agent.introspect import (
        _pattern_field_for_config_class,
        declared_kinds_for_class,
    )

    # Mode filters spaces and reports, and nothing below them -- there is no
    # dataset or query pattern to resolve to, deliberately.
    known_unfiltered = {"mode": ["Dataset", "Query"]}

    unresolved: Dict[str, List[str]] = {}
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        for kind in sorted(declared_kinds_for_class(source_type, config_cls)):
            checked += 1
            if _pattern_field_for_config_class(config_cls, kind) is None:
                unresolved.setdefault(source_type, []).append(kind)

    assert unresolved == known_unfiltered, (
        "the set of declared kinds with no filter field changed.\n"
        f"  now:      {unresolved}\n"
        f"  expected: {known_unfiltered}\n"
        "A new entry is either a level the source really does not filter (add it "
        "here) or an annotation that got dropped -- pydantic v2 replaces the "
        "annotation when a subclass redeclares an inherited field, which is how "
        "Teradata lost Filters(Database) without anything failing."
    )
    assert checked > 20, f"only {checked} (source, kind) pairs reached"


def test_describe_and_probe_filter_agree_about_every_field():
    """The two halves of the same feature must not contradict each other.

    They did: `describe` read only the explicit Filters(...) annotation while
    `probe filter` resolved through the annotation *and then* the name
    convention. Teradata redeclares database_pattern, pydantic v2 drops the
    inherited annotation, and the two commands then gave opposite answers about
    the same field -- describe reporting no filter, probe filter excluding DBC
    by it. From outside there is no way to tell which is lying.
    """
    from datahub.ingestion.agent.introspect import (
        _filter_kinds_by_field,
        _pattern_field_for_config_class,
        describe_source,
    )

    disagreements = []
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        try:
            spec = describe_source(source_type)
        except Exception:
            continue
        described = {f.name: f.filters for f in spec.fields if f.filters}
        for field, kind in _filter_kinds_by_field(source_type, config_cls).items():
            checked += 1
            if described.get(field) != kind:
                disagreements.append(
                    f"{source_type}.{field}: probe filter resolves kind {kind!r}, "
                    f"describe reports {described.get(field)!r}"
                )
        # And the reverse direction: nothing described as filtering a kind that
        # probe filter would resolve to a different field.
        for field, kind in described.items():
            resolved = _pattern_field_for_config_class(config_cls, kind)
            if resolved is not None and resolved != field:
                disagreements.append(
                    f"{source_type}: describe says {field} filters {kind!r}, "
                    f"but probe filter would use {resolved}"
                )

    assert not disagreements, "describe and probe filter disagree:\n  " + "\n  ".join(
        disagreements
    )
    assert checked > 20, f"only {checked} fields reached"


def test_which_connectors_still_lean_on_the_name_convention():
    """Tracks the gap the annotation was meant to close, so it cannot widen unseen.

    `Filters(...)` looks like the mechanism; for these fields the `<kind>_pattern`
    name convention is the mechanism. Every entry is a config that redeclares an
    inherited annotated field -- pydantic v2 replaces the annotation wholesale,
    so restating the field drops it silently.

    This is a ratchet, not an approval: a new connector may not join the list
    without someone editing it, and the list should shrink. It is deliberately
    not a requirement that the list be empty, because emptying it means editing
    a dozen connectors and that is its own change.
    """
    from datahub.ingestion.agent.introspect import (
        _declared_filter_kind,
        _filter_kinds_by_field,
    )

    known = {
        # bigquery was here until dataset_pattern gained its Filters(...)
        # annotation. Its entry was not a harmless naming gap: the convention
        # resolved Schema to the deprecated schema_pattern alias, which is
        # allow-all unless set, so every dataset read as included while
        # ingestion filtered on dataset_pattern. The list shrinking is the
        # point of the ratchet.
        "cockroachdb": ["schema_pattern"],
        "druid": ["schema_pattern"],
        "hana": ["schema_pattern"],
        "starrocks": ["schema_pattern"],
        "unity-catalog": ["schema_pattern", "table_pattern"],
    }

    actual = {}
    for source_type, config_cls in _probe_capable_configs():
        explicit = {
            name
            for name, info in config_cls.model_fields.items()
            if _declared_filter_kind(info) is not None
        }
        by_convention = sorted(
            set(_filter_kinds_by_field(source_type, config_cls)) - explicit
        )
        if by_convention:
            actual[source_type] = by_convention

    assert actual == known, (
        "the set of fields resolved by name convention changed.\n"
        f"  now:      {actual}\n"
        f"  expected: {known}\n"
        "If you added one, annotate the field with Filters(...) instead. If you "
        "removed one, delete it from `known` here."
    )
