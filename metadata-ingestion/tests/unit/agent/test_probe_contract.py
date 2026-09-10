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
    # `unloadable` was computed and only ever interpolated into the message
    # above, so the scan could lose most of its providers with every guard
    # still green. probe_provider_class imports the provider module lazily, so
    # a provider whose import breaks leaves the *config* loadable -- the
    # `checked > 20` guards elsewhere keep passing while the tripwire below
    # inspects a fraction of the specs it claims to.
    assert unloadable == [], (
        f"{len(unloadable)} providers could not be loaded, so the gate scan "
        f"silently skipped them: {unloadable}"
    )
    assert len(scanned) >= 25, (
        f"only {len(scanned)} providers scanned; the tripwire is inspecting "
        "fewer sources than it should"
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
        "probe_unfiltered_kinds",
        "probe_schema_needs_parent",
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


def test_every_declared_kind_either_filters_or_says_it_does_not():
    """No kind may resolve to nothing by accident.

    `probe filter --kind X` reports every name included when it can find no
    pattern field, and that is the right answer when the source genuinely
    filters nothing at that level -- Mode filters spaces and reports, and
    nothing below them. It is a confidently wrong answer when a filter exists
    and its annotation was dropped, which is what happened to Teradata's
    database_pattern: pydantic v2 replaces the annotation when a subclass
    redeclares the field.

    The two were indistinguishable, so this was a hand-maintained list of which
    was which. A source can now say `probe_unfiltered_kinds()`, so the list is
    replaced by the rule it was standing in for.
    """
    from datahub.ingestion.agent.introspect import (
        _pattern_field_for_config_class,
        declared_kinds_for_class,
        declared_unfiltered_kinds,
    )

    silent: Dict[str, List[str]] = {}
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        unfiltered = declared_unfiltered_kinds(config_cls)
        for kind in sorted(declared_kinds_for_class(source_type, config_cls)):
            checked += 1
            if kind in unfiltered:
                continue
            if _pattern_field_for_config_class(config_cls, kind) is None:
                silent.setdefault(source_type, []).append(kind)

    assert not silent, (
        "these kinds resolve to no filter field and are not declared "
        f"unfiltered:\n  {silent}\n"
        "Either the field lost its Filters(...) annotation -- pydantic v2 drops "
        "it when a subclass redeclares an inherited field -- or the source "
        "really does not filter that level, in which case say so with "
        "probe_unfiltered_kinds()."
    )
    assert checked > 20, f"only {checked} (source, kind) pairs reached"


def test_a_source_cannot_both_declare_a_kind_unfiltered_and_filter_it():
    """Declaring "nothing filters this" while holding a field the resolver
    would find is a contradiction, and resolving it silently is how the two
    halves of this feature came to disagree in the first place."""
    from datahub.ingestion.agent.introspect import (
        _pattern_field_for_config_class,
        declared_unfiltered_kinds,
    )

    contradictions = []
    for source_type, config_cls in _probe_capable_configs():
        for kind in sorted(declared_unfiltered_kinds(config_cls)):
            field = _pattern_field_for_config_class(config_cls, kind)
            if field is not None:
                contradictions.append(
                    f"{source_type}: declares {kind!r} unfiltered but "
                    f"{field} would filter it"
                )
    assert not contradictions, "\n  ".join(contradictions)


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


def test_no_connector_leans_on_the_name_convention():
    """Every kind an agent can ask about resolves through an explicit
    Filters(...), not through the `<kind>_pattern` name guess.

    The guess is still there as a net, because a connector this test cannot see
    is better served by a correct guess than by silence -- resolving to nothing
    makes `probe filter` report every object included, which is a confidently
    wrong answer rather than a missing one. But nothing may *depend* on it: it
    is not a documented contract, and it silently covered for six connectors
    that had dropped their inherited annotation by redeclaring the field, which
    pydantic v2 replaces wholesale.

    BigQuery is why this is a hard assertion rather than a list. Its guess
    resolved to `schema_pattern`, a hidden deprecated alias that is allow-all
    unless set, so `probe filter --kind Schema` reported every dataset included
    while ingestion filtered on `dataset_pattern` and dropped them.
    """
    from datahub.ingestion.agent.introspect import (
        _declared_filter_kind,
        _filter_kinds_by_field,
    )

    leaning = {}
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        checked += 1
        explicit = {
            name
            for name, info in config_cls.model_fields.items()
            if _declared_filter_kind(info) is not None
        }
        by_convention = sorted(
            set(_filter_kinds_by_field(source_type, config_cls)) - explicit
        )
        if by_convention:
            leaning[source_type] = by_convention

    assert leaning == {}, (
        "these fields resolve only by the name convention:\n"
        f"  {leaning}\n"
        "Annotate each with Filters(...). Check first that it is the field "
        "ingestion actually filters on -- BigQuery's guess found a deprecated "
        "alias, and annotating that would have made the wrong field permanent."
    )
    assert checked > 20, f"only {checked} probe-capable configs reached"


def test_no_config_declares_a_catalog_scope_its_provider_overrides():
    """Scope can be declared in two places, and the provider's wins.

    That is not drift to be tidied away: SnowflakeSummaryConfig is not a
    SQLCommonConfig and cannot carry probe_catalog_scope, so the provider
    attribute is the only place covering both Snowflake sources. Moving the
    declaration onto the config would narrow snowflake-summary to
    information_schema without a word.

    What must not happen is *both*. A config that overrides probe_catalog_scope
    while its provider sets catalog_scope has written dead code that reads as
    live -- I added exactly that to snowflake_config.py, verified it correct in
    isolation, and it did nothing; the only reason it surfaced was the CLI
    reporting a relation count that did not match.
    """
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    conflicts = []
    checked = 0
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception:
            continue
        if provider_cls is None or "catalog_scope" not in vars(provider_cls):
            continue
        checked += 1
        config_cls = config_class_for(source_type)
        own = getattr(config_cls, "probe_catalog_scope", None)
        base = getattr(SQLCommonConfig, "probe_catalog_scope", None)
        if own is not None and base is not None and own.__func__ is not base.__func__:
            conflicts.append(
                f"{source_type}: {config_cls.__name__}.probe_catalog_scope is "
                f"ignored because {provider_cls.__name__} sets catalog_scope"
            )

    assert not conflicts, (
        "these configs declare a catalog scope nothing reads:\n  "
        + "\n  ".join(conflicts)
        + "\nDeclare it on the provider, or remove the provider's attribute."
    )
    assert checked >= 2, (
        f"only {checked} providers declare catalog_scope; expected at least the "
        "Snowflake and BigQuery ones, so this test is not scanning nothing"
    )


def test_every_config_hook_matches_the_signature_the_framework_calls():
    """Hooks are resolved by getattr, so mypy cannot see a signature drift.

    That is not hypothetical: widening probe_schema_verdict_override with a
    parent_path kwarg updated two implementations and left SQLCommonConfig's
    base behind, breaking `probe filter --kind Schema` on every other SQL
    source. The name-only check above passed throughout, because the name was
    never the problem.
    """
    import inspect

    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    # Keyword arguments the framework passes, per hook. A hook must accept
    # every one of these -- by name, since every call site uses keywords.
    required_kwargs = {
        "probe_schema_verdict_override": {"schema", "parent_path"},
    }

    problems = []
    checked = 0
    for hook, kwargs in required_kwargs.items():
        implementers = [SQLCommonConfig]
        for source_type in sorted(source_registry.mapping):
            try:
                config_cls = config_class_for(source_type)
            except Exception:
                continue
            # Walk the MRO: BigQuery defines this on BigQueryFilterConfig, not
            # on the class the registry returns, so a __dict__ check misses it.
            for klass in config_cls.__mro__:
                if hook in klass.__dict__ and klass not in implementers:
                    implementers.append(klass)

        for cls in implementers:
            fn = getattr(cls, hook, None)
            if fn is None:
                continue
            checked += 1
            accepted = set(inspect.signature(fn).parameters) - {"self", "cls"}
            missing = kwargs - accepted
            if missing:
                problems.append(
                    f"{cls.__name__}.{hook} does not accept {sorted(missing)}; "
                    f"the framework calls it with {sorted(kwargs)}"
                )

    assert not problems, "\n  ".join(problems)
    assert checked >= 3, (
        f"only {checked} implementations checked; expected the base plus the "
        "Redshift and BigQuery overrides"
    )
