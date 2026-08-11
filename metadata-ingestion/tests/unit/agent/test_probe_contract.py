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

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
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


def test_every_advertised_provider_can_actually_be_built_and_has_commands():
    """`probe methods` describes a provider; `probe run` builds and invokes it.

    Both now go through the single class `probe_provider_class()` returns, so they
    cannot name different things -- the Snowflake/BigQuery bug, where discovery
    inherited the SQLAlchemy answer while execution used the connector's own
    client, is unrepresentable rather than merely tested for. What is still worth
    checking is that the class it names is usable: constructible from a config,
    and carrying at least one command to run.
    """
    broken: Dict[str, str] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception:
            continue  # covered by test_the_scan_actually_reached_providers
        if provider_cls is None:
            continue
        if not callable(getattr(provider_cls, "for_config", None)):
            broken[source_type] = (
                f"{provider_cls.__name__} has no for_config(config) classmethod"
            )
        elif not _iter_specs(provider_cls):
            broken[source_type] = f"{provider_cls.__name__} declares no probe methods"
    assert broken == {}, broken


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


def test_every_provider_is_a_context_manager():
    """__exit__ is where a probe's connection gets closed.

    SqlCatalogPassthrough supplies __enter__ but deliberately not __exit__, since
    what closing means differs -- dispose a pool, close a connection, close a
    client. A provider that inherits the first and forgets the second fails only
    when `probe run` opens the `with` block, which is after a connection exists.
    """
    incomplete: Dict[str, str] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception:
            continue
        if provider_cls is None:
            continue
        missing = [m for m in ("__enter__", "__exit__") if not hasattr(provider_cls, m)]
        if missing:
            incomplete[source_type] = f"{provider_cls.__name__} lacks {missing}"
    assert incomplete == {}, incomplete


# Every hook the framework reads off a config by name. A method that looks like one
# of these but is not exactly one is the failure this list exists to catch.
_CONFIG_HOOKS = frozenset(
    {
        "probe_provider_class",
        "probe_catalog_scope",
        "probe_match_target",
        "probe_filter_target",
        "probe_schema_verdict_override",
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
