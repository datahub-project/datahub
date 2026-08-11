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

from typing import Dict, List, Optional, Set, Tuple

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


def test_discovery_and_execution_describe_the_same_provider():
    """`probe methods` and `probe run` must resolve to one provider class.

    Discovery reads config.probe_provider_class(); execution calls
    config.build_probe_provider(). A connector that overrides one and inherits
    the other advertises commands that fail at invocation -- Snowflake and
    BigQuery each advertised six SQLAlchemy getters their own probe does not
    have, and every one of them would have raised "no probe method bound".
    """
    mismatched: Dict[str, str] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            config_cls = config_class_for(source_type)
        except Exception:
            continue  # covered by test_the_scan_actually_reached_providers
        if config_cls is None or not hasattr(config_cls, "probe_provider_class"):
            continue
        declared = _defining_class(config_cls, "probe_provider_class")
        built = _defining_class(config_cls, "build_probe_provider")
        if declared != built:
            mismatched[source_type] = (
                f"probe_provider_class on {declared}, build_probe_provider on {built}"
            )
    assert mismatched == {}, (
        "override probe_provider_class and build_probe_provider on the same class "
        f"-- they must name one provider: {mismatched}"
    )


def _defining_class(cls: type, attr: str) -> Optional[str]:
    for klass in cls.__mro__:
        if attr in vars(klass):
            return klass.__name__
    return None


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
