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

import difflib
import inspect
import re
from pathlib import Path
from typing import Annotated, Dict, Iterator, List, Optional, Set, Tuple

import pytest
from pydantic import Field
from pydantic.fields import FieldInfo

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Enables,
    Filters,
    FiltersByRule,
    HiddenFromDocs,
    Qualifier,
)
from datahub.ingestion.agent.probe_methods import (
    CLASS_CONFIG_HOOKS,
    CONFIG_HOOKS,
    PROVIDER_ATTRIBUTES,
    ProbeMethodSpec,
    ProbeProvider,
    _iter_specs,
    _provider_class,
    config_class_for,
    probe_method,
)
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.source.sql.sql_config import SQL_FAMILY_HOOKS

# Parameter names that carry something a connector hands to an interpreter, a
# filesystem or the network. A getter taking one of these must declare the
# matching gate so the framework checks it first.
_SQLISH_PARAMS = frozenset({"query", "sql", "statement", "ddl", "script", "expression"})
_PATHISH_PARAMS = frozenset({"path", "url", "uri", "endpoint", "route"})

# Any parameter literally named `limit` bounds how much comes back, so it must be
# declared for the framework to clamp it (probe_methods._bounded_kwargs).
_LIMIT_PARAM = "limit"


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


def _absent_extra(exc: BaseException) -> Optional[str]:
    """The third-party package this source needed, when that is why it failed.

    The registry reports both causes as the same ValueError ("unknown or
    unloadable source type ..."), so the surface type cannot tell an
    uninstalled extra from a provider that is actually broken. The cause chain
    can: a missing extra arrives as
    ValueError <- ConfigurationError <- ModuleNotFoundError naming the package.

    A missing module under `datahub` is our own code, so it counts as breakage
    rather than an absent extra.
    """
    cur: Optional[BaseException] = exc
    while cur is not None:
        if isinstance(cur, ModuleNotFoundError) and cur.name:
            root = cur.name.split(".")[0]
            return None if root == "datahub" else root
        cur = cur.__cause__ or cur.__context__
    return None


def _scan() -> Tuple[Dict[str, List[str]], Set[str], List[str], List[str]]:
    """Walk every registered source's probe provider.

    Returns findings keyed by "source.command", the sources actually scanned,
    the sources that are broken, and the sources whose extra is simply not
    installed. The last two are returned rather than swallowed so a shrinking
    scan shows up instead of looking like success, and they are kept apart
    because only one of them is a defect.
    """
    findings: Dict[str, List[str]] = {}
    scanned: Set[str] = set()
    broken: List[str] = []
    absent: List[str] = []
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception as exc:
            extra = _absent_extra(exc)
            if extra:
                absent.append(f"{source_type}: needs {extra}")
            else:
                broken.append(f"{source_type}: {type(exc).__name__}: {exc}")
            continue
        if provider_cls is None:
            continue
        scanned.add(source_type)
        for command, spec in _iter_specs(provider_cls):
            problems = _violations(spec)
            if problems:
                findings[f"{source_type}.{command}"] = problems
    return findings, scanned, broken, absent


def test_no_probe_method_takes_a_dangerous_parameter_without_declaring_a_gate():
    findings, _, _, _ = _scan()
    assert findings == {}, (
        "these probe methods take a parameter the framework cannot check, because "
        "the method never declared it -- add the matching scoped_sql_param / "
        f"scoped_path_param / row_limit_param: {findings}"
    )


def test_the_scan_actually_reached_providers():
    # Guards the test above against passing vacuously: if plugin loading breaks,
    # _scan() finds nothing to check and every rule here trivially holds.
    _, scanned, broken, absent = _scan()
    # probe_provider_class imports its provider lazily, so a provider whose
    # import breaks leaves the config loadable, and one broken provider still
    # clears the count below. Not asserted on `absent`: an optional extra
    # that is not installed is a fact about the environment, not a defect.
    assert broken == [], (
        f"{len(broken)} providers are installed but could not be loaded, so "
        f"the gate scan silently skipped them: {broken}"
    )
    assert len(scanned) >= 25, (
        f"only {len(scanned)} providers scanned; the tripwire is inspecting "
        f"fewer sources than it should; extras not installed: {absent}"
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


def _unread_probe_hooks(config_cls: type) -> Dict[str, List[str]]:
    """class name -> its `probe_` attributes that no reader would ever call."""
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    known = CONFIG_HOOKS
    if issubclass(config_cls, SQLCommonConfig):
        known = known | SQL_FAMILY_HOOKS
    unread: Dict[str, List[str]] = {}
    for klass in config_cls.__mro__:
        suspects = [
            name
            for name in vars(klass)
            if name.startswith("probe_") and name not in known
        ]
        if suspects:
            unread[klass.__name__] = sorted(suspects)
    return unread


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
        unknown.update(_unread_probe_hooks(config_cls))
    assert unknown == {}, (
        "these look like probe hooks but the framework reads none of them, so they "
        f"do nothing; expected one of {sorted(CONFIG_HOOKS)}, or on a "
        f"SQLCommonConfig one of {sorted(SQL_FAMILY_HOOKS)}: {unknown}"
    )


def test_a_sql_family_hook_is_read_only_on_a_sql_config():
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    class _Outside(ConfigModel):
        def probe_normalize_container(self, name: str) -> str:
            return name

        def probe_match_targets(self) -> None:
            return None

    class _Inside(SQLCommonConfig):
        def get_sql_alchemy_url(self) -> str:
            return "sqlite://"

        def probe_normalize_container(self, name: str) -> str:
            return name

        def probe_match_targets(self) -> None:
            return None

    assert _unread_probe_hooks(_Outside) == {
        "_Outside": ["probe_match_targets", "probe_normalize_container"]
    }
    assert _unread_probe_hooks(_Inside) == {"_Inside": ["probe_match_targets"]}


# How close a name must be to a provider attribute to count as a misspelling
# of it: `probe_reports`, `sql_dialects`, `silence_loggers` are; `api_session`
# and `api_headers`, which RestApiPassthrough defines, are not.
_MISSPELLING_RATIO = 0.85


def _unread_provider_attributes(provider_cls: type) -> Dict[str, List[str]]:
    """class name -> attributes that look like provider attributes the
    framework reads, but are none of them.

    A probe command is exempt whatever its name (Mode's `probe_data_sources`):
    the framework finds commands by their declaration, not their name.
    """
    unread: Dict[str, List[str]] = {}
    for klass in provider_cls.__mro__:
        if klass is object:
            continue
        suspects = [
            name
            for name, value in vars(klass).items()
            if name not in PROVIDER_ATTRIBUTES
            and not isinstance(
                getattr(value, "__probe_command__", None), ProbeMethodSpec
            )
            and (
                name.startswith("probe_")
                or difflib.get_close_matches(
                    name, PROVIDER_ATTRIBUTES, n=1, cutoff=_MISSPELLING_RATIO
                )
            )
        ]
        if suspects:
            unread[klass.__name__] = sorted(suspects)
    return unread


def test_no_provider_declares_an_attribute_the_framework_will_never_read():
    """A misspelled provider attribute is silence, like a misspelled hook.

    The framework reads these by name (`getattr(provider, "probe_report")`),
    so a provider exposing `probe_reports` reports its ingestion failures to
    nobody: the read fails, and the probe answers "empty" for what it could
    not read.
    """
    unread: Dict[str, List[str]] = {}
    for source_type in sorted(source_registry.mapping):
        try:
            provider_cls = _provider_class(source_type)
        except Exception:
            continue  # covered by test_the_scan_actually_reached_providers
        if provider_cls is not None:
            unread.update(_unread_provider_attributes(provider_cls))
    assert unread == {}, (
        "these look like provider attributes but the framework reads none of "
        f"them; expected one of {sorted(PROVIDER_ATTRIBUTES)}: {unread}"
    )


def test_the_provider_attribute_check_catches_a_misspelling():
    class _Typos:
        sql_dialects = "postgres"
        silence_loggers = ("vendor.sdk",)
        api_session = None

        @property
        def probe_reports(self) -> object:
            return None

        @probe_method(name="things")
        def probe_things(self) -> List[str]:
            """A command: found by its declaration, so its name is free."""
            return []

    assert _unread_provider_attributes(_Typos) == {
        "_Typos": ["probe_reports", "silence_loggers", "sql_dialects"]
    }


def test_the_base_declares_every_provider_attribute_the_framework_reads():
    from datahub.ingestion.agent.provider_helpers import ProbeProviderBase

    missing = sorted(
        name for name in PROVIDER_ATTRIBUTES if not hasattr(ProbeProviderBase, name)
    )
    assert missing == []
    assert _unread_provider_attributes(ProbeProviderBase) == {}


_GUIDE = (
    Path(__file__).resolve().parents[3] / "docs" / "dev_guides" / "probe_interface.md"
)
_HOOK_REFERENCE_HEADING = "## Hook reference"
# The first cell of a table row. Prose and other tables mention `probe_report`,
# `probe_method` and friends, which are not config hooks.
_HOOK_ROW = re.compile(r"^\|\s*`(probe_\w+)`", re.MULTILINE)


def _documented_config_hooks(markdown: str) -> Set[str]:
    """Hook names in the first column of the guide's hook reference table."""
    start = markdown.find(f"\n{_HOOK_REFERENCE_HEADING}\n")
    if start == -1:
        raise ValueError(f"the guide has no '{_HOOK_REFERENCE_HEADING}' section")
    body = markdown[start + len(_HOOK_REFERENCE_HEADING) + 2 :]
    # "\n## " does not match "\n### ", so subsections stay in the section.
    end = body.find("\n## ")
    return set(_HOOK_ROW.findall(body if end == -1 else body[:end]))


def test_the_guide_documents_exactly_the_hooks_the_framework_reads():
    """A hook missing from the guide is one a connector author cannot find. A
    name the guide lists that the framework does not read is one they
    implement for nothing.
    """
    documented = _documented_config_hooks(_GUIDE.read_text(encoding="utf-8"))
    undocumented = sorted(CONFIG_HOOKS - documented)
    unread = sorted(documented - CONFIG_HOOKS)
    assert not undocumented and not unread, (
        f"{_GUIDE.name} '{_HOOK_REFERENCE_HEADING}' and CONFIG_HOOKS disagree. "
        f"In CONFIG_HOOKS but missing from the guide: {undocumented}. "
        f"In the guide but not in CONFIG_HOOKS: {unread}."
    )


_SQL_FAMILY_HEADING = "### SQL-family hooks"


def _documented_sql_family_hooks(markdown: str) -> Set[str]:
    """Hook names in the first column of the guide's SQL-family section."""
    start = markdown.find(f"\n{_SQL_FAMILY_HEADING}\n")
    if start == -1:
        raise ValueError(f"the guide has no '{_SQL_FAMILY_HEADING}' section")
    body = markdown[start + len(_SQL_FAMILY_HEADING) + 2 :]
    ends = [i for i in (body.find("\n## "), body.find("\n### ")) if i != -1]
    return set(_HOOK_ROW.findall(body[: min(ends)] if ends else body))


def test_the_guide_documents_exactly_the_sql_family_hooks():
    documented = _documented_sql_family_hooks(_GUIDE.read_text(encoding="utf-8"))
    assert documented == set(SQL_FAMILY_HOOKS), (
        f"{_GUIDE.name} '{_SQL_FAMILY_HEADING}' and SQL_FAMILY_HOOKS disagree. "
        f"Missing from the guide: {sorted(SQL_FAMILY_HOOKS - documented)}. "
        f"Not in SQL_FAMILY_HOOKS: {sorted(documented - SQL_FAMILY_HOOKS)}."
    )


def test_only_the_hook_reference_table_counts_as_documentation():
    markdown = "\n".join(
        [
            "# Guide",
            "Prose naming `probe_report` and `probe_ancestor_kinds`.",
            "",
            _HOOK_REFERENCE_HEADING,
            "",
            "| Hook | Signature |",
            "| --- | --- |",
            "| `probe_provider_class` | `(cls) -> type`; see `probe_report` |",
            "",
            "### A subsection",
            "",
            "| `probe_unfiltered_kinds` | `(cls) -> Set[str]` |",
            "",
            "## Next section",
            "",
            "| `probe_match_target` | not in the reference |",
        ]
    )
    assert _documented_config_hooks(markdown) == {
        "probe_provider_class",
        "probe_unfiltered_kinds",
    }
    with pytest.raises(ValueError):
        _documented_config_hooks("# Guide\n\nNo reference here.\n")


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


def _loaded_source_configs() -> Iterator[Tuple[str, type]]:
    """(source_type, config_cls) for every registered source whose config loads.

    One skeleton. _probe_capable_configs and _sql_source_types each carried
    their own copy of resolve-or-skip, differing only in the filter applied
    afterwards -- and this file exists precisely because a scan that quietly
    shrinks stays green, so two ways of deciding what "loadable" means is
    the wrong number of ways.

    An uninstalled extra is skipped and is not this helper's business; a
    provider that is installed and BROKEN is caught by
    test_the_scan_actually_reached_providers, which classifies the two apart.
    """
    for source_type in sorted(source_registry.mapping):
        try:
            source_cls = source_registry.get(source_type)
            get_config_class = getattr(source_cls, "get_config_class", None)
            if get_config_class is None:
                continue
            config_cls = get_config_class()
        except Exception:
            continue
        if isinstance(config_cls, type):
            yield source_type, config_cls


def _probe_capable_configs():
    """(source_type, config_cls) for every source that declares any probe kind."""
    from datahub.ingestion.agent.introspect import declared_kinds_for_class

    out = []
    for source_type, config_cls in _loaded_source_configs():
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
    from datahub.ingestion.agent.declarations import (
        declared_rule_filtered_kinds,
        declared_unfiltered_kinds,
    )
    from datahub.ingestion.agent.introspect import (
        _pattern_field_for_config_class,
        declared_kinds_for_class,
    )

    silent: Dict[str, List[str]] = {}
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        unfiltered = declared_unfiltered_kinds(config_cls)
        rule_kinds = declared_rule_filtered_kinds(config_cls)
        for kind in sorted(declared_kinds_for_class(source_type, config_cls)):
            checked += 1
            if kind in unfiltered or kind in rule_kinds:
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
    from datahub.ingestion.agent.declarations import declared_unfiltered_kinds
    from datahub.ingestion.agent.introspect import _pattern_field_for_config_class

    contradictions = []
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        for kind in sorted(declared_unfiltered_kinds(config_cls)):
            checked += 1
            field = _pattern_field_for_config_class(config_cls, kind)
            if field is not None:
                contradictions.append(
                    f"{source_type}: declares {kind!r} unfiltered but "
                    f"{field} would filter it"
                )
    assert not contradictions, "\n  ".join(contradictions)
    # _probe_capable_configs skips a source that will not load and
    # declared_unfiltered_kinds is empty for a config that cannot answer, so
    # without this an emptied scan would pass as "no contradictions".
    assert checked > 0, (
        "no source declared an unfiltered kind, so this tripwire checked nothing"
    )


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


def _fields_leaning_on_the_name_convention(
    source_type: str, config_cls: type
) -> List[str]:
    """The fields `describe` maps to a kind only through the `<kind>_pattern` guess.

    A rule field (FiltersByRule) counts as explicit: the connector named it
    for that kind, which is the opposite of a guess.
    """
    from datahub.ingestion.agent.config_fields import iter_config_fields
    from datahub.ingestion.agent.declarations import (
        declared_filter_kind,
        declared_rule_filtered_kinds,
    )
    from datahub.ingestion.agent.introspect import _filter_kinds_by_field

    explicit = {
        path
        for path, info in iter_config_fields(config_cls)
        if declared_filter_kind(info) is not None
    } | set(declared_rule_filtered_kinds(config_cls).values())
    return sorted(set(_filter_kinds_by_field(source_type, config_cls)) - explicit)


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

    Bounded, and knowing where: it sweeps only the kinds each source DECLARES,
    because inverting the convention across undeclared ones would report
    `procedure_pattern` and `profile_pattern` as hierarchy levels. So a source
    with a pattern field for a level it does not declare is outside this
    assertion: `probe filter --kind` on that level resolves the field by name.
    That is what introspect's _warn_convention exists to surface at runtime,
    since no test here can.
    """
    leaning = {}
    checked = 0
    for source_type, config_cls in _probe_capable_configs():
        checked += 1
        by_convention = _fields_leaning_on_the_name_convention(source_type, config_cls)
        if by_convention:
            leaning[source_type] = by_convention

    assert leaning == {}, (
        "these fields resolve only by the name convention:\n"
        f"  {leaning}\n"
        "Annotate each with Filters(...). Check first that it is the field "
        "ingestion actually filters on: the guess can land on a deprecated "
        "alias, and annotating that makes the wrong field permanent."
    )
    assert checked > 20, f"only {checked} probe-capable configs reached"


def test_no_config_declares_a_catalog_scope_its_provider_never_reads():
    """The sql gate reads one scope: the provider's catalog_scope.

    The SQLAlchemy provider sets it from the config's probe_catalog_scope,
    because it serves every SQL-family dialect and the dialect's config is
    the only thing that knows its catalog. A provider of its own declares
    catalog_scope on its class (Snowflake's covers snowflake-summary, whose
    config is not a SQLCommonConfig). A config overriding probe_catalog_scope
    for any other provider has written a scope nothing reads.
    """
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
    from datahub.ingestion.source.sql.sqlalchemy_probe import (
        SqlAlchemyMetadataProbe,
    )

    unread = []
    checked = 0
    for source_type, config_cls in _sql_source_types().items():
        declaring = next(
            klass
            for klass in config_cls.__mro__
            if "probe_catalog_scope" in vars(klass)
        )
        if declaring is SQLCommonConfig:
            continue
        checked += 1
        provider_cls = _provider_class(source_type)
        if provider_cls is None or not issubclass(
            provider_cls, SqlAlchemyMetadataProbe
        ):
            unread.append(
                f"{source_type}: {config_cls.__name__}.probe_catalog_scope is "
                f"never read by {getattr(provider_cls, '__name__', None)}"
            )

    assert not unread, (
        "these configs declare a catalog scope nothing reads:\n  "
        + "\n  ".join(unread)
        + "\nDeclare catalog_scope on the provider instead."
    )
    assert checked >= 3, (
        f"only {checked} SQL configs declare a catalog scope; expected at "
        "least three, so this test is not scanning nothing"
    )


def test_every_config_hook_matches_the_signature_the_framework_calls():
    """Hooks are resolved by getattr, so mypy cannot see a signature drift:
    widening a hook updates its overrides and can leave SQLCommonConfig's
    base behind, breaking `probe filter` on every other SQL source while the
    name-only check above passes.
    """
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    # Keyword arguments the framework passes, per hook. A hook must accept
    # every one of these -- by name, since every call site uses keywords.
    required_kwargs = {
        "probe_validation_context": {"source_type"},
        "probe_match_target": {"ctx"},
        "probe_verdict_override": {"ctx"},
        # `database` is the container above the schema, which an override
        # whose recipe spans several databases needs from the caller.
        "probe_filter_target": {"schema", "entity", "warn", "database"},
        "probe_ancestor_kinds": {"kind"},
    }

    problems = []
    checked = 0
    implementers_by_hook: Dict[str, List[type]] = {}
    for hook, kwargs in required_kwargs.items():
        implementers: List[type] = [SQLCommonConfig]
        implementers_by_hook[hook] = implementers
        for source_type in sorted(source_registry.mapping):
            try:
                config_cls = config_class_for(source_type)
            except Exception:
                continue
            if config_cls is None:
                # A registered source with no get_config_class. The except
                # above does not cover this -- it wraps the call, not the walk
                # -- so the AttributeError would error the test rather than
                # skip the source. Guarded the way the sibling test guards it.
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

    # Per hook, not a sum: a total is satisfied by one hook's implementers
    # while another has no override to compare the base against, which is the
    # case worth catching.
    for hook, found in implementers_by_hook.items():
        assert SQLCommonConfig in found, f"{hook}: the base was not checked"
    assert len(implementers_by_hook["probe_filter_target"]) >= 2, (
        "probe_filter_target has no override left, so this test compares the "
        "base signature against nothing and cannot see a drift"
    )


def _class_hook_problems(config_cls: type) -> List[str]:
    """The CLASS_CONFIG_HOOKS `config_cls` declares as anything but a
    classmethod or staticmethod. The framework calls them on the class, where
    an instance method fails for want of `self` -- and only when called, so
    `describe` or `probe methods` breaks while `probe filter` works."""
    problems = []
    for hook in CLASS_CONFIG_HOOKS:
        try:
            declared = inspect.getattr_static(config_cls, hook)
        except AttributeError:
            continue
        if not isinstance(declared, (classmethod, staticmethod)):
            problems.append(
                f"{config_cls.__name__}.{hook} is a {type(declared).__name__}; "
                f"the framework calls it on the class, so make it a classmethod"
            )
    return problems


def test_every_class_called_hook_is_a_classmethod():
    assert set(CLASS_CONFIG_HOOKS) <= CONFIG_HOOKS
    problems: List[str] = []
    declaring = 0
    for _source_type, config_cls in _loaded_source_configs():
        if any(hasattr(config_cls, hook) for hook in CLASS_CONFIG_HOOKS):
            declaring += 1
        problems.extend(_class_hook_problems(config_cls))
    assert problems == [], "\n  ".join(problems)
    assert declaring > 20, f"only {declaring} configs declare a class-called hook"


def test_the_class_hook_check_catches_an_instance_method():
    class _Instance(ConfigModel):
        def probe_unfiltered_kinds(self) -> Set[str]:
            return {"Dataset"}

    class _Inherits(_Instance):
        pass

    class _Class(ConfigModel):
        @classmethod
        def probe_unfiltered_kinds(cls) -> Set[str]:
            return {"Dataset"}

    assert len(_class_hook_problems(_Instance)) == 1
    assert len(_class_hook_problems(_Inherits)) == 1
    assert _class_hook_problems(_Class) == []


# --- across every registered SQL source -------------------------------------


def _sql_source_types():
    from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

    found = {
        source_type: config_cls
        for source_type, config_cls in _loaded_source_configs()
        if issubclass(config_cls, SQLCommonConfig)
    }
    return found


def test_no_sql_source_falls_back_to_the_bare_fqn():
    """The get_identifier shim must work for every registered SQL source.

    The shim builds the Source via __new__ and gives it only its config, so
    an override reaching for state the shim does not carry degrades to the
    plain fqn -- discoverable only at runtime, on whichever connector nobody
    probed.

    The degrade is warned rather than silent, so it is a quality floor
    rather than a leak. This turns it into a build-time floor: all 29
    sources are clean today, and a connector that stops being clean fails
    here instead of returning a worse filter target in production.

    The suggested alternative -- a typed adapter that does not call the
    connector's own get_identifier -- is deliberately not taken. Calling the
    real one is the whole point: Db2 uppercases, StarRocks pins its built-in
    catalog, mssql prefers its per-database value, and reimplementing any of
    that is the drift the shim exists to prevent.
    """
    from datahub.ingestion.agent.verdicts import ClassifyContext
    from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
    from datahub.ingestion.source.sql.sql_probe import (
        IDENTIFIER_DEGRADE_MARKER,
        _identifier_target,
        _source_class_for,
    )

    # Enough of a config for a get_identifier to read; unset required fields
    # would make the shim raise for reasons that are about this test rather
    # than about the connector.
    common = {
        "host_port": "host:1234",
        "username": "u",
        "password": "p",
        "database": "DB",
        "scheme": "postgresql",
    }

    found = _sql_source_types()
    assert len(found) > 20, f"only {len(found)} SQL sources discovered; the scan broke"

    degraded = {}
    for source_type, config_cls in found.items():
        fields = set(getattr(config_cls, "model_fields", {}))
        config = config_cls.model_construct(
            **{k: v for k, v in common.items() if k in fields}
        )
        warnings: list = []
        ctx = ClassifyContext(
            config=config,
            name="T1",
            fqn="T1",
            pattern_field="table_pattern",
            parent_path=("DB", "SCH"),
            warn=warnings.append,
        )
        try:
            target = _identifier_target(ctx)
        except Exception as exc:
            degraded[source_type] = f"raised {type(exc).__name__}: {exc}"
            continue
        # Imported rather than a copied substring: this is the ONLY thing
        # separating a degrade from a connector that legitimately returns a
        # bare name, since both return ctx.fqn. Matching the prose meant a
        # reworded warning would leave `reason` None and skip a degraded
        # source through the has_override branch below -- disarming the
        # build-time floor this test is documented to provide.
        reason = next((w for w in warnings if IDENTIFIER_DEGRADE_MARKER in w), None)
        if reason:
            degraded[source_type] = reason
        elif target == ctx.fqn and "." not in target:
            # Druid legitimately matches the bare name, and says so by
            # overriding get_identifier -- that is not a degrade. A source
            # reaching the fqn WITHOUT an override is.
            #
            # Both mechanisms count, because the shim reaches get_identifier
            # two ways: the deprecated config-level hook that
            # SQLAlchemySource.get_identifier routes through (druid, mysql,
            # trino, oracle and their aliases) and a Source-class override
            # that _source_class_for resolves (postgres, db2, mssql,
            # teradata, vertica, starrocks, cockroachdb, timescaledb).
            #
            # Checking only the config MRO would call the second group
            # degraded for doing deliberately what the first group is
            # exempted for. Only druid returns a bare name today, so that was
            # a latent false positive rather than a live one -- and checking
            # only the Source MRO instead would swap it for the opposite bug,
            # since druid's override is the config-level kind.
            has_override = "get_identifier" in {
                k for c in type(config).__mro__ for k in vars(c)
            } or any(
                "get_identifier" in vars(klass)
                for klass in _source_class_for(config).__mro__
                if klass is not SQLAlchemySource
            )
            if not has_override:
                degraded[source_type] = f"bare fqn {target!r} with no override"

    assert not degraded, (
        "the get_identifier shim degrades for these sources, so their filter "
        "targets are worse than what ingestion matches on:\n  "
        + "\n  ".join(f"{k}: {v}" for k, v in degraded.items())
    )


def test_containers_reports_the_tier_every_sql_config_declares():
    """`containers` lists schemas on a three-tier source and databases on a
    two-tier one. `probe methods` and `probe run` report the kind from
    probe_kind_overrides, while the ancestor chain `probe filter` judges
    --parent with follows probe_container_kind, so the two must agree on
    every SQL source.
    """
    from datahub.ingestion.agent.probe_methods import list_probe_methods

    found = _sql_source_types()
    assert len(found) > 20, f"only {len(found)} SQL sources discovered; the scan broke"

    disagreed = {}
    checked = 0
    for source_type, config_cls in found.items():
        listed = {spec.command: spec.kind for spec in list_probe_methods(source_type)}
        if "containers" not in listed:
            # A SQL config with a provider of its own may have no
            # `containers` command to report a kind for.
            continue
        checked += 1
        tier = str(config_cls.probe_container_kind())
        if listed["containers"] != tier:
            disagreed[source_type] = (
                f"probe methods says {listed['containers']!r}, "
                f"probe_container_kind says {tier!r}"
            )

    assert checked > 5, f"only {checked} SQL sources list containers; scan broke"
    assert not disagreed, (
        "`containers` reports a kind its config's tier does not, so `probe filter` "
        "would judge the wrong pattern:\n  "
        + "\n  ".join(f"{k}: {v}" for k, v in disagreed.items())
    )


def test_every_config_declares_its_markers_as_the_probe_reads_them():
    """The probe refuses a misdeclared config when it first reads it; this
    finds one in-tree before a user's `probe filter` does."""
    from datahub.ingestion.agent.declarations import (
        declared_kind_enablers,
        marker_problems,
    )

    problems: Dict[str, List[str]] = {}
    declaring = 0
    for source_type, config_cls in _loaded_source_configs():
        found = marker_problems(config_cls)
        if found:
            problems[source_type] = found
        elif declared_kind_enablers(config_cls):
            declaring += 1
    assert problems == {}, (
        "these configs misdeclare a probe marker, so the probe refuses them "
        "(exit 1). Fix each declaration as its message says; the rules are "
        f"in declarations.marker_problems:\n  {problems}"
    )
    # The SQL family alone declares two: an emptied scan must not pass.
    assert declaring > 20, f"only {declaring} configs declare Enables"


def _rule_kinds_a_pattern_is_found_for(config_cls: type) -> List[str]:
    """Rule-filtered kinds the `<kind>_pattern` name guess also finds a field
    for. probe filter judges a rule kind by its rules and ignores any pattern,
    so that field, which ingestion may well apply, never reaches a verdict. A
    Filters-declared one is marker_problems' to refuse."""
    from datahub.ingestion.agent.declarations import declared_rule_filtered_kinds
    from datahub.ingestion.agent.introspect import _pattern_field_for_config_class

    problems = []
    for kind in sorted(declared_rule_filtered_kinds(config_cls)):
        pattern_field = _pattern_field_for_config_class(config_cls, kind)
        if pattern_field is not None:
            problems.append(
                f"{config_cls.__name__}: '{kind}' is declared rule-filtered "
                f"but also resolves to the pattern field '{pattern_field}'"
            )
    return problems


def test_no_rule_filtered_kind_also_resolves_to_a_pattern():
    problems = [
        problem
        for _source_type, config_cls in _probe_capable_configs()
        for problem in _rule_kinds_a_pattern_is_found_for(config_cls)
    ]
    assert problems == [], (
        "declare Filters on the pattern if ingestion applies it (and drop "
        "FiltersByRule for that kind), or rename it off the `<kind>_pattern` "
        f"convention if it filters something else:\n  {problems}"
    )


class _RulesAndPattern(ConfigModel):
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )
    table_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        return Verdict.include()


def test_the_rule_kind_check_catches_a_pattern_found_by_name():
    problems = _rule_kinds_a_pattern_is_found_for(_RulesAndPattern)
    assert len(problems) == 1
    assert "table_pattern" in problems[0]


def test_describe_maps_a_rule_kind_to_its_rule_field_only(monkeypatch):
    from datahub.ingestion.agent import introspect

    monkeypatch.setattr(
        introspect, "declared_kinds_for_class", lambda _st, _cls: {"Table"}
    )
    assert introspect._filter_kinds_by_field("fake-source", _RulesAndPattern) == {
        "path_specs": "Table"
    }


def test_a_rule_field_is_not_mistaken_for_the_name_guess(monkeypatch):
    from datahub.ingestion.agent import introspect

    class _RulesOnly(ConfigModel):
        path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
            default_factory=list
        )

        def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
            return Verdict.include()

    monkeypatch.setattr(
        introspect, "declared_kinds_for_class", lambda _st, _cls: {"Table"}
    )
    assert _fields_leaning_on_the_name_convention("fake-source", _RulesOnly) == []


_FIELD_MARKERS = (Filters, Enables, FiltersByRule, Qualifier)


def _field_markers(info: FieldInfo) -> List[object]:
    return [m for m in info.metadata if isinstance(m, _FIELD_MARKERS)]


def _inherited_field(klass: type, name: str) -> Optional[FieldInfo]:
    """`name` as klass would inherit it: from the nearest base declaring it."""
    for base in klass.__mro__[1:]:
        fields = getattr(base, "model_fields", None) or {}
        if name in fields and name in inspect.get_annotations(base):
            return fields[name]
    return None


def _dropped_markers(config_cls: type) -> List[str]:
    """The markers a class in config_cls's MRO loses by redeclaring an
    inherited field: pydantic replaces a redeclared field's metadata, so a
    redeclaration that only changes a default drops the marker unseen.

    A drop is deliberate where the class carries the marker on another field
    (it moved) or hides the field from the docs (a deprecated alias that no
    longer does the job).
    """
    from datahub.ingestion.agent.introspect import _is_hidden_field

    problems = []
    for klass in config_cls.__mro__:
        fields = getattr(klass, "model_fields", None) or {}
        for name in inspect.get_annotations(klass):
            inherited = _inherited_field(klass, name)
            if name not in fields or inherited is None:
                continue
            if _is_hidden_field(klass, name):
                continue
            kept = {
                marker for info in fields.values() for marker in _field_markers(info)
            }
            lost = [m for m in _field_markers(inherited) if m not in kept]
            if lost:
                problems.append(
                    f"{klass.__module__}.{klass.__qualname__}.{name} redeclares "
                    f"an inherited field and drops {lost}"
                )
    return problems


def test_no_config_drops_a_marker_by_redeclaring_its_field():
    problems: Set[str] = set()
    checked = 0
    for _source_type, config_cls in _loaded_source_configs():
        checked += 1
        problems.update(_dropped_markers(config_cls))
    assert sorted(problems) == [], (
        "these fields lost a probe marker when redeclared. Repeat the marker "
        "on the redeclaration, move it to the field that now does the job, or "
        "hide the field (HiddenFromDocs) if it is a deprecated alias."
    )
    assert checked > 50, f"only {checked} configs reached"


def test_the_redeclaration_check_catches_a_dropped_marker():
    class _Base(ConfigModel):
        include_things: Annotated[bool, Enables("Thing")] = True
        thing_pattern: Annotated[AllowDenyPattern, Filters("Thing")] = Field(
            default=AllowDenyPattern.allow_all()
        )
        legacy_pattern: Annotated[AllowDenyPattern, Filters("Box")] = Field(
            default=AllowDenyPattern.allow_all()
        )
        old_pattern: Annotated[AllowDenyPattern, Filters("Crate")] = Field(
            default=AllowDenyPattern.allow_all()
        )

    class _Redeclares(_Base):
        include_things: bool = False
        thing_pattern: Annotated[AllowDenyPattern, Filters("Thing")] = Field(
            default=AllowDenyPattern.allow_all()
        )
        legacy_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
        box_pattern: Annotated[AllowDenyPattern, Filters("Box")] = Field(
            default=AllowDenyPattern.allow_all()
        )
        old_pattern: HiddenFromDocs[AllowDenyPattern] = Field(
            default=AllowDenyPattern.allow_all()
        )

    problems = _dropped_markers(_Redeclares)
    assert len(problems) == 1 and "include_things" in problems[0]


def test_the_framework_reads_no_config_hook_outside_its_list():
    from datahub.ingestion.agent.probe_methods import config_hook
    from datahub.ingestion.agent.verdicts import ProbeInternalError

    with pytest.raises(ProbeInternalError):
        config_hook(object(), "probe_match_targets")
    assert config_hook(object(), "probe_match_target") is None
