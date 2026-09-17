import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol

from datahub.ingestion.source.montecarlo.queries import (
    ALERTS_QUERY,
    CUSTOM_RULES_QUERY,
    MONITORS_QUERY,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

# Desired field selections per Monte Carlo GraphQL type, in camelCase (the
# casing the gateway expects; pycarlo snake_cases the response keys). A ``None``
# value marks a scalar field; a nested ``dict`` marks an object field whose
# sub-selection is itself a desired-field tree. The query builder intersects
# this tree with the live introspected schema and emits only the fields the
# gateway still exposes, so a removed field degrades gracefully (its
# ``MonteCarloAssertionDef`` slot stays ``None``) instead of failing the whole
# fetch with a 400. The drift gate diffs the same tree against the live schema
# to surface what was dropped.
DesiredFields = Dict[str, Optional[Any]]

MONITOR_DESIRED_FIELDS: DesiredFields = {
    "uuid": None,
    "name": None,
    "description": None,
    "monitorType": None,
    # customSql/severity were removed from the Monitor type and renamed: the
    # SQL predicate is now top-level `whereCondition`, and `severity` is now
    # `priority`. Both old and new names are requested so the connector adapts
    # to either gateway version; assertion.py maps priority->severity and
    # whereCondition->custom_sql as fallbacks (see client.MonteCarloAssertionDef).
    "customSql": None,
    "whereCondition": None,
    "entityMcons": None,
    "resourceId": None,
    "severity": None,
    "priority": None,
    "dataQualityDimension": None,
    "comparisons": {
        "comparisonType": None,
        "operator": None,
        "metric": None,
        "customMetric": {"uuid": None, "metricName": None},
        "field": None,
        "fields": None,
        "threshold": None,
        "upperThreshold": None,
        "lowerThreshold": None,
    },
}

CUSTOM_RULE_DESIRED_FIELDS: DesiredFields = {
    "uuid": None,
    "ruleName": None,
    "ruleType": None,
    "description": None,
    "customSql": None,
    "entityMcons": None,
    # severity was renamed to priority on CustomRule (customSql is still
    # exposed). Request both so the connector adapts to either gateway version;
    # assertion.py maps priority->severity as a fallback.
    "severity": None,
    "priority": None,
    "comparisons": {
        "comparisonType": None,
        "operator": None,
        "metric": None,
        "customMetric": {"uuid": None, "metricName": None},
        "field": None,
        "fields": None,
        "threshold": None,
        "upperThreshold": None,
        "lowerThreshold": None,
    },
}

ALERT_DESIRED_FIELDS: DesiredFields = {
    "id": None,
    "type": None,
    "subTypes": None,
    "severity": None,
    "priority": None,
    "status": None,
    "createdTime": None,
    "monitorUuids": None,
    "assets": {"mcon": None},
}

# Fields whose absence from the live schema means the connector cannot function
# for that type: uuid/id is the item identity, entityMcons/monitorUuids attach
# the item to a dataset/assertion. Missing any of these aborts the phase rather
# than emitting malformed/empty records.
MONITOR_CRITICAL_FIELDS = frozenset({"uuid", "entityMcons"})
CUSTOM_RULE_CRITICAL_FIELDS = frozenset({"uuid", "entityMcons"})
ALERT_CRITICAL_FIELDS = frozenset({"id", "monitorUuids"})

# Minimal known-good selections used when introspection itself fails (network
# error / auth error on the __type call). These request the renamed, currently
# stable fields (whereCondition/priority on Monitor, customSql/priority on
# CustomRule) rather than the removed customSql/severity, so the fallback
# never re-triggers a 400. The connector proceeds with degraded data rather
# than zeroing out the whole phase.
_MONITOR_FALLBACK_SELECTION = (
    "uuid\n    name\n    description\n    monitorType\n    whereCondition\n    "
    "priority\n    entityMcons"
)
_CUSTOM_RULE_FALLBACK_SELECTION = (
    "uuid\n    ruleName\n    ruleType\n    description\n    customSql\n    "
    "priority\n    entityMcons"
)
_ALERT_FALLBACK_SELECTION = (
    "id\n    type\n    severity\n    status\n    createdTime\n    monitorUuids\n"
    "    assets { mcon }"
)


class DriftVerdict(StrEnum):
    """Outcome of a schema-drift check for one type.

    PROCEED: no drift; the live schema exposes every desired field.
    DEGRADED: optional fields are missing; ingestion proceeds without them (the
      query builder already dropped them, and every consumer handles None). A
      warning is reported naming the dropped fields.
    ABORT: a critical field is missing, or strict_schema_drift is enabled and any
      drift was found. The phase must stop cleanly rather than emit
      malformed/empty records.
    """

    PROCEED = "PROCEED"
    DEGRADED = "DEGRADED"
    ABORT = "ABORT"


@dataclass
class TypeDrift:
    """Per-type drift result. missing = desired fields absent from the live
    schema; new_fields = live fields the connector does not request (an under-
    ingestion signal, surfaced as info, not actionable for the operator).
    critical_missing = the subset of missing that is critical for this type."""

    type_name: str
    verdict: DriftVerdict
    missing: list = field(default_factory=list)
    critical_missing: list = field(default_factory=list)
    new_fields: list = field(default_factory=list)

    def summary(self) -> str:
        parts = [f"type={self.type_name}", f"verdict={self.verdict.value}"]
        if self.missing:
            parts.append(f"missing={','.join(self.missing)}")
        if self.critical_missing:
            parts.append(f"critical_missing={','.join(self.critical_missing)}")
        if self.new_fields:
            parts.append(f"new_uningested={','.join(self.new_fields)}")
        return ", ".join(parts)


@dataclass
class SchemaDrift:
    """Aggregate drift across all checked types, with the overall verdict (the
    worst per-type verdict). ABORT dominates DEGRADED dominates PROCEED."""

    per_type: Dict[str, TypeDrift] = field(default_factory=dict)

    @property
    def verdict(self) -> DriftVerdict:
        if not self.per_type:
            return DriftVerdict.PROCEED
        if any(t.verdict == DriftVerdict.ABORT for t in self.per_type.values()):
            return DriftVerdict.ABORT
        if any(t.verdict == DriftVerdict.DEGRADED for t in self.per_type.values()):
            return DriftVerdict.DEGRADED
        return DriftVerdict.PROCEED

    def aborting_types(self) -> list:
        return [t for t in self.per_type.values() if t.verdict == DriftVerdict.ABORT]

    def summary(self) -> str:
        return "; ".join(t.summary() for t in self.per_type.values())


# Introspection query for one type. Uses an alias (mcType) so the response key
# is snake_cased to ``mc_type`` by pycarlo, avoiding any dunder-key ambiguity
# on ``__type``. Selects field name + the (unwrapped) type kind/name, recursing
# one level into object fields' element types via a second introspection call.
_INTROSPECT_QUERY = """
query mcIntrospect($name: String!) {
  mcType: __type(name: $name) {
    fields {
      name
      type {
        kind
        name
        ofType { kind name ofType { kind name ofType { kind name } } }
      }
    }
  }
}
"""


@dataclass
class _TypeField:
    """One field of an introspected GraphQL type. is_object is True when the
    field's (unwrapped) type is OBJECT, in which case object_type_name is the
    name to introspect for the sub-selection."""

    name: str
    is_object: bool
    object_type_name: Optional[str] = None


@dataclass
class _TypeShape:
    fields: Dict[str, _TypeField] = field(default_factory=dict)


def _unwrap_type(type_dict: Optional[Dict[str, Any]]) -> tuple:
    """Walk the ofType chain (NON_NULL/LIST wrappers) to the underlying type.
    Returns (kind, name) where kind is OBJECT/SCALAR/ENUM and name is the type
    name (non-None only for OBJECT/SCALAR/ENUM)."""
    node = type_dict
    while node is not None and node.get("kind") in ("NON_NULL", "LIST"):
        node = node.get("of_type") or node.get("ofType")
    if node is None:
        return ("SCALAR", None)
    return (node.get("kind"), node.get("name"))


def _parse_type_shape(response: Dict[str, Any]) -> _TypeShape:
    """Parse an introspection response (one __type call) into a _TypeShape.
    Tolerates a missing/empty type (e.g. a name typo) by returning an empty
    shape rather than raising, so the gate can report total drift instead of
    crashing the run."""
    shape = _TypeShape()
    type_node = response.get("mc_type") or response.get("__type")
    if not isinstance(type_node, dict):
        return shape
    for f in type_node.get("fields") or []:
        if not isinstance(f, dict):
            continue
        name = f.get("name")
        if not name:
            continue
        kind, type_name = _unwrap_type(f.get("type"))
        is_object = kind == "OBJECT"
        shape.fields[name] = _TypeField(
            name=name,
            is_object=is_object,
            object_type_name=type_name if is_object else None,
        )
    return shape


def _build_selection_with_subshapes(
    desired: DesiredFields,
    shape: _TypeShape,
    object_shapes: Dict[str, _TypeShape],
    indent: str,
) -> str:
    """Intersect the desired-field tree with the live type shape and emit a
    GraphQL selection string (camelCase field names). Fields absent from the
    live shape are silently dropped; object fields recurse into their element
    type's shape (looked up from ``object_shapes``, keyed by object type name).
    can introspect element types once and pass the shapes down for recursion."""
    lines = []
    for fname, sub in desired.items():
        live = shape.fields.get(fname)
        if live is None:
            continue
        if sub is None:
            lines.append(f"{indent}{fname}")
        elif isinstance(sub, dict):
            if not live.is_object or not live.object_type_name:
                continue
            sub_shape = object_shapes.get(live.object_type_name, _TypeShape())
            sub_lines = _build_selection_with_subshapes(
                sub, sub_shape, object_shapes, indent + "    "
            )
            if not sub_lines.strip():
                # Object field present but none of its desired sub-fields exist;
                # request an empty selection would be invalid, so skip the field.
                continue
            lines.append(f"{indent}{fname} {{")
            lines.append(sub_lines)
            lines.append(f"{indent}}}")
    return "\n".join(lines)


def _diff_drift(
    type_name: str,
    desired: DesiredFields,
    shape: _TypeShape,
    critical: frozenset,
    strict: bool,
) -> TypeDrift:
    """Diff a desired-field tree against a live type shape and produce a
    TypeDrift verdict. missing = desired fields absent from the live shape
    (top-level only; nested drift is reported under the parent field name).
    new_fields = live fields the connector does not request."""
    missing = [f for f in desired if f not in shape.fields]
    critical_missing = [f for f in missing if f in critical]
    desired_set = set(desired)
    new_fields = [f for f in shape.fields if f not in desired_set]
    if critical_missing or missing and strict:
        verdict = DriftVerdict.ABORT
    elif missing:
        verdict = DriftVerdict.DEGRADED
    else:
        verdict = DriftVerdict.PROCEED
    return TypeDrift(
        type_name=type_name,
        verdict=verdict,
        missing=missing,
        critical_missing=critical_missing,
        new_fields=new_fields,
    )


class QueryBuilder(Protocol):
    """Builds the GraphQL query strings the client sends for the three
    drift-prone fetches (monitors, custom rules, alerts). Decoupled from the
    client so tests can inject a StaticQueryBuilder and bypass introspection."""

    def monitors_query(self) -> str: ...

    def custom_rules_query(self) -> str: ...

    def alerts_query(self) -> str: ...

    def check_drift(
        self, strict: bool = False, types: Optional[list] = None
    ) -> SchemaDrift: ...


# Query envelopes (variable signatures) for the three fetches. The variable
# signature does not drift the way field selections do, so the envelope is
# fixed and only the selection set is built dynamically.
_MONITORS_ENVELOPE = (
    "query getMonitors($domainIds: [UUID!], $limit: Int, $offset: Int) "
    "{{\n  getMonitors(domainIds: $domainIds, limit: $limit, offset: $offset) {{\n"
    "    {selection}\n  }}\n}}"
)
_CUSTOM_RULES_ENVELOPE = (
    "query getCustomRules($first: Int, $after: String) {{\n"
    "  getCustomRules(first: $first, after: $after) {{\n    edges {{\n"
    "      node {{\n        {selection}\n      }}\n    }}\n"
    "    pageInfo {{ hasNextPage endCursor }}\n  }}\n}}"
)
_ALERTS_ENVELOPE = (
    "query getAlerts($first: Int, $after: String, $createdTime: DateTimeRangeInput) {{\n"
    "  getAlerts(first: $first, after: $after, createdTime: $createdTime) {{\n"
    "    edges {{\n      node {{\n        {selection}\n      }}\n    }}\n"
    "    pageInfo {{ hasNextPage endCursor }}\n  }}\n}}"
)


def _wrap(envelope: str, selection: str) -> str:
    return envelope.format(selection=selection)


class StaticQueryBuilder:
    """Returns fixed query strings verbatim. Used by tests (which mock
    ``client._call`` directly and never send the query) and as the fallback
    when introspection fails. Holds the original hardcoded queries from
    ``queries.py`` so existing behaviour is preserved exactly when introspection
    is bypassed."""

    def __init__(
        self,
        monitors_query: str = MONITORS_QUERY,
        custom_rules_query: str = CUSTOM_RULES_QUERY,
        alerts_query: str = ALERTS_QUERY,
    ) -> None:
        self._monitors = monitors_query
        self._custom_rules = custom_rules_query
        self._alerts = alerts_query

    def monitors_query(self) -> str:
        return self._monitors

    def custom_rules_query(self) -> str:
        return self._custom_rules

    def alerts_query(self) -> str:
        return self._alerts

    def check_drift(
        self, strict: bool = False, types: Optional[list] = None
    ) -> SchemaDrift:
        # No introspection was performed, so drift cannot be assessed.
        return SchemaDrift()


class IntrospectingQueryBuilder:
    """Builds GraphQL queries dynamically from the live MCD schema, dropping
    fields the gateway no longer exposes so schema drift degrades gracefully
    instead of failing the whole fetch with a 400. Introspects each type once
    per run (cached) and resolves object sub-selections (e.g. ``comparisons``,
    ``assets``) by introspecting their element types. On an introspection
    failure (network/auth error on the ``__type`` call) falls back to a minimal
    known-good selection and reports a warning via the injected ``warner``.

    The ``call`` callable is the client's ``_call`` (so introspection inherits
    auth, retry and daily-budget handling); ``warner`` is the client's ``_warn``
    so fallback warnings surface in the ingestion report.
    """

    def __init__(
        self,
        call: "Callable[..., Dict[str, Any]]",
        warner: "Callable[..., None]",
        fatal_error_types: tuple = (),
    ) -> None:
        self._call = call
        self._warner = warner
        self._fatal_error_types = fatal_error_types
        self._shapes: Dict[str, _TypeShape] = {}
        self._introspection_failed = False

    def _introspect(self, type_name: str) -> _TypeShape:
        if type_name in self._shapes:
            return self._shapes[type_name]
        try:
            response = self._call(_INTROSPECT_QUERY, {"name": type_name})
        except self._fatal_error_types:
            # Fatal run-level errors (exhausted daily budget, rejected
            # credentials) must propagate, not be demoted to a per-introspection
            # warning — a bad token must abort the run with a clear auth error.
            raise
        except Exception as e:
            self._introspection_failed = True
            self._warner(
                title="Monte Carlo schema introspection failed",
                message=(
                    f"Could not introspect the {type_name} type; falling back to a "
                    "minimal field selection. Schema drift will not be detected this "
                    "run."
                ),
                context=f"type={type_name}",
                exc=e,
            )
            self._shapes[type_name] = _TypeShape()
            return self._shapes[type_name]
        shape = _parse_type_shape(response)
        self._shapes[type_name] = shape
        # Pre-introspect element types for object fields the connector wants, so
        # sub-selections resolve without a second round-trip per page.
        for _fname, live in shape.fields.items():
            if live.is_object and live.object_type_name:
                self._introspect(live.object_type_name)
        return shape

    def _object_shapes(self) -> Dict[str, _TypeShape]:
        return self._shapes

    def _build(
        self,
        type_name: str,
        desired: DesiredFields,
        envelope: str,
        fallback_selection: str,
    ) -> str:
        shape = self._introspect(type_name)
        if self._introspection_failed or not shape.fields:
            return _wrap(envelope, fallback_selection)
        selection = _build_selection_with_subshapes(
            desired, shape, self._object_shapes(), "        "
        )
        if not selection.strip():
            self._warner(
                title="Monte Carlo schema drift: no requested fields available",
                message=(
                    f"None of the requested {type_name} fields exist on the live "
                    "schema; falling back to a minimal selection."
                ),
                context=f"type={type_name}",
            )
            return _wrap(envelope, fallback_selection)
        return _wrap(envelope, selection)

    def monitors_query(self) -> str:
        return self._build(
            "Monitor",
            MONITOR_DESIRED_FIELDS,
            _MONITORS_ENVELOPE,
            _MONITOR_FALLBACK_SELECTION,
        )

    def custom_rules_query(self) -> str:
        return self._build(
            "CustomRule",
            CUSTOM_RULE_DESIRED_FIELDS,
            _CUSTOM_RULES_ENVELOPE,
            _CUSTOM_RULE_FALLBACK_SELECTION,
        )

    def alerts_query(self) -> str:
        return self._build(
            "Alert",
            ALERT_DESIRED_FIELDS,
            _ALERTS_ENVELOPE,
            _ALERT_FALLBACK_SELECTION,
        )

    def check_drift(
        self, strict: bool = False, types: Optional[list] = None
    ) -> SchemaDrift:
        drift = SchemaDrift()
        specs = {
            "Monitor": (MONITOR_DESIRED_FIELDS, MONITOR_CRITICAL_FIELDS),
            "CustomRule": (
                CUSTOM_RULE_DESIRED_FIELDS,
                CUSTOM_RULE_CRITICAL_FIELDS,
            ),
            "Alert": (ALERT_DESIRED_FIELDS, ALERT_CRITICAL_FIELDS),
        }
        # Default to all three; callers can restrict to the types they ingest.
        type_names = types if types is not None else list(specs)
        for type_name in type_names:
            if type_name not in specs:
                continue
            desired, critical = specs[type_name]
            shape = self._introspect(type_name)
            if self._introspection_failed or not shape.fields:
                # Introspection failed: cannot assess drift; do not fabricate an
                # ABORT (a transient introspection blip must not stop the run).
                drift.per_type[type_name] = TypeDrift(
                    type_name=type_name, verdict=DriftVerdict.PROCEED
                )
                continue
            drift.per_type[type_name] = _diff_drift(
                type_name, desired, shape, critical, strict
            )
        return drift
