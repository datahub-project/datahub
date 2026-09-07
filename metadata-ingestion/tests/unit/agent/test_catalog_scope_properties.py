"""Property tests for CatalogScope matching.

This is the component the review round caught out twice, in opposite directions:
first too loose on three-part paths (a user database whose schema was named
ACCOUNT_USAGE read as Snowflake's system view), then too loose on one-part ones
(a bare entry like Oracle's "all_tables" licensing "hr.all_tables"). Both were
asymmetries in how a listed name is matched against a reference, and both were
found by a reviewer rather than by the example-based tests, which only covered
the shapes we had thought of.

The differential test below is the point: `_model_permits` decides the same
question by string-suffix comparison, where the implementation slices lists of
parts. Two formulations that disagree on any generated input mean one of them
has an off-by-one or a boundary error, and it does not matter which -- either
way there is a bug to look at.
"""

import string
from typing import List

from hypothesis import HealthCheck, assume, given, settings, strategies as st

from datahub.ingestion.agent.sql_gate import INFORMATION_SCHEMA, CatalogScope

# No dots: a dot is the separator, so an identifier containing one would be
# describing a different path than the test thinks it is.
_IDENT = st.text(
    alphabet=string.ascii_letters + string.digits + "_", min_size=1, max_size=6
)
_PATH = st.lists(_IDENT, min_size=2, max_size=4)
_ENTRY = st.lists(_IDENT, min_size=1, max_size=3).map(".".join)
_SCOPE = st.builds(
    CatalogScope,
    schemas=st.frozensets(_IDENT, max_size=3),
    relations=st.frozensets(_ENTRY, max_size=6),
    excluded_relations=st.frozensets(_IDENT, max_size=3),
)


def _model_permits(scope: CatalogScope, parts: List[str]) -> bool:
    """The same rule, decided on the rendered path instead of the part list.

    Deliberately not a copy of the implementation: it renders the reference and
    asks whether any *qualified* entry is a dot-delimited suffix of it. The
    "." + entry form is what enforces the segment boundary -- a plain endswith
    would let "xpg_catalog.pg_class" match "pg_catalog.pg_class".
    """
    schema, relation = parts[-2].lower(), parts[-1].lower()
    if schema in {s.lower() for s in scope.schemas}:
        return relation not in {r.lower() for r in scope.excluded_relations}
    dotted = ".".join(p.lower() for p in parts)
    for entry in scope.relations:
        lowered = entry.lower()
        if "." not in lowered:
            # Bare entries name a relation the dialect exposes unqualified;
            # permits_unqualified owns them and they license nothing qualified.
            continue
        if dotted == lowered or dotted.endswith("." + lowered):
            return True
    return False


@settings(max_examples=400, suppress_health_check=[HealthCheck.too_slow])
@given(scope=_SCOPE, parts=_PATH)
def test_two_independent_formulations_agree(
    scope: CatalogScope, parts: List[str]
) -> None:
    assert scope.permits_path(parts) == _model_permits(scope, parts)


@settings(max_examples=200)
@given(bare=_IDENT, prefix=st.lists(_IDENT, min_size=1, max_size=3))
def test_a_bare_entry_never_licenses_a_qualified_reference(
    bare: str, prefix: List[str]
) -> None:
    """The regression suffix matching introduced. A bare entry says "this dialect
    exposes the relation unqualified", not "this name is metadata under any
    schema"."""
    scope = CatalogScope(relations=frozenset({bare}))
    assert not scope.permits_path(prefix + [bare])
    # But it must still be reachable the way it is meant to be.
    assert scope.permits_unqualified(bare)


@settings(max_examples=200)
@given(catalog=_IDENT, schema=_IDENT, relation=_IDENT, foreign=_IDENT)
def test_pinning_the_catalog_refuses_a_lookalike_schema_elsewhere(
    catalog: str, schema: str, relation: str, foreign: str
) -> None:
    """A three-part entry pins the catalog, which is the only way to tell a system
    schema from a user-created one wearing the same name."""
    # assume, not pytest.skip: a skip inside a Hypothesis test abandons the whole
    # property rather than the one example, so the test reports "skipped" and
    # asserts nothing.
    assume(foreign.lower() != catalog.lower())
    scope = CatalogScope(relations=frozenset({f"{catalog}.{schema}.{relation}"}))
    assert scope.permits_path([catalog, schema, relation])
    assert not scope.permits_path([foreign, schema, relation])
    # Two parts cannot be shown to be the pinned catalog either: an unqualified
    # reference resolves against whatever database is current.
    assert not scope.permits_path([schema, relation])


@settings(max_examples=200)
@given(scope=_SCOPE, parts=_PATH)
def test_matching_ignores_case_in_both_directions(
    scope: CatalogScope, parts: List[str]
) -> None:
    """Dialects disagree about identifier case, so a decision must not turn on it."""
    upper = CatalogScope(
        schemas=frozenset(s.upper() for s in scope.schemas),
        relations=frozenset(r.upper() for r in scope.relations),
        excluded_relations=frozenset(r.upper() for r in scope.excluded_relations),
    )
    assert scope.permits_path(parts) == upper.permits_path([p.upper() for p in parts])
    assert scope.permits_path(parts) == upper.permits_path([p.lower() for p in parts])


@settings(max_examples=100)
@given(parts=_PATH)
def test_a_scope_declaring_nothing_permits_nothing(parts: List[str]) -> None:
    """Fail closed: no schemas and no relations means nothing is in scope.

    Spelled out rather than written CatalogScope(), which is NOT empty -- its
    schemas default to {information_schema}. The earlier version of this test
    used the bare constructor and passed only because generated identifiers are
    at most six characters and so can never spell "information_schema": a
    property whose generator cannot reach the interesting input proves nothing.
    The default's own behaviour is asserted separately, below.
    """
    nothing = CatalogScope(schemas=frozenset(), relations=frozenset())
    assert not nothing.permits_path(parts)
    assert not nothing.permits_unqualified(parts[-1])


@settings(max_examples=100)
@given(relation=_IDENT)
def test_the_bare_default_allows_information_schema_and_only_that(
    relation: str,
) -> None:
    """The framework default is a schema-level allow, which is why every source
    inheriting it has to be reviewed. Pinned here so the constructor's meaning
    is not something a reader has to infer."""
    default = CatalogScope()
    assert default.permits_path([INFORMATION_SCHEMA, relation])
    assert not default.permits_path(["pg_catalog", relation])


@settings(max_examples=200)
@given(schema=_IDENT, relation=_IDENT)
def test_an_exclusion_beats_the_schema_that_would_have_allowed_it(
    schema: str, relation: str
) -> None:
    """The exclusion list is the only thing standing between a schema-level allow
    and the query-text views living in that schema."""
    scope = CatalogScope(
        schemas=frozenset({schema}), excluded_relations=frozenset({relation})
    )
    assert not scope.permits_path([schema, relation])
    assert scope.permits_path([schema, relation + "_other"])


@settings(max_examples=200)
@given(entry=st.lists(_IDENT, min_size=2, max_size=3), extra=_IDENT)
def test_an_entry_matches_the_suffix_and_not_the_head_of_a_path(
    entry: List[str], extra: str
) -> None:
    """A listed relation may have segments above it, never below: appending a
    segment names something inside it, which was never listed.

    The assume() excludes paths of repeated identifiers ("a.a" plus "a"), where
    the longer path's suffix genuinely still equals the entry -- a real match,
    not a leak, and the first thing Hypothesis found here.
    """
    lengthened = entry + [extra]
    assume([p.lower() for p in lengthened[-len(entry) :]] != [p.lower() for p in entry])
    scope = CatalogScope(relations=frozenset({".".join(entry)}))
    assert scope.permits_path(entry)
    assert not scope.permits_path(lengthened)
