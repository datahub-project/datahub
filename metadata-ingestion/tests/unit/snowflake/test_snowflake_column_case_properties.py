from typing import Dict, Iterable, List, Set, Tuple, Type, TypeVar

from hypothesis import HealthCheck, given, settings, strategies as st

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeSemanticView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_semantic_model import (
    SnowflakeSemanticModelMapper,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    MetricInfoClass,
    MetricRelationshipsClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
)

_TABLE = "T"
_DB = "DB"
_SCHEMA = "SCH"
# Distinct from every generated name, so the metric doing the deriving can never
# be one of the metrics it references.
_DERIVED = "derived_metric"

_AspectT = TypeVar("_AspectT")

# Deliberately collision-prone: every base name can appear in several casings, so
# a generated schema routinely holds case-only pairs. Real Snowflake fixtures are
# almost all uppercase, which is exactly why hand-written cases keep missing this
# class -- the collision has to be generated on purpose to show up at all.
_BASES = ["col", "amount", "id"]
_CASINGS = [str.lower, str.upper, str.title]

_stored_names = st.lists(
    st.tuples(st.sampled_from(_BASES), st.sampled_from(range(len(_CASINGS)))),
    min_size=1,
    max_size=6,
).map(lambda pairs: sorted({_CASINGS[i](base) for base, i in pairs}))


def _identifiers(
    *, preserve_column_case: bool, convert_urns_to_lowercase: bool
) -> SnowflakeIdentifierBuilder:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "u",
            "password": "p",
            "preserve_column_case": preserve_column_case,
            "convert_urns_to_lowercase": convert_urns_to_lowercase,
        }
    )
    return SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=SnowflakeV2Report()
    )


def _view(stored_names: List[str]) -> SnowflakeSemanticView:
    view = SnowflakeSemanticView(
        name="V", created=None, last_altered=None, comment=None, view_definition=""
    )
    # Tag each column's expression with its own name so the answer identifies
    # which column produced it -- a sibling's answer is otherwise indistinguishable.
    view.column_occurrences = {
        name: [
            SemanticViewColumnMetadata(
                name=name,
                data_type="TEXT",
                comment=None,
                subtype=SemanticViewColumnSubtype.DIMENSION,
                table_name=_TABLE,
                synonyms=[],
                expression=f"EXPR::{name}",
            )
        ]
        for name in stored_names
    }
    return view


_CELLS = [
    (preserve, convert) for preserve in (False, True) for convert in (False, True)
]


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_preserved_casing_never_collapses_two_columns(stored_names: List[str]) -> None:
    """With casing preserved, distinct stored names keep distinct emitted paths.

    A collapse here is what silently merges two real columns into one field, so
    one of them disappears from DataHub without any error.
    """
    for convert in (False, True):
        identifiers = _identifiers(
            preserve_column_case=True, convert_urns_to_lowercase=convert
        )
        emitted = [identifiers.logical_dataset_field_path(n) for n in stored_names]
        assert len(set(emitted)) == len(stored_names), (
            f"convert={convert}: {stored_names} collapsed onto {sorted(set(emitted))}"
        )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_every_column_resolves_to_itself_not_a_sibling(
    stored_names: List[str],
) -> None:
    """Looking a column up returns that column's own data, in every config cell.

    Stated against the emitted path rather than the stored name so it holds with
    the flag off too: there, case-only spellings are deliberately one column, so
    either answer is correct precisely because both emit the same path.
    """
    view = _view(stored_names)

    for preserve, convert in _CELLS:
        identifiers = _identifiers(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )

        for name in stored_names:
            want = identifiers.logical_dataset_field_path(name)

            expression = SnowflakeSchemaGenerator._semantic_column_expression(
                view, name, _TABLE
            )
            assert expression is not None, f"{name} resolved to no expression"
            answered = expression.removeprefix("EXPR::")
            assert identifiers.logical_dataset_field_path(answered) == want, (
                f"preserve={preserve} convert={convert}: asked {name!r}, "
                f"got {answered!r}'s expression"
            )

            resolved = view.dimension_name_for_join_key(name, _TABLE)
            assert identifiers.logical_dataset_field_path(resolved) == want, (
                f"preserve={preserve} convert={convert}: join key {name!r} "
                f"resolved to {resolved!r}"
            )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_identity_key_agrees_with_the_emitted_path(stored_names: List[str]) -> None:
    """Two names share an identity key exactly when they share an emitted path.

    These are separate functions answering separate questions, and the indices
    keyed by one are used to decide what gets emitted under the other. When they
    disagree, one metric splits into two entities or two collapse into one --
    which is the shape of every bug this suite has had.
    """
    for preserve, convert in _CELLS:
        identifiers = _identifiers(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )

        by_identity: Dict[str, Set[str]] = {}
        by_emitted: Dict[str, Set[str]] = {}
        for name in stored_names:
            by_identity.setdefault(identifiers.column_identity_key(name), set()).add(
                name
            )
            by_emitted.setdefault(
                identifiers.logical_dataset_field_path(name), set()
            ).add(name)

        assert sorted(by_identity.values(), key=sorted) == sorted(
            by_emitted.values(), key=sorted
        ), (
            f"preserve={preserve} convert={convert}: identity groups "
            f"{sorted(by_identity.values(), key=sorted)} != emitted groups "
            f"{sorted(by_emitted.values(), key=sorted)}"
        )


def _mapper(
    *, preserve_column_case: bool, convert_urns_to_lowercase: bool
) -> SnowflakeSemanticModelMapper:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "u",
            "password": "p",
            "preserve_column_case": preserve_column_case,
            "convert_urns_to_lowercase": convert_urns_to_lowercase,
        }
    )
    report = SnowflakeV2Report()
    return SnowflakeSemanticModelMapper(
        config=config,
        report=report,
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
    )


def _metric(name: str, expression: str) -> SemanticViewColumnMetadata:
    # View-scoped (no table_name), so references are unqualified and resolve
    # through view_scoped_metrics.
    return SemanticViewColumnMetadata(
        name=name,
        data_type="NUMBER",
        comment=None,
        subtype=SemanticViewColumnSubtype.METRIC,
        table_name=None,
        synonyms=[],
        expression=expression,
    )


def _metric_view(stored_names: List[str], derived_from: str) -> SnowflakeSemanticView:
    view = SnowflakeSemanticView(
        name="V", created=None, last_altered=None, comment=None, view_definition=""
    )
    occurrences: Dict[str, List[SemanticViewColumnMetadata]] = {
        name: [_metric(name, "COUNT(1)")] for name in stored_names
    }
    # Quoted, because that is the only reference Snowflake resolves to a metric
    # whose stored name is not already uppercase.
    occurrences[_DERIVED] = [_metric(_DERIVED, f'"{derived_from}" * 2')]
    view.column_occurrences = occurrences
    return view


def _aspects(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> List[Tuple[str, _AspectT]]:
    """(entity urn, aspect) for every work unit carrying that aspect type."""
    found: List[Tuple[str, _AspectT]] = []
    for workunit in workunits:
        mcp = workunit.metadata
        if not isinstance(mcp, MetadataChangeProposalWrapper):
            continue
        if isinstance(mcp.aspect, aspect_type) and mcp.entityUrn:
            found.append((mcp.entityUrn, mcp.aspect))
    return found


def _workunits(
    mapper: SnowflakeSemanticModelMapper, view: SnowflakeSemanticView
) -> List[MetadataWorkUnit]:
    return list(
        mapper.gen_workunits(
            semantic_view=view,
            schema_name=_SCHEMA,
            db_name=_DB,
            fine_grained_lineages=[],
        )
    )


def _emitted_metric_urns(
    mapper: SnowflakeSemanticModelMapper, view: SnowflakeSemanticView
) -> Set[str]:
    return {urn for urn, _ in _aspects(_workunits(mapper, view), MetricInfoClass)}


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_one_metric_entity_per_distinct_identity(stored_names: List[str]) -> None:
    """Metric entities correspond one-to-one with distinct metric identities.

    Too few means two real metrics merged into one URN and one of them is gone;
    too many means a single metric was emitted twice under different URNs. Both
    have happened here, in opposite config cells.
    """
    view = _metric_view(stored_names, stored_names[0])

    for preserve, convert in _CELLS:
        mapper = _mapper(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )
        identities = {
            mapper.identifiers.column_identity_key(n) for n in [*stored_names, _DERIVED]
        }

        urns = _emitted_metric_urns(mapper, view)

        assert len(urns) == len(identities), (
            f"preserve={preserve} convert={convert}: {len(identities)} distinct "
            f"metric identities among {[*stored_names, _DERIVED]} produced "
            f"{len(urns)} entities"
        )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names, target_index=st.integers(min_value=0))
def test_a_derived_metric_points_at_the_metric_it_names(
    stored_names: List[str], target_index: int
) -> None:
    """A quoted reference resolves to the metric entity that spelling belongs to.

    Never a sibling, and never nothing: derivedFrom is isLineage, so a wrong edge
    is worse than a missing one, and a dropped edge loses the lineage silently.
    """
    target = stored_names[target_index % len(stored_names)]
    view = _metric_view(stored_names, target)

    for preserve, convert in _CELLS:
        mapper = _mapper(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )
        workunits = _workunits(mapper, view)
        derived_urn = mapper.identifiers.gen_metric_urn(
            _DERIVED, view.name, _SCHEMA, _DB
        )
        # The entity the target's spelling actually landed on, read from what was
        # emitted rather than recomputed -- whichever occurrence won the merge
        # under this config, that is the URN the edge has to name.
        want = {
            mapper.identifiers.gen_metric_urn(n, view.name, _SCHEMA, _DB)
            for n in stored_names
            if mapper.identifiers.column_identity_key(n)
            == mapper.identifiers.column_identity_key(target)
        } & _emitted_metric_urns(mapper, view)

        edges = [
            edge.destinationUrn
            for urn, aspect in _aspects(workunits, MetricRelationshipsClass)
            if urn == derived_urn
            for edge in aspect.derivedFrom
        ]

        assert len(edges) == 1, (
            f"preserve={preserve} convert={convert}: reference to {target!r} "
            f"produced {len(edges)} edges"
        )
        assert edges[0] in want, (
            f"preserve={preserve} convert={convert}: reference to {target!r} "
            f"pointed at {edges[0]}, expected one of {sorted(want)}"
        )


# Logical tables are the other name space in a semantic view, and the one the
# column properties above never vary. Generated collision-prone for the same
# reason: `"orders"` and `"ORDERS"` are two legal logical tables over two
# different base tables, and folding them is what silently merged them.
_logical_tables = st.lists(
    st.tuples(st.sampled_from(_BASES), st.sampled_from(range(len(_CASINGS)))),
    min_size=1,
    max_size=4,
).map(lambda pairs: sorted({_CASINGS[i](base) for base, i in pairs}))

_SHARED_METRIC = "total"


def _view_with_logical_tables(logical_tables: List[str]) -> SnowflakeSemanticView:
    view = SnowflakeSemanticView(
        name="V", created=None, last_altered=None, comment=None, view_definition=""
    )
    view.logical_to_physical_table = {
        table: (_DB, _SCHEMA, f"{table}_TBL") for table in logical_tables
    }
    # The same metric name on every logical table, so a fold that merges the
    # tables also merges their metrics -- visible as a count.
    view.column_occurrences = {table: [_metric_on(table)] for table in logical_tables}
    return view


def _metric_on(table: str) -> SemanticViewColumnMetadata:
    return SemanticViewColumnMetadata(
        name=_SHARED_METRIC,
        data_type="NUMBER",
        comment=None,
        subtype=SemanticViewColumnSubtype.METRIC,
        table_name=table,
        synonyms=[],
        expression="COUNT(1)",
    )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(logical_tables=_logical_tables)
def test_one_logical_dataset_per_distinct_urn(logical_tables: List[str]) -> None:
    """Logical datasets correspond one-to-one with the URNs they resolve to.

    Fewer means two tables silently merged; more is impossible. Two tables that
    differ only by case resolve to one URN while lowercasing is on, and that is a
    collapse the connector has to make once and report, not emit twice.
    """
    view = _view_with_logical_tables(logical_tables)

    for preserve, convert in _CELLS:
        mapper = _mapper(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )
        expected = {
            mapper.identifiers.gen_semantic_model_dataset_urn(
                view.name, table, _SCHEMA, _DB
            )
            for table in logical_tables
        }

        emitted = [
            urn
            for urn, _ in _aspects(
                _workunits(mapper, view), SemanticModelPropertiesClass
            )
        ]

        # Counted, not just compared as a set: emitting one dataset twice is the
        # failure this exists to catch, and two sets would look identical.
        assert sorted(emitted) == sorted(expected), (
            f"preserve={preserve} convert={convert}: {logical_tables} -> {emitted}"
        )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(logical_tables=_logical_tables)
def test_every_metric_belongs_to_a_logical_table_that_exists(
    logical_tables: List[str],
) -> None:
    """A metric is only emitted for a logical table that got a dataset.

    A table dropped because its URN was already claimed must take its metrics with
    it, or they reference a semantic model that never lists their dataset. And two
    tables that do get separate datasets must get separate metrics, even when the
    metric name is identical -- folding the table collapsed those into one.
    """
    view = _view_with_logical_tables(logical_tables)

    for preserve, convert in _CELLS:
        mapper = _mapper(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )
        workunits = _workunits(mapper, view)

        retained = {urn for urn, _ in _aspects(workunits, SemanticModelPropertiesClass)}
        expected = {
            mapper.identifiers.gen_metric_urn(
                _SHARED_METRIC, view.name, _SCHEMA, _DB, logical_table=table
            )
            for table in logical_tables
            if mapper.identifiers.gen_semantic_model_dataset_urn(
                view.name, table, _SCHEMA, _DB
            )
            in retained
        }

        metrics = [urn for urn, _ in _aspects(workunits, MetricInfoClass)]

        # Counted for the same reason: a metric belonging to a discarded logical
        # table lands on the surviving table's URN, so a set hides it entirely.
        assert sorted(metrics) == sorted(expected), (
            f"preserve={preserve} convert={convert}: {logical_tables} -> {metrics}"
        )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(logical_tables=_logical_tables)
def test_the_emitted_graph_has_no_dangling_references(
    logical_tables: List[str],
) -> None:
    """Every entity an emitted aspect points at was itself emitted.

    Mechanism-independent, which is the point: the ways a reference can go
    dangling are exactly the ways nobody thought of. Two have happened here --
    a metric index that still held a logical table the emitter had dropped, and
    a model listing a dataset that was never produced -- and both would have
    failed this without anyone naming them.

    Scoped to the semantic model's own graph. Physical base tables are referenced
    by upstreamLineage and deliberately live outside it.
    """
    # Each logical table gets a metric named only for it, so a reference to a
    # discarded table's metric cannot silently land on a survivor's URN. Sharing
    # one metric name across the tables makes the dangling edge invisible: the
    # URNs collide in exactly the configuration where the table is dropped.
    view = _view_with_logical_tables(logical_tables)
    view.column_occurrences = {
        f"m_{table}": [
            SemanticViewColumnMetadata(
                name=f"m_{table}",
                data_type="NUMBER",
                comment=None,
                subtype=SemanticViewColumnSubtype.METRIC,
                table_name=table,
                synonyms=[],
                expression="COUNT(1)",
            )
        ]
        for table in logical_tables
    }
    # One derived metric on the first table, referencing every table's own metric.
    view.column_occurrences["derived"] = [
        SemanticViewColumnMetadata(
            name="derived",
            data_type="NUMBER",
            comment=None,
            subtype=SemanticViewColumnSubtype.METRIC,
            table_name=logical_tables[0],
            synonyms=[],
            expression=" + ".join(f'"{t}"."m_{t}"' for t in logical_tables),
        )
    ]

    for preserve, convert in _CELLS:
        mapper = _mapper(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )
        workunits = _workunits(mapper, view)

        emitted = {urn for urn, _ in _aspects(workunits, MetricInfoClass)} | {
            urn for urn, _ in _aspects(workunits, SemanticModelPropertiesClass)
        }

        referenced = set()
        for _urn, aspect in _aspects(workunits, MetricRelationshipsClass):
            referenced |= {edge.destinationUrn for edge in aspect.derivedFrom}
        for _urn, model_info in _aspects(workunits, SemanticModelInfoClass):
            referenced |= set(model_info.datasets or [])

        assert referenced <= emitted, (
            f"preserve={preserve} convert={convert}: {logical_tables} -> "
            f"dangling {sorted(referenced - emitted)}"
        )
