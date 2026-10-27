import json
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeColumn,
    SnowflakeDataDictionary,
    SnowflakeSemanticView,
    SnowflakeSemanticViewRelationship,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_semantic_model import (
    SnowflakeSemanticModelMapper,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
    snowflake_identity_key,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    SchemaMetadataClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn

# Semantic-view code threads column references in uppercase so they match across
# the view's own metadata. That is a lookup key, not an identity — these tests pin
# that the emitted field paths carry the stored casing instead, because the
# existing semantic-view tests mock the identifier builder and cannot see it.

MIXED_CASE_COLUMN = "MixedCol"


def _make_gen(report: SnowflakeV2Report, **overrides: Any) -> SnowflakeSchemaGenerator:
    # Pinned rather than inherited: these assert exact field paths and exact report
    # contents, so they must not follow the ambient default that the flag-on sweep
    # flips.
    overrides.setdefault("preserve_column_case", False)
    config = SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore[arg-type]
        **overrides,
    )
    return SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=MagicMock(),
        filters=MagicMock(),
        # A real builder, not a mock: the whole point is what it returns.
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        domain_registry=None,
        profiler=None,
        aggregator=MagicMock(),
        snowsight_url_builder=None,
    )


def _semantic_view(column_names: List[str]) -> SnowflakeSemanticView:
    return SnowflakeSemanticView(
        name="MY_SEMANTIC_VIEW",
        comment=None,
        created=None,
        last_altered=None,
        view_definition=None,
        columns=[
            SnowflakeColumn(
                name=name,
                ordinal_position=i + 1,
                is_nullable=True,
                data_type="TEXT",
                comment=None,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
            )
            for i, name in enumerate(column_names)
        ],
    )


class TestSemanticViewColumnCase:
    @pytest.mark.parametrize(
        ("preserve", "expected"),
        [(False, "mixedcol"), (True, MIXED_CASE_COLUMN)],
    )
    def test_stored_name_becomes_the_field_path(
        self, preserve: bool, expected: str
    ) -> None:
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=preserve)

        # The conversion every semantic-view field path goes through. Snowflake
        # hands back stored names, so this is the whole of it.
        assert gen.snowflake_column_identifier(MIXED_CASE_COLUMN) == expected

    @pytest.mark.parametrize("preserve", [True, False])
    @pytest.mark.parametrize("lowercase_urns", [True, False])
    def test_lineage_field_paths_match_the_schema(
        self, preserve: bool, lowercase_urns: bool
    ) -> None:
        # The invariant behind every casing bug here: a semantic view's lineage
        # anchors on the same field paths its schema declares. Both axes matter —
        # with convert_urns_to_lowercase off the casing survives into the URN, so
        # that is the only combination where a mismatch is actually visible.
        report = SnowflakeV2Report()
        gen = _make_gen(
            report,
            preserve_column_case=preserve,
            convert_urns_to_lowercase=lowercase_urns,
        )
        view = _semantic_view([MIXED_CASE_COLUMN, "AMOUNT"])

        schema_paths = {
            f.fieldPath for f in gen.gen_schema_metadata(view, "SCH", "DB").fields
        }
        lineage_paths = {
            gen.snowflake_column_identifier(col.name) for col in view.columns
        }

        assert lineage_paths == schema_paths

    @pytest.mark.parametrize(
        ("preserve", "expected_columns"),
        [
            # Bucketed by uppercase name, so the pair merges into one column.
            (False, 1),
            # Two real columns, kept apart.
            (True, 2),
        ],
    )
    def test_case_only_columns_split_when_preserving(
        self, preserve: bool, expected_columns: int
    ) -> None:
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            SemanticViewColumnMetadata,
            SnowflakeDataDictionary,
        )

        data_dict = SnowflakeDataDictionary(
            connection=MagicMock(),
            report=SnowflakeV2Report(),
            emit_semantic_model_entities=True,
            preserve_column_case=preserve,
        )
        occurrences = [
            SemanticViewColumnMetadata(
                name=name,
                identity_key=snowflake_identity_key(name, preserve_column_case=False),
                data_type="TEXT",
                comment=None,
                subtype=SemanticViewColumnSubtype.DIMENSION,
                table_name="ORDERS",
                synonyms=[],
                expression=None,
            )
            for name in ("col", "COL")
        ]

        groups = data_dict._group_occurrences_by_case(occurrences)

        assert len(groups) == expected_columns
        if preserve:
            assert sorted(g[0].name for g in groups) == ["COL", "col"]
        else:
            # Merged, not dropped: one entry holding both occurrences, which is
            # what collapses them to a single field path downstream instead of
            # emitting two fields that share one.
            assert [o.name for o in groups[0]] == ["col", "COL"]


class TestColumnExistenceCheck:
    """`_declared_field_path` gates direct vs. derived column lineage, and names
    the upstream field.

    It looks columns up in the schema resolver, which keys on the emitted field
    path. That path's casing follows the identifier config, so a check hard-coded
    to one representation reports every column missing under the others. Because
    the same answer names the URN, a case-folded hit has to hand back the
    schema's spelling rather than the reference's.
    """

    @staticmethod
    def _gen_with_schema(
        field_paths: List[str], **overrides: Any
    ) -> SnowflakeSchemaGenerator:
        gen = _make_gen(SnowflakeV2Report(), **overrides)
        assert isinstance(gen.aggregator, MagicMock)
        gen.aggregator._schema_resolver._resolve_schema_info.return_value = {
            path: "VARCHAR" for path in field_paths
        }
        return gen

    @pytest.mark.parametrize(
        "field_path,overrides",
        [
            ("order_id", {}),
            ("ORDER_ID", {"preserve_column_case": True}),
            ("ORDER_ID", {"convert_urns_to_lowercase": False}),
            (MIXED_CASE_COLUMN, {"preserve_column_case": True}),
        ],
        ids=["default", "preserve_column_case", "no_lowercase", "mixed_case"],
    )
    def test_column_found_whatever_the_emitted_casing(
        self, field_path: str, overrides: Any
    ) -> None:
        gen = self._gen_with_schema([field_path], **overrides)

        # A reference written unquoted in the view's DDL comes back folded up,
        # so it matches none of these paths exactly except by luck. What comes
        # back is the schema's spelling, which is what the URN must carry.
        assert (
            gen._declared_field_path("DB", "SCHEMA", "TBL", field_path.upper())
            == field_path
        )

    def test_absent_column_still_reports_missing(self) -> None:
        gen = self._gen_with_schema(["order_id"])

        assert gen._declared_field_path("DB", "SCHEMA", "TBL", "NO_SUCH") is None

    def test_a_folded_hit_never_names_a_path_the_schema_lacks(self) -> None:
        """The failure this returns a path to prevent.

        With preserve_column_case on, snowflake_column_identifier is the identity,
        so citing the reference's own spelling after a case-folded match builds a
        schemaField URN against a path nothing declares. Both halves look correct
        in isolation, and the lineage just quietly points at nothing.
        """
        gen = self._gen_with_schema(["col"], preserve_column_case=True)

        resolved = gen._declared_field_path("DB", "SCHEMA", "TBL", "COL")

        assert resolved == "col"
        assert gen.snowflake_column_identifier("COL") == "COL"

    def test_an_unresolvable_schema_falls_back_to_this_run_s_naming(self) -> None:
        # Fail open: the table may simply not be ingested yet, and dropping the
        # lineage would be worse than naming it the way this run names columns.
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=True)
        assert isinstance(gen.aggregator, MagicMock)
        gen.aggregator._schema_resolver._resolve_schema_info.return_value = {}

        assert gen._declared_field_path("DB", "SCHEMA", "TBL", "MixedCol") == "MixedCol"


class TestSemanticViewJsonProps:
    """Subtype and synonym maps are keyed by the column's stored name, in both
    flag states -- see _process_column_occurrences, which sets column_key from
    occurrences[0].name unconditionally. The lookup uses that same name.
    """

    @staticmethod
    def _props(col_name: str, subtypes: Any, synonyms: Any) -> Any:
        gen = _make_gen(SnowflakeV2Report())
        raw = gen._build_json_props(col_name, subtypes, synonyms)
        return None if raw is None else json.loads(raw)

    def test_exact_key_supplies_both(self) -> None:
        assert self._props(
            MIXED_CASE_COLUMN,
            {MIXED_CASE_COLUMN: "DIMENSION"},
            {MIXED_CASE_COLUMN: ["alias_one"]},
        ) == {"columnSubType": "DIMENSION", "synonyms": ["alias_one"]}

    def test_a_column_the_maps_do_not_mention_yields_nothing(self) -> None:
        assert self._props(MIXED_CASE_COLUMN, {"OTHER": "FACT"}, {}) is None


class TestLogicalDatasetFieldPath:
    """The one field path in the connector that is not the stored name.

    Semantic-model logical datasets are built by the mapper rather than
    gen_schema_metadata, and it has always uppercased their paths. That has to
    stay true by default or every one of those schemaField URNs re-keys; it has
    to stop being true when preserve_column_case is on, or the mapper's paths
    disagree with the lineage anchored on them. Deleting the branch satisfied the
    whole suite before this test existed.
    """

    @staticmethod
    def _path(column_name: str, **overrides: Any) -> str:
        overrides.setdefault("preserve_column_case", False)
        config = SnowflakeV2Config(
            account_id="a",
            username="u",
            password="p",  # type: ignore[arg-type]
            **overrides,
        )
        identifiers = SnowflakeIdentifierBuilder(config, SnowflakeV2Report())
        return identifiers.logical_dataset_field_path(column_name)

    def test_default_uppercases_then_folds_as_before(self) -> None:
        # Uppercased first, then lowercased by convert_urns_to_lowercase -- the
        # historical path, and why the default output is byte-identical.
        assert self._path(MIXED_CASE_COLUMN) == MIXED_CASE_COLUMN.lower()

    def test_preserving_keeps_the_stored_spelling(self) -> None:
        assert self._path(MIXED_CASE_COLUMN, preserve_column_case=True) == (
            MIXED_CASE_COLUMN
        )

    def test_without_lowercasing_the_default_still_uppercases(self) -> None:
        # The combination the docs get wrong: with both off, this path is not the
        # stored name. It is the uppercased one.
        assert self._path(MIXED_CASE_COLUMN, convert_urns_to_lowercase=False) == (
            MIXED_CASE_COLUMN.upper()
        )


class TestDerivedColumnsKeepTheirSpelling:
    """Derived columns take the path through _process_unassociated_columns.

    Its input is already the column's stored name. Uppercasing it there and
    resolving it back collapses a case-only pair: both members fold to the same
    key, and the resolution hands back whichever matched first, so one column's
    lineage anchors on the other's field path.
    """

    def test_a_case_only_derived_pair_resolves_to_two_paths(self) -> None:
        gen = _make_gen(SnowflakeV2Report())
        view = _semantic_view(["col", "COL"])
        # Derived: they carry an expression and no table mapping of their own.
        for column in view.columns:
            column.expression = f"\"{column.name}\" || 'x'"
        view.column_table_mappings = {}
        view.column_occurrences = {
            name: [
                SemanticViewColumnMetadata(
                    name=name,
                    identity_key=snowflake_identity_key(
                        name, preserve_column_case=False
                    ),
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="SRC",
                    synonyms=[],
                    expression=f'"{name}"',
                )
            ]
            for name in ("col", "COL")
        }

        seen: List[str] = []

        def resolver(column_name: str, logical_table: Any) -> str:
            seen.append(column_name)
            return f"urn:li:schemaField:(urn:li:dataset:(x,y,PROD),{column_name})"

        gen._process_unassociated_columns(
            view, "urn:li:dataset:(x,y,PROD)", [], downstream_urn_resolver=resolver
        )

        assert sorted(seen) == ["COL", "col"], (
            f"each derived column must keep its own spelling, got {seen}"
        )


class TestJoinKeysResolveToDimensionNames:
    """A relationship key names the base-table column; a logical dataset's field
    is named after the dimension defined over it. Verified on a live account that
    these can differ in casing:

        DIMENSIONS   ( chi."fkcol" AS "FkCol" )   -> dimension name 'fkcol'
        RELATIONSHIPS( rel AS chi ("FkCol") ... ) -> foreign_keys ['FkCol']

    Emitting the join key unresolved anchors the relationship on a field path the
    logical dataset never declares.
    """

    @staticmethod
    def _view_with_diverging_dimension() -> SnowflakeSemanticView:
        view = _semantic_view(["fkcol"])
        view.column_occurrences = {
            "fkcol": [
                SemanticViewColumnMetadata(
                    name="fkcol",
                    identity_key=snowflake_identity_key(
                        "fkcol", preserve_column_case=False
                    ),
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="CHI",
                    synonyms=[],
                    expression='"FkCol"',
                )
            ]
        }
        return view

    def test_join_key_resolves_to_the_dimension_spelling(self) -> None:
        view = self._view_with_diverging_dimension()

        assert view.dimension_name_for_join_key("FkCol", "CHI") == "fkcol"

    def test_unresolvable_key_passes_through(self) -> None:
        # A dimension renamed outright cannot be matched by name; unchanged is
        # the same behaviour as before the resolution existed.
        view = self._view_with_diverging_dimension()

        assert view.dimension_name_for_join_key("NO_SUCH", "CHI") == "NO_SUCH"

    def test_scoped_to_the_logical_table(self) -> None:
        view = self._view_with_diverging_dimension()

        assert view.dimension_name_for_join_key("FkCol", "OTHER_TABLE") == "FkCol"

    def test_resolution_does_not_depend_on_the_dict_key(self) -> None:
        # column_occurrences keys are labels: _process_column_occurrences sets
        # each to occurrences[0].name, so a consumer that reads the key instead
        # of the occurrence's own name is reading a copy that can drift. Refiling
        # the group under a different label must not change what resolves.
        # Most fixtures in this suite key their groups differently from the names
        # inside them, so without this the drift is invisible to the tests.
        view = self._view_with_diverging_dimension()
        view.column_occurrences = {
            "A_LABEL_NOTHING_SHOULD_MATCH_ON": next(
                iter(view.column_occurrences.values())
            )
        }

        assert view.dimension_name_for_join_key("FkCol", "CHI") == "fkcol"


class TestCaseOnlyPairOnOneTable:
    """The scenario the flag exists for, and the one the suite kept missing.

    Every other test here separates same-named columns by logical table, which
    is what the resolution was designed around. A case-only pair sits on ONE
    table, so scoping cannot tell them apart -- and folding the lookup made both
    candidates match, so the first entry won whichever was asked for. The live
    fixture missed it too: its derived column references the first of the pair,
    so first-wins happened to be right.
    """

    @staticmethod
    def _view() -> SnowflakeSemanticView:
        view = _semantic_view(["col", "COL"])
        view.column_occurrences = {
            name: [
                SemanticViewColumnMetadata(
                    name=name,
                    identity_key=snowflake_identity_key(
                        name, preserve_column_case=False
                    ),
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="T",
                    synonyms=[],
                    expression=f"EXPR_{name}",
                )
            ]
            for name in ("col", "COL")
        }
        return view

    @pytest.mark.parametrize("asked", ["col", "COL"])
    def test_each_spelling_resolves_to_its_own_occurrence(self, asked: str) -> None:
        occurrences = self._view().occurrences_for(asked)

        # The sibling may follow, so a caller scoping by another logical table can
        # still reach it -- but the spelling that was asked for comes first.
        assert occurrences[0].name == asked
        assert occurrences[0].expression == f"EXPR_{asked}"

    @pytest.mark.parametrize("asked", ["col", "COL"])
    def test_each_spelling_resolves_to_its_own_join_key(self, asked: str) -> None:
        assert self._view().dimension_name_for_join_key(asked, "T") == asked

    def test_a_variant_on_another_table_stays_reachable(self) -> None:
        """The opposite failure to the one above, and the reason this orders
        rather than filters.

        Returning only the exact match drops lineage when the variant lives on a
        different logical table: the caller scoping by table finds nothing behind
        the exact hit and falls through to the raw reference.
        """
        view = _semantic_view(["FkCol"])
        view.column_occurrences = {
            name: [
                SemanticViewColumnMetadata(
                    name=name,
                    identity_key=snowflake_identity_key(
                        name, preserve_column_case=False
                    ),
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name=table,
                    synonyms=[],
                    expression=None,
                )
            ]
            for name, table in (("FkCol", "A"), ("fkcol", "B"))
        }

        assert view.dimension_name_for_join_key("FkCol", "A") == "FkCol"
        assert view.dimension_name_for_join_key("FkCol", "B") == "fkcol"

    def test_a_folded_reference_still_finds_the_pair(self) -> None:
        # No exact hit, so the fold still runs -- that path is what makes an
        # unquoted DDL reference resolve at all.
        occurrences = self._view().occurrences_for("Col")

        assert sorted(o.name for o in occurrences) == ["COL", "col"]


class TestLegacyDerivedExpressionLookup:
    """Legacy semantic-view mode leaves column_occurrences empty, so the
    expression lookup falls through to the merged columns list. That fallback
    compares against a stored name, which an uppercased comparison never matches
    for a mixed- or lower-case column -- the lineage would just be skipped.
    """

    @pytest.mark.parametrize("column", ["MixedCol", "UPPER_COL", "lower_col"])
    def test_expression_resolves_whatever_the_stored_casing(self, column: str) -> None:
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=True)
        view = _semantic_view([column])
        view.column_occurrences = {}
        for col in view.columns:
            col.expression = f'"{col.name}"'

        # The exact expression, not just truthiness -- a non-None check would
        # pass even if the lookup returned a sibling column's expression.
        assert gen._semantic_column_expression(view, column, "SRC") == f'"{column}"'


class TestExpressionColumnFolding:
    """Derived-column lineage starts by parsing the expression. Uppercasing every
    reference makes "col" and "COL" indistinguishable, so the upstream edge lands
    on whichever of the pair is found first. Snowflake folds an unquoted
    reference and leaves a quoted one alone; the parser has to do the same.
    """

    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            # Quoted: already the stored spelling, so leave it.
            ("\"col\" || 'x'", "col"),
            ("\"COL\" || 'x'", "COL"),
            ("\"MixedCol\" || 'x'", "MixedCol"),
            # Unquoted: Snowflake folds it up, so we must too.
            ("col || 'x'", "COL"),
            ("MixedCol || 'x'", "MIXEDCOL"),
        ],
    )
    def test_reference_folding_matches_snowflake(
        self, expression: str, expected: str
    ) -> None:
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=True)

        assert gen._extract_columns_from_expression(expression) == [(None, expected)]

    def test_a_case_only_pair_stays_two_references(self) -> None:
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=True)

        assert gen._extract_columns_from_expression('"col" || "COL"') == [
            (None, "col"),
            (None, "COL"),
        ]


class TestMetricNamesFollowTheColumnRule:
    """A metric name is a semantic-view identifier like a dimension's.

    Both arrive through one extraction into one SemanticViewColumnCollection,
    differing only by subtype, so they fold the same way. Keying metrics by
    .upper() instead collapsed two case-only metrics into one entity even where
    their URNs were distinct -- and let a dimension "col" mark a metric "COL"
    as shadowed, which are two different things once casing is preserved.
    """

    @staticmethod
    def _urns(**overrides: Any) -> set:
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=SnowflakeV2Config(
                account_id="a",
                username="u",
                password="p",  # type: ignore[arg-type]
                **overrides,
            ),
            structured_reporter=SnowflakeV2Report(),
        )
        return {
            identifiers.gen_metric_urn(name, "V", "SCH", "DB", "T")
            for name in ("col", "COL")
        }

    def test_default_still_collapses_them(self) -> None:
        # Unchanged from before the flag existed: one URN, so one entity.
        assert len(self._urns(preserve_column_case=False)) == 1

    def test_preserving_keeps_them_apart(self) -> None:
        assert len(self._urns(preserve_column_case=True)) == 2

    def test_lowercasing_off_keeps_them_apart_too(self) -> None:
        # The pre-existing drop: distinct URNs, but .upper() keying meant only
        # one metric entity was ever emitted.
        assert len(self._urns(convert_urns_to_lowercase=False)) == 2

    def test_a_shadowed_sibling_does_not_block_a_distinct_metric(self) -> None:
        """Shadowing is a property of the spelling that resolves, not of any.

        An unquoted reference has two candidate spellings -- what Snowflake folds
        it to, and what was written. Testing both against the shadow set lets a
        dimension "col" block the reference from ever reaching a distinct metric
        "COL", the exact pair _shadowed_metric_names keeps separate.
        """
        config = SnowflakeV2Config(
            account_id="a",
            username="u",
            password="p",  # type: ignore[arg-type]
            preserve_column_case=True,
        )
        report = SnowflakeV2Report()
        mapper = SnowflakeSemanticModelMapper(
            config=config,
            report=report,
            identifiers=SnowflakeIdentifierBuilder(config, report),
            domain_registry=None,
        )

        def col(
            name: str, subtype: SemanticViewColumnSubtype, expression: Any = None
        ) -> SemanticViewColumnMetadata:
            return SemanticViewColumnMetadata(
                name=name,
                identity_key=snowflake_identity_key(name, preserve_column_case=True),
                data_type="NUMBER",
                comment=None,
                subtype=subtype,
                table_name=(
                    None if subtype is SemanticViewColumnSubtype.METRIC else "T"
                ),
                synonyms=[],
                expression=expression,
            )

        view = _semantic_view([])
        view.column_occurrences = {
            "col": [col("col", SemanticViewColumnSubtype.DIMENSION)],
            "COL": [col("COL", SemanticViewColumnSubtype.METRIC, "SUM(x)")],
            "Derived": [col("Derived", SemanticViewColumnSubtype.METRIC, "col * 2")],
        }
        distinct = mapper._distinct_metrics(view)
        occurrence = view.column_occurrences["Derived"][0]

        edges = list(
            mapper._derived_from_metrics(
                occurrence=occurrence,
                semantic_view=view,
                table_bound_metrics={},
                view_scoped_metrics={
                    key.name_key: occ
                    for key, occ in distinct.items()
                    if key.logical_table is None
                },
                shadowed_metric_names=mapper._shadowed_metric_names(view),
                schema_name="SCH",
                db_name="DB",
                logical_table=None,
                parsed=mapper._parse_metric_expression(occurrence, view),
            )
        )

        assert len(edges) == 1, "the dimension blocked a metric it does not shadow"


class TestSemanticViewCollisionReport:
    """The report has to fire in the case that loses a column.

    A semantic view's columns are merged per case-insensitive bucket, so with
    the flag off a case-only pair reaches schema generation as a single column.
    Detecting on that list means the report can only ever fire when the flag is
    on -- when both spellings survive and nothing is lost. Exactly backwards.
    """

    @staticmethod
    def _view(*, columns: List[str], collisions: Any = None) -> SnowflakeSemanticView:
        view = _semantic_view(columns)
        if collisions:
            view.column_case_collisions = collisions
        return view

    def test_flag_off_warns_even_though_the_pair_arrives_merged(self) -> None:
        report = SnowflakeV2Report()
        gen = _make_gen(report)

        # One column reaches schema generation; the second spelling survives only
        # in column_case_collisions, recorded before the merge.
        gen._report_column_case_collisions(
            self._view(columns=["col"], collisions={"col": {"col", "COL"}}),
            "db.schema.v",
        )

        assert len(list(report.warnings)) == 1, (
            "a column was dropped and nothing told the operator"
        )

    def test_flag_on_stays_quiet_because_nothing_is_lost(self) -> None:
        report = SnowflakeV2Report()
        gen = _make_gen(report, preserve_column_case=True)

        gen._report_column_case_collisions(
            self._view(columns=["col", "COL"], collisions={"col": {"col", "COL"}}),
            "db.schema.v",
        )

        # Both spellings survive as distinct field paths. There is nothing for an
        # operator to do, and one notice per such table is noise.
        assert not list(report.warnings)
        assert not list(report.infos)

    def test_lowercasing_off_still_loses_the_column(self) -> None:
        """The case a collapsed-paths test misses.

        With convert_urns_to_lowercase off, `col` and `COL` would have emitted two
        distinct paths -- nothing needed to collapse. The bucket merged them
        anyway, so a column is dropped for no reason at all. Deciding from the
        emitted paths says everything is fine here; deciding from what the dataset
        actually declares does not.
        """
        report = SnowflakeV2Report()
        gen = _make_gen(report, convert_urns_to_lowercase=False)

        # One column survived the merge, as the bucketing produces in this config.
        gen._report_column_case_collisions(
            self._view(columns=["col"], collisions={"col": {"col", "COL"}}),
            "db.schema.v",
        )

        assert len(list(report.warnings)) == 1

    def test_a_view_without_a_collision_reports_nothing(self) -> None:
        report = SnowflakeV2Report()
        gen = _make_gen(report)

        gen._report_column_case_collisions(
            self._view(columns=["id", "amount"]), "db.schema.v"
        )

        assert not list(report.warnings)
        assert not list(report.infos)


class TestLogicalTableStoredCasing:
    """Logical table names are folded to uppercase on every path, for no recorded
    reason: all five INFORMATION_SCHEMA views report the stored spelling, so
    nothing needed normalising. The fold costs a mangled alias for every
    mixed-case logical table, and collapses a case-only pair outright.
    """

    def test_ddl_parser_folds_only_unquoted_aliases(self) -> None:
        # Snowflake folds an unquoted alias and keeps a quoted one, exactly as it
        # does for columns. The parser tokenizes, so it can tell them apart.
        ddl = (
            'CREATE SEMANTIC VIEW v TABLES ("Orders" AS db.sch.o1, plain AS db.sch.o2)'
        )

        parsed = SnowflakeDataDictionary._parse_base_tables_from_ddl(ddl)

        assert set(parsed) == {"Orders", "PLAIN"}

    def test_population_keeps_both_spellings(self) -> None:
        connection = MagicMock()
        connection.query.return_value = [
            {
                "SEMANTIC_VIEW_SCHEMA": "SCH",
                "SEMANTIC_VIEW_NAME": "V",
                "SEMANTIC_TABLE_NAME": name,
                "BASE_TABLE_CATALOG": "DB",
                "BASE_TABLE_SCHEMA": "SCH",
                "BASE_TABLE_NAME": base,
            }
            for name, base in (("orders", "orders_tbl"), ("ORDERS", "ORDERS_TBL"))
        ]
        data_dict = SnowflakeDataDictionary(
            connection=connection,
            report=SnowflakeV2Report(),
            emit_semantic_model_entities=True,
            preserve_column_case=False,
        )
        view = _semantic_view(["col"])
        view.name = "V"

        data_dict._populate_semantic_view_base_tables("DB", {"SCH": [view]})

        assert set(view.logical_to_physical_table) == {"orders", "ORDERS"}

    def test_alias_carries_the_stored_casing(self) -> None:
        mapper = SnowflakeSemanticModelMapper(
            config=_make_gen(SnowflakeV2Report()).config,
            report=SnowflakeV2Report(),
            identifiers=_make_gen(SnowflakeV2Report()).identifiers,
        )
        view = _semantic_view(["col"])
        view.logical_to_physical_table = {"Orders": ("DB", "SCH", "ORDERS_TBL")}

        aliases = [
            wu.metadata.aspect.alias
            for wu in mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=[],
            )
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SemanticModelPropertiesClass)
        ]

        assert aliases == ["Orders"]

    @pytest.mark.parametrize(
        ("convert_urns_to_lowercase", "expected_aliases", "expected_warnings"),
        [
            # The URN lowercases the name, so both spellings resolve to one
            # dataset. Emit it once -- the first declared -- and say so, rather
            # than writing both tables' schema, alias and lineage to it with the
            # last one winning.
            (True, ["orders"], 1),
            # Casing survives into the URN, so they are genuinely two datasets.
            (False, ["orders", "ORDERS"], 0),
        ],
    )
    def test_case_only_pair_emits_one_dataset_per_urn(
        self,
        convert_urns_to_lowercase: bool,
        expected_aliases: List[str],
        expected_warnings: int,
    ) -> None:
        report = SnowflakeV2Report()
        gen = _make_gen(report, convert_urns_to_lowercase=convert_urns_to_lowercase)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=report, identifiers=gen.identifiers
        )
        view = _semantic_view(["col"])
        view.logical_to_physical_table = {
            "orders": ("DB", "SCH", "orders_tbl"),
            "ORDERS": ("DB", "SCH", "ORDERS_TBL"),
        }

        aliases = [
            wu.metadata.aspect.alias
            for wu in mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=[],
            )
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SemanticModelPropertiesClass)
        ]

        # Which one survived, not just how many: the warning promises the first
        # is kept, and a count passes just as happily if the wrong one won or the
        # alias came out mangled.
        assert aliases == expected_aliases
        assert len(list(report.warnings)) == expected_warnings

    def test_primary_key_columns_follow_the_column_rule(self) -> None:
        # Verified against Snowflake: a PK on a quoted column reports
        # PRIMARY_KEYS=["My_Key"], FOREIGN_KEYS/REF_KEYS agree, and the column can
        # only be selected as "My_Key" -- unquoted My_Key is an invalid identifier.
        # So the PK set must keep the stored spelling when casing is preserved,
        # or isPartOfKey lands on the wrong field of a case-only pair.
        data_dict = SnowflakeDataDictionary(
            connection=MagicMock(),
            report=SnowflakeV2Report(),
            emit_semantic_model_entities=True,
            preserve_column_case=True,
        )

        assert data_dict._parse_unique_key_sets('[["My_Key"]]', "ctx") == [{"My_Key"}]

    def test_is_part_of_key_matches_the_stored_spelling(self) -> None:
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=True)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=gen.report, identifiers=gen.identifiers
        )
        view = _semantic_view(["col"])
        view.logical_to_physical_table = {"T": ("DB", "SCH", "T_TBL")}
        view.primary_key_columns_by_table = {"T": {"My_Key"}}
        view.column_occurrences = {
            name: [
                SemanticViewColumnMetadata(
                    name=name,
                    identity_key=snowflake_identity_key(
                        name, preserve_column_case=True
                    ),
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="T",
                    synonyms=[],
                    expression=None,
                )
            ]
            for name in ("My_Key", "MY_KEY")
        }

        fields = {
            f.fieldPath: f.isPartOfKey for f in mapper._build_schema_fields(view, "T")
        }

        # Only the column the key actually names.
        assert fields == {"My_Key": True, "MY_KEY": False}

    def test_legacy_schema_marks_a_mixed_case_primary_key(self) -> None:
        # The flat primary_key_columns set now keeps the stored spelling when
        # casing is preserved, and legacy gen_schema_metadata consumes that set.
        # Uppercasing on only one side leaves mixed-case keys unmarked.
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=True)
        view = _semantic_view(["My_Key", "other"])
        view.primary_key_columns = {"My_Key"}

        fields = {
            f.fieldPath: f.isPartOfKey
            for f in gen.gen_schema_metadata(view, "SCH", "DB").fields
        }

        assert fields == {"My_Key": True, "other": False}

    def test_metrics_of_a_discarded_logical_table_are_not_emitted(self) -> None:
        # When two logical tables collapse onto one dataset URN, the second gets
        # no dataset -- so its table-bound metrics must not be emitted either, or
        # they reference a model that never lists their dataset.
        report = SnowflakeV2Report()
        # Pinned, not inherited: the whole assertion depends on the two URNs
        # colliding, which is what lowercasing does.
        gen = _make_gen(report, convert_urns_to_lowercase=True)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=report, identifiers=gen.identifiers
        )
        view = _semantic_view(["col"])
        view.logical_to_physical_table = {
            "orders": ("DB", "SCH", "lower_tbl"),
            "ORDERS": ("DB", "SCH", "UPPER_TBL"),
        }
        view.column_occurrences = {
            table: [
                SemanticViewColumnMetadata(
                    name=f"m_{table}",
                    identity_key=snowflake_identity_key(
                        f"m_{table}", preserve_column_case=False
                    ),
                    data_type="NUMBER",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.METRIC,
                    table_name=table,
                    synonyms=[],
                    expression="COUNT(1)",
                )
            ]
            for table in ("orders", "ORDERS")
        }

        metrics = [
            wu.metadata.aspect.name
            for wu in mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=[],
            )
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, MetricInfoClass)
        ]

        # Only the surviving table's metric.
        assert metrics == ["m_orders"]

    def test_no_derived_edge_to_a_discarded_table_s_metric(self) -> None:
        # A metric on a URN-collided logical table is never emitted, so a
        # reference to it must not produce a derivedFrom edge -- the destination
        # entity does not exist, and derivedFrom is isLineage.
        report = SnowflakeV2Report()
        gen = _make_gen(report, convert_urns_to_lowercase=True)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=report, identifiers=gen.identifiers
        )
        view = _semantic_view(["col"])
        view.logical_to_physical_table = {
            "orders": ("DB", "SCH", "lower_tbl"),
            "ORDERS": ("DB", "SCH", "UPPER_TBL"),
        }
        view.column_occurrences = {
            "kept": [
                SemanticViewColumnMetadata(
                    name="kept",
                    identity_key=snowflake_identity_key(
                        "kept", preserve_column_case=False
                    ),
                    data_type="NUMBER",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.METRIC,
                    table_name="orders",
                    synonyms=[],
                    expression="COUNT(1)",
                )
            ],
            "gone": [
                SemanticViewColumnMetadata(
                    name="gone",
                    identity_key=snowflake_identity_key(
                        "gone", preserve_column_case=False
                    ),
                    data_type="NUMBER",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.METRIC,
                    table_name="ORDERS",
                    synonyms=[],
                    expression="COUNT(1)",
                )
            ],
            "derived": [
                SemanticViewColumnMetadata(
                    name="derived",
                    identity_key=snowflake_identity_key(
                        "derived", preserve_column_case=False
                    ),
                    data_type="NUMBER",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.METRIC,
                    table_name="orders",
                    synonyms=[],
                    expression='ORDERS."gone" * 2',
                )
            ],
        }

        workunits = list(
            mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=[],
            )
        )
        emitted = {
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, MetricInfoClass)
        }
        edges = [
            d.destinationUrn
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, MetricRelationshipsClass)
            for d in wu.metadata.aspect.derivedFrom
        ]

        # Whatever edges exist must point at metrics that were actually emitted.
        assert set(edges) <= emitted, f"dangling: {set(edges) - emitted}"

    @pytest.mark.parametrize(
        ("preserve", "expected"), [(False, "MY_COL"), (True, "My_Col")]
    )
    def test_extraction_stores_the_identity_key(
        self, preserve: bool, expected: str
    ) -> None:
        # Computed once, where the flag is known. Consumers read it rather than
        # re-deriving it, which is where every fold bug on this branch started.
        connection = MagicMock()
        connection.query.return_value = [
            {
                "SEMANTIC_VIEW_SCHEMA": "SCH",
                "SEMANTIC_VIEW_NAME": "V",
                "NAME": "My_Col",
                "TABLE_NAME": "T",
                "DATA_TYPE": "TEXT",
            }
        ]
        data_dict = SnowflakeDataDictionary(
            connection=connection,
            report=SnowflakeV2Report(),
            emit_semantic_model_entities=True,
            preserve_column_case=preserve,
        )
        collection: Any = {}
        data_dict._fetch_semantic_columns(
            "DB",
            collection,
            SemanticViewColumnSubtype.DIMENSION,
            lambda db: "SELECT 1",
            "TEXT",
        )

        occurrence = collection[("SCH", "V")].columns["MY_COL"][0]

        assert occurrence.name == "My_Col"
        assert occurrence.identity_key == expected

    def test_no_lineage_onto_a_field_the_dataset_does_not_declare(self) -> None:
        # A logical table discarded for a URN collision still has columns, and the
        # producer anchors their FGLs on the URN -- which is the survivor's. Those
        # land on field paths the survivor never declares, so the lineage points
        # at nothing. Metrics are already filtered; lineage was not.
        report = SnowflakeV2Report()
        gen = _make_gen(report, convert_urns_to_lowercase=True)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=report, identifiers=gen.identifiers
        )
        view = _semantic_view(["col"])
        view.logical_to_physical_table = {
            "orders": ("DB", "SCH", "LOW"),
            "ORDERS": ("DB", "SCH", "UP"),
        }
        view.column_occurrences = {
            name: [
                SemanticViewColumnMetadata(
                    name=name,
                    identity_key=snowflake_identity_key(
                        name, preserve_column_case=False
                    ),
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name=table,
                    synonyms=[],
                    expression=None,
                )
            ]
            for name, table in (("a_low", "orders"), ("b_up", "ORDERS"))
        }
        urns = mapper._build_logical_dataset_urns(view, "SCH", "DB")
        fgls = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[f"urn:li:schemaField:(urn:li:dataset:(x,y,PROD),{c})"],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[
                    make_schema_field_urn(
                        mapper.identifiers.gen_semantic_model_dataset_urn(
                            view.name, t, "SCH", "DB"
                        ),
                        mapper.identifiers.logical_dataset_field_path(c),
                    )
                ],
            )
            for c, t in (("a_low", "orders"), ("b_up", "ORDERS"))
        ]

        workunits = list(
            mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=fgls,
            )
        )

        declared = {
            f.fieldPath
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SchemaMetadataClass)
            for f in wu.metadata.aspect.fields
        }
        routed = {
            SchemaFieldUrn.from_string(d).field_path
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, UpstreamLineageClass)
            for fg in (wu.metadata.aspect.fineGrainedLineages or [])
            for d in (fg.downstreams or [])
        }

        assert routed <= declared, (
            f"lineage onto undeclared fields: {routed - declared}"
        )
        assert len(list(urns)) == 1

    def test_no_lineage_onto_a_dataset_that_declares_no_fields(self) -> None:
        # A logical table with no columns emits no schemaMetadata at all, so there
        # is no field for lineage to land on. "Declares nothing" has to be treated
        # the same as "does not declare this field": a check scoped to datasets that
        # emitted a schema skips this shape entirely, and the edge survives unseen.
        report = SnowflakeV2Report()
        gen = _make_gen(report, convert_urns_to_lowercase=False)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=report, identifiers=gen.identifiers
        )
        view = _semantic_view([])
        view.logical_to_physical_table = {"Orders": ("DB", "SCH", "T")}
        view.column_occurrences = {}
        dataset_urn = mapper.identifiers.gen_semantic_model_dataset_urn(
            view.name, "Orders", "SCH", "DB"
        )
        fgls = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=["urn:li:schemaField:(urn:li:dataset:(x,y,PROD),c)"],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(dataset_urn, "ghost_field")],
            )
        ]

        workunits = list(
            mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=fgls,
            )
        )

        routed = {
            SchemaFieldUrn.from_string(d).field_path
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, UpstreamLineageClass)
            for fg in (wu.metadata.aspect.fineGrainedLineages or [])
            for d in (fg.downstreams or [])
        }

        assert routed == set(), (
            f"lineage emitted onto a dataset that declares no fields: {routed}"
        )

    def test_no_relationship_naming_a_table_that_got_no_dataset(self) -> None:
        # Third consumer of the same collision. Metrics on a discarded logical
        # table are skipped and its column lineage is filtered, but relationships
        # name tables by their stored alias -- not by URN -- so a join can still
        # point at a table absent from SemanticModelInfo.datasets and silently
        # never resolve. Aliases are invisible to the URN-based dangling-reference
        # property, which is why this needs its own test.
        report = SnowflakeV2Report()
        gen = _make_gen(report, convert_urns_to_lowercase=True)
        mapper = SnowflakeSemanticModelMapper(
            config=gen.config, report=report, identifiers=gen.identifiers
        )
        view = _semantic_view(["id"])
        view.logical_to_physical_table = {
            "orders": ("DB", "SCH", "LOW"),
            "ORDERS": ("DB", "SCH", "UP"),
        }
        view.relationships = [
            SnowflakeSemanticViewRelationship(
                name="join_to_dropped",
                from_table="orders",
                from_columns=["id"],
                to_table="ORDERS",
                to_columns=["id"],
            )
        ]

        workunits = list(
            mapper.gen_workunits(
                semantic_view=view,
                schema_name="SCH",
                db_name="DB",
                fine_grained_lineages=[],
            )
        )

        info = next(
            wu.metadata.aspect
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SemanticModelInfoClass)
        )
        aliases = {
            wu.metadata.aspect.alias
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SemanticModelPropertiesClass)
            and wu.metadata.aspect.alias
        }
        named = {r.from_ for r in (info.relationships or [])} | {
            r.to for r in (info.relationships or [])
        }

        assert named <= aliases, (
            f"relationship names a table with no dataset: {sorted(named - aliases)}"
        )
