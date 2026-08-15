import json
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeColumn,
    SnowflakeSemanticView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)

# Semantic-view code threads column references in uppercase so they match across
# the view's own metadata. That is a lookup key, not an identity — these tests pin
# that the emitted field paths carry the stored casing instead, because the
# existing semantic-view tests mock the identifier builder and cannot see it.

MIXED_CASE_COLUMN = "MixedCol"


def _make_gen(report: SnowflakeV2Report, **overrides: Any) -> SnowflakeSchemaGenerator:
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
    """Subtype and synonym maps are keyed by the column's stored name when casing
    is preserved and by its uppercased form otherwise, so the lookup tries both.
    Picking one keying would silently drop the metadata under the other.
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

    def test_uppercased_key_still_resolves(self) -> None:
        # What the default path produces: the maps arrive uppercased while the
        # column keeps its stored spelling.
        assert self._props(
            MIXED_CASE_COLUMN,
            {MIXED_CASE_COLUMN.upper(): "METRIC"},
            {MIXED_CASE_COLUMN.upper(): ["alias_two"]},
        ) == {"columnSubType": "METRIC", "synonyms": ["alias_two"]}

    def test_absent_under_either_keying_yields_nothing(self) -> None:
        assert self._props(MIXED_CASE_COLUMN, {"OTHER": "FACT"}, {}) is None


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
