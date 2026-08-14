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
    semantic_column_field_path,
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
    def test_stored_name_resolves_from_uppercase_key(
        self, preserve: bool, expected: str
    ) -> None:
        gen = _make_gen(SnowflakeV2Report(), preserve_column_case=preserve)
        view = _semantic_view([MIXED_CASE_COLUMN, "AMOUNT"])

        # This is the conversion every semantic-view field path goes through.
        resolved = semantic_column_field_path(
            gen.identifiers, view, MIXED_CASE_COLUMN.upper()
        )
        assert resolved == expected

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
            semantic_column_field_path(gen.identifiers, view, col.name.upper())
            for col in view.columns
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

    def test_unknown_column_falls_back_to_the_key(self) -> None:
        view = _semantic_view([MIXED_CASE_COLUMN])

        assert view.stored_column_name("NOT_A_COLUMN") == "NOT_A_COLUMN"

    def test_same_name_on_two_tables_resolves_per_table(self) -> None:
        # The regression the logical-table argument exists for: one uppercased
        # key, two logical tables, different stored casing on each. An unscoped
        # lookup returns whichever came first and anchors lineage on a field path
        # the other table does not have.
        view = _semantic_view([])
        view.column_occurrences = {
            "col": [
                SemanticViewColumnMetadata(
                    name="col",
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="ORDERS",
                    synonyms=[],
                    expression=None,
                )
            ],
            "COL": [
                SemanticViewColumnMetadata(
                    name="COL",
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="CUSTOMERS",
                    synonyms=[],
                    expression=None,
                )
            ],
        }

        assert view.stored_column_name("COL", "ORDERS") == "col"
        assert view.stored_column_name("COL", "CUSTOMERS") == "COL"

    def test_legacy_mode_resolves_without_occurrences(self) -> None:
        # Legacy dataset mode never populates column_occurrences, so resolution
        # falls through to the column list — where a case-only pair both match
        # case-insensitively and would otherwise share one field path.
        view = _semantic_view(["col", "COL", "OTHER"])
        assert not view.column_occurrences

        assert view.stored_column_name("col") == "col"
        assert view.stored_column_name("COL") == "COL"
        # A reference whose casing matches nothing exactly still resolves, via
        # the case-insensitive fallback rather than the exact-match branch.
        assert view.stored_column_name("other") == "OTHER"

    def test_pair_on_one_table_keeps_both_spellings(self) -> None:
        # Scoping by logical table cannot separate a case-only pair that lives on
        # the same table. Both resolved to whichever came first, so the two fields
        # shared a path and one was dropped downstream — an exact spelling wins.
        view = _semantic_view([])
        view.column_occurrences = {
            name: [
                SemanticViewColumnMetadata(
                    name=name,
                    data_type="TEXT",
                    comment=None,
                    subtype=SemanticViewColumnSubtype.DIMENSION,
                    table_name="SRC",
                    synonyms=[],
                    expression=None,
                )
            ]
            for name in ("col", "COL")
        }

        assert view.stored_column_name("col", "SRC") == "col"
        assert view.stored_column_name("COL", "SRC") == "COL"


class TestColumnExistenceCheck:
    """`_verify_column_exists_in_table` gates direct vs. derived column lineage.

    It looks columns up in the schema resolver, which keys on the emitted field
    path. That path's casing follows the identifier config, so a check hard-coded
    to one representation reports every column missing under the others.
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

        # Semantic-view metadata hands us the uppercase-canonicalized reference,
        # which matches none of these paths exactly except by luck.
        assert gen._verify_column_exists_in_table(
            "DB", "SCHEMA", "TBL", field_path.upper()
        )

    def test_absent_column_still_reports_missing(self) -> None:
        gen = self._gen_with_schema(["order_id"])

        assert not gen._verify_column_exists_in_table("DB", "SCHEMA", "TBL", "NO_SUCH")


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
