import json
from typing import List
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)


def _make_config(**overrides: object) -> SnowflakeV2Config:
    # Pinned rather than inherited: these assert exact field paths, so they
    # must not follow the ambient default that the flag-on sweep flips.
    overrides.setdefault("preserve_column_case", False)
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore[arg-type]
        **overrides,  # type: ignore[arg-type]
    )


def _make_identifiers(**overrides: object) -> SnowflakeIdentifierBuilder:
    return SnowflakeIdentifierBuilder(
        identifier_config=_make_config(**overrides),
        structured_reporter=SnowflakeV2Report(),
    )


def _make_table(column_names: List[str]) -> SnowflakeTable:
    return SnowflakeTable(
        name="MY_TABLE",
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
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


def _make_schema_gen(
    report: SnowflakeV2Report, **overrides: object
) -> SnowflakeSchemaGenerator:
    config = _make_config(**overrides)
    return SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=MagicMock(),
        filters=MagicMock(),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        domain_registry=None,
        profiler=None,
        aggregator=MagicMock(),
        snowsight_url_builder=None,
    )


class TestColumnIdentifier:
    def test_lowercases_by_default(self) -> None:
        identifiers = _make_identifiers()
        assert identifiers.snowflake_column_identifier("CUSTOMER_ID") == "customer_id"

    def test_preserves_case_when_enabled(self) -> None:
        identifiers = _make_identifiers(preserve_column_case=True)
        assert identifiers.snowflake_column_identifier("CUSTOMER_ID") == "CUSTOMER_ID"

    def test_distinguishes_case_only_collisions_when_enabled(self) -> None:
        identifiers = _make_identifiers(preserve_column_case=True)
        assert identifiers.snowflake_column_identifier(
            "COL"
        ) != identifiers.snowflake_column_identifier("col")

    def test_noop_when_urns_are_not_lowercased(self) -> None:
        # convert_urns_to_lowercase=False already preserves casing, so the flag
        # cannot change the outcome either way.
        for preserve in (True, False):
            identifiers = _make_identifiers(
                convert_urns_to_lowercase=False, preserve_column_case=preserve
            )
            assert identifiers.snowflake_column_identifier("MyCol") == "MyCol"

    @pytest.mark.parametrize("preserve", [True, False])
    def test_dataset_identifiers_are_unaffected(self, preserve: bool) -> None:
        # Column casing must never re-key dataset URNs.
        identifiers = _make_identifiers(preserve_column_case=preserve)
        assert (
            identifiers.get_dataset_identifier("TABLE", "SCHEMA", "DB")
            == "db.schema.table"
        )


class TestCaseOnlyColumnCollisions:
    def test_columns_collapse_and_warn_by_default(self) -> None:
        report = SnowflakeV2Report()
        gen = _make_schema_gen(report)

        schema_metadata = gen.gen_schema_metadata(
            _make_table(["COL", "col", "OTHER"]), "MY_SCHEMA", "MY_DB"
        )

        field_paths = [f.fieldPath for f in schema_metadata.fields]
        # Both columns still produce a field, but they share one path — the data loss
        # this flag exists to surface.
        assert field_paths == ["col", "col", "other"]
        assert len(set(field_paths)) == 2
        assert any(
            "collapsed into a single field path" in (warning.title or "")
            for warning in report.warnings
        )

    def test_columns_stay_distinct_when_preserving_case(self) -> None:
        report = SnowflakeV2Report()
        gen = _make_schema_gen(report, preserve_column_case=True)

        schema_metadata = gen.gen_schema_metadata(
            _make_table(["COL", "col", "OTHER"]), "MY_SCHEMA", "MY_DB"
        )

        field_paths = [f.fieldPath for f in schema_metadata.fields]
        assert field_paths == ["COL", "col", "OTHER"]
        assert len(set(field_paths)) == 3
        # Nothing is lost, so the run stays quiet. The report exists to surface a
        # dropped column, not to narrate a table's quoting conventions.
        assert not list(report.warnings)
        assert not list(report.infos)

    def test_no_warning_without_collisions(self) -> None:
        report = SnowflakeV2Report()
        gen = _make_schema_gen(report)

        gen.gen_schema_metadata(
            _make_table(["CUSTOMER_ID", "AMOUNT"]), "MY_SCHEMA", "MY_DB"
        )

        assert not list(report.warnings)


DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_table,PROD)"
)


def _profile_workunit(field_paths: List[str]) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=DATASET_URN,
        aspect=DatasetProfileClass(
            timestampMillis=0,
            fieldProfiles=[
                DatasetFieldProfileClass(fieldPath=path) for path in field_paths
            ],
        ),
    ).as_workunit()


def _restored_paths(profiler_field_paths: List[str], **overrides: object) -> List[str]:
    """Profile paths as emitted, with the rule the connector hands the profiler.

    The connector no longer translates after the fact; it passes
    snowflake_column_identifier in and the profiler applies it at its own
    boundary. So the thing to exercise is that boundary, holding the same rule.
    """
    identifiers = _make_identifiers(**overrides)
    profiler = SQLAlchemyProfiler(
        conn=sa.create_engine("sqlite:///:memory:"),
        report=SQLSourceReport(),
        config=ProfilingConfig(),
        platform="snowflake",
        env="PROD",
        field_path_transform=identifiers.snowflake_column_identifier,
    )
    emitted = profiler._to_emitted_field_paths(
        [DatasetFieldProfileClass(fieldPath=path) for path in profiler_field_paths],
        # Neither is consulted once a transform is supplied -- that is the point
        # of injecting it.
        MagicMock(),
        MagicMock(),
        "db.schema.table",
    )
    return [field.fieldPath for field in emitted]


class TestProfileFieldPathAlignment:
    def test_default_folds_to_the_schema_rule(self) -> None:
        assert _restored_paths(["CUSTOMER_ID", "MixedCol"]) == [
            "customer_id",
            "mixedcol",
        ]

    def test_profile_paths_match_schema_when_preserving_case(self) -> None:
        # The profiler reports the stored name, and with the flag on the schema
        # keeps it too, so the path passes through unchanged.
        assert _restored_paths(["CUSTOMER_ID"], preserve_column_case=True) == [
            "CUSTOMER_ID"
        ]

    def test_case_only_collision_resolves_to_distinct_paths(self) -> None:
        # The profiler reports case-colliding columns under their as-stored names,
        # so both resolve exactly rather than being dropped as ambiguous.
        assert _restored_paths(["COL", "col", "OTHER"], preserve_column_case=True) == [
            "COL",
            "col",
            "OTHER",
        ]

    def test_the_pair_collapses_to_one_profile_by_default(self) -> None:
        # Folded, the two land on one field path, and the schema declares one
        # field for them -- so one profile, not two on the same path.
        assert _restored_paths(["COL", "col", "OTHER"]) == ["col", "other"]

    def test_the_connector_hands_the_profiler_the_column_rule(self) -> None:
        """Guards the injection itself.

        The whole design rests on this one kwarg. Without it the profiler falls
        back to the dialect's normalize_name, which lowercases an all-uppercase
        stored name regardless of config -- wrong whenever the flag is on.
        """
        profiler = SnowflakeProfiler(
            config=_make_config(preserve_column_case=True), report=SnowflakeV2Report()
        )
        module = "datahub.ingestion.source.snowflake.snowflake_profiler"
        with (
            patch(f"{module}.create_engine"),
            patch(f"{module}.inspect"),
            patch(
                "datahub.ingestion.source.sqlalchemy_profiler."
                "sqlalchemy_profiler.SQLAlchemyProfiler"
            ) as built,
        ):
            profiler.get_profiler_instance(db_name="DB")

        transform = built.call_args.kwargs["field_path_transform"]
        assert transform("MixedCol") == "MixedCol"
        assert (
            transform.__func__ is type(profiler.identifiers).snowflake_column_identifier
        )


class TestUsageFieldCounts:
    """Usage field counts name columns too, and land on the same schemaField URNs
    as the schema aspect. They are built from a separate code path, so they can
    drift out of step with it independently.
    """

    @staticmethod
    def _field_paths(columns: List[str], **overrides: object) -> List[str]:
        extractor = SnowflakeUsageExtractor(
            config=_make_config(**overrides),
            report=SnowflakeV2Report(),
            connection=MagicMock(),
            filter=MagicMock(),
            identifiers=_make_identifiers(**overrides),
            redundant_run_skip_handler=None,
        )
        counts = json.dumps([{"col": column, "total": 1} for column in columns])
        return [entry.fieldPath for entry in extractor._map_field_counts(counts)]

    def test_default_lowercases(self) -> None:
        assert self._field_paths(["ORDER_ID", "MixedCol"]) == ["mixedcol", "order_id"]

    def test_preserved_keeps_stored_case(self) -> None:
        assert self._field_paths(
            ["ORDER_ID", "MixedCol"], preserve_column_case=True
        ) == ["MixedCol", "ORDER_ID"]

    def test_case_only_pair_stays_two_distinct_paths(self) -> None:
        assert self._field_paths(["col", "COL"], preserve_column_case=True) == [
            "COL",
            "col",
        ]


class TestCollisionReportMatchesReality:
    """The warning tells operators to enable preserve_column_case. It must only
    fire when the emitted paths actually collapse, which depends on both knobs:
    convert_urns_to_lowercase=False keeps the two spellings apart on its own.
    """

    @pytest.mark.parametrize(
        ("preserve", "lowercase", "collapses"),
        [
            (False, True, True),  # the default: "col" and "COL" fold together
            (False, False, False),  # no lowercasing, so both survive already
            (True, True, False),
            (True, False, False),
        ],
    )
    def test_warns_only_when_paths_actually_collapse(
        self, preserve: bool, lowercase: bool, collapses: bool
    ) -> None:
        report = SnowflakeV2Report()
        gen = _make_schema_gen(
            report,
            preserve_column_case=preserve,
            convert_urns_to_lowercase=lowercase,
        )
        table = _make_table(["col", "COL", "id"])

        paths = [
            f.fieldPath for f in gen.gen_schema_metadata(table, "SCH", "DB").fields
        ]
        assert (len(paths) != len(set(paths))) is collapses

        warned = any(
            "collapsed into a single field path" in (w.title or "")
            for w in report.warnings
        )
        assert warned is collapses, f"warning={warned} but paths collapsed={collapses}"
