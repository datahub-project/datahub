import json
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
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
        # Nothing is lost, so this is not a warning — but the pair is still
        # indistinguishable to consumers that match case-insensitively.
        assert not list(report.warnings)
        assert any(
            "differing only by case" in (info.title or "") for info in report.infos
        )

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


def _restored_paths(
    profiler_field_paths: List[str],
    report: Optional[SnowflakeV2Report] = None,
    **overrides: object,
) -> List[str]:
    report = report if report is not None else SnowflakeV2Report()
    profiler = SnowflakeProfiler(config=_make_config(**overrides), report=report)
    restored = list(
        profiler._to_schema_field_paths([_profile_workunit(profiler_field_paths)])
    )
    profile = restored[0].get_aspect_of_type(DatasetProfileClass)
    assert profile is not None and profile.fieldProfiles is not None
    return [f.fieldPath for f in profile.fieldProfiles]


class TestProfileFieldPathAlignment:
    # Alignment under the default configuration is covered by
    # test_snowflake_profile_alignment.py; these cover what the flag changes.

    def test_profile_paths_match_schema_when_preserving_case(self) -> None:
        # The profiler reports the stored name, and with the flag on the schema
        # keeps it too, so the path passes through unchanged.
        assert _restored_paths(["CUSTOMER_ID"], preserve_column_case=True) == [
            "CUSTOMER_ID"
        ]

    def test_case_only_collision_resolves_to_distinct_paths(self) -> None:
        # The profiler reports case-colliding columns under their as-stored names,
        # so both resolve exactly rather than being dropped as ambiguous.
        report = SnowflakeV2Report()
        assert _restored_paths(
            ["COL", "col", "OTHER"],
            report=report,
            preserve_column_case=True,
        ) == ["COL", "col", "OTHER"]
        assert not list(report.warnings)


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
