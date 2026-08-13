from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeTable,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_table,PROD)"
)


def _make_config(**overrides: Any) -> SnowflakeV2Config:
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore[arg-type]
        **overrides,
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


def _profile_workunit(
    field_paths: Optional[List[str]],
) -> MetadataWorkUnit:
    field_profiles = (
        [DatasetFieldProfileClass(fieldPath=path) for path in field_paths]
        if field_paths is not None
        else None
    )
    return MetadataChangeProposalWrapper(
        entityUrn=DATASET_URN,
        aspect=DatasetProfileClass(
            timestampMillis=0, rowCount=5, fieldProfiles=field_profiles
        ),
    ).as_workunit()


def _restore(
    profiler_field_paths: Optional[List[str]],
    table_columns: List[str],
    report: Optional[SnowflakeV2Report] = None,
) -> Optional[List[str]]:
    report = report if report is not None else SnowflakeV2Report()
    profiler = SnowflakeProfiler(config=_make_config(), report=report)
    name_map: Dict[str, str] = profiler._build_column_name_map(
        _make_table(table_columns)
    )
    restored = list(
        profiler._restore_column_case(
            [_profile_workunit(profiler_field_paths)], {DATASET_URN: name_map}
        )
    )
    profile = restored[0].get_aspect_of_type(DatasetProfileClass)
    assert profile is not None
    if profile.fieldProfiles is None:
        return None
    return [f.fieldPath for f in profile.fieldProfiles]


class TestProfileFieldPathAlignment:
    def test_mixed_case_column_aligns_with_schema(self) -> None:
        # snowflake-sqlalchemy preserves `MixedCol` while the schema lowercases it,
        # so without the rewrite the profile never attaches to its field.
        assert _restore(["MixedCol"], ["MixedCol"]) == ["mixedcol"]

    def test_uppercase_column_is_unchanged(self) -> None:
        assert _restore(["customer_id"], ["CUSTOMER_ID"]) == ["customer_id"]

    def test_case_only_duplicates_collapse_to_one_profile(self) -> None:
        # Both columns fold onto one field path, matching the schema where the
        # duplicate field is dropped. Emitting both would put two profiles on it.
        assert _restore(["col", "COL", "OTHER"], ["col", "COL", "OTHER"]) == [
            "col",
            "other",
        ]

    def test_table_level_profile_keeps_absent_field_profiles(self) -> None:
        # Rewriting must leave absent fieldProfiles absent, not turn them into [].
        assert _restore(None, ["CUSTOMER_ID"]) is None

    def test_unknown_field_path_is_reported(self) -> None:
        report = SnowflakeV2Report()
        assert _restore(["ghost"], ["CUSTOMER_ID"], report=report) == ["ghost"]
        assert any(
            "does not match any column" in (warning.title or "")
            for warning in report.warnings
        )
