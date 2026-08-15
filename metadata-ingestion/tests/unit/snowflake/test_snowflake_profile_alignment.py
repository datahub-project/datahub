from typing import Any, List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
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
    **config_overrides: Any,
) -> Optional[List[str]]:
    profiler = SnowflakeProfiler(
        config=_make_config(**config_overrides), report=SnowflakeV2Report()
    )
    restored = list(
        profiler._to_schema_field_paths([_profile_workunit(profiler_field_paths)])
    )
    profile = restored[0].get_aspect_of_type(DatasetProfileClass)
    assert profile is not None
    if profile.fieldProfiles is None:
        return None
    return [f.fieldPath for f in profile.fieldProfiles]


class TestProfileFieldPathAlignment:
    """The profiler names columns as Snowflake stores them; these pin the
    translation onto the field paths schemaMetadata was built with."""

    @pytest.mark.parametrize(
        ("column", "lowercase", "expected"),
        [
            # The default: the schema lowercases, so the profile must too, or it
            # never attaches to its field.
            ("MixedCol", True, ["mixedcol"]),
            ("CUSTOMER_ID", True, ["customer_id"]),
            # With lowercasing off the schema keeps the stored spelling, and a
            # profile that lowercased anyway would not attach to it.
            ("MixedCol", False, ["MixedCol"]),
            ("CUSTOMER_ID", False, ["CUSTOMER_ID"]),
        ],
    )
    def test_field_path_follows_the_schema_rule(
        self, column: str, lowercase: bool, expected: List[str]
    ) -> None:
        assert _restore([column], convert_urns_to_lowercase=lowercase) == expected

    @pytest.mark.parametrize(
        ("lowercase", "expected"),
        [
            # Both fold onto one path, matching the schema where the duplicate
            # field is dropped. Emitting both would put two profiles on it.
            (True, ["col", "other"]),
            # Without lowercasing the two spellings are already distinct paths,
            # so both profiles attach and nothing is dropped.
            (False, ["col", "COL", "OTHER"]),
        ],
    )
    def test_case_only_pair_follows_the_schema_too(
        self, lowercase: bool, expected: List[str]
    ) -> None:
        assert (
            _restore(["col", "COL", "OTHER"], convert_urns_to_lowercase=lowercase)
            == expected
        )

    def test_table_level_profile_keeps_absent_field_profiles(self) -> None:
        # Rewriting must leave absent fieldProfiles absent, not turn them into [].
        assert _restore(None) is None
