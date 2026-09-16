import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from unittest import mock

import pytest
import time_machine

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.metadata.urns import SchemaFieldUrn
from tests.integration.snowflake.common import FROZEN_TIME, default_query_results

pytestmark = pytest.mark.integration_batch_1

# A column's field path is written by one aspect and referenced by several others
# (lineage, profiles, usage). Nothing in the codebase forces them to agree, and a
# disagreement is silent: lineage still emits, it just anchors on a schemaField
# URN no schema declares. `preserve_column_case` changes the field path, so it
# changes every one of those producers at once — this pins that they stay in step.


def _run(tmp_path: Path, filename: str, **config_overrides: Any) -> List[dict]:
    output_file = tmp_path / filename

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",  # type: ignore[arg-type]
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=True,
                        include_usage_stats=True,
                        format_sql_queries=True,
                        validate_upstreams_against_patterns=False,
                        include_operational_stats=True,
                        incremental_lineage=False,
                        use_queries_v2=False,
                        start_time=datetime(2022, 6, 6).replace(tzinfo=timezone.utc),
                        end_time=datetime(2022, 6, 7, 7, 17).replace(
                            tzinfo=timezone.utc
                        ),
                        profiling=GEProfilingConfig(
                            enabled=True,
                            profile_if_updated_since_days=None,
                            profile_table_row_limit=None,
                            profile_table_size_limit=None,
                            # Column-level profiling needs queries this fixture does
                            # not mock; profile field paths are covered by
                            # tests/unit/snowflake/test_snowflake_profile_alignment.py.
                            profile_table_level_only=True,
                        ),
                        extract_tags=TagOption.without_lineage,
                        **config_overrides,
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.raise_from_status()

    return json.loads(output_file.read_text())


def _schemas_by_dataset(records: List[dict]) -> Dict[str, List[str]]:
    """Field paths each dataset declares, in emission order."""
    schemas: Dict[str, List[str]] = {}
    for record in records:
        aspect = record.get("aspect") or {}
        if record.get("aspectName") != "schemaMetadata":
            continue
        schemas[record["entityUrn"]] = [
            field["fieldPath"] for field in aspect["json"]["fields"]
        ]
    return schemas


def _referenced_fields(records: List[dict]) -> Set[Tuple[str, str]]:
    """Every (dataset urn, field path) that some aspect other than the schema cites."""
    referenced: Set[Tuple[str, str]] = set()
    for record in records:
        name = record.get("aspectName")
        body = (record.get("aspect") or {}).get("json") or {}

        if name == "upstreamLineage":
            for edge in body.get("fineGrainedLineages") or []:
                for side in ("upstreams", "downstreams"):
                    for field_urn in edge.get(side) or []:
                        parsed = SchemaFieldUrn.from_string(field_urn)
                        referenced.add((str(parsed.parent), parsed.field_path))

        elif name == "datasetProfile":
            for field_profile in body.get("fieldProfiles") or []:
                referenced.add((record["entityUrn"], field_profile["fieldPath"]))

        elif name == "datasetUsageStatistics":
            for field_count in body.get("fieldCounts") or []:
                referenced.add((record["entityUrn"], field_count["fieldPath"]))

    return referenced


@pytest.mark.parametrize("preserve", [False, True], ids=["default", "preserve"])
@time_machine.travel(FROZEN_TIME, tick=False)
def test_every_referenced_field_is_declared_by_a_schema(
    tmp_path: Path, mock_time: Any, mock_datahub_graph: Any, preserve: bool
) -> None:
    records = _run(
        tmp_path,
        f"events_preserve_{preserve}.json",
        preserve_column_case=preserve,
    )
    schemas = _schemas_by_dataset(records)
    assert schemas, "fixture emitted no schemas; the test would pass vacuously"

    checked = [
        (dataset, field)
        for dataset, field in sorted(_referenced_fields(records))
        # Only datasets we ingested a schema for can be checked; lineage legitimately
        # points at upstreams outside the ingestion scope.
        if dataset in schemas
        # METADATA$ACTION / $ISUPDATE / $ROW_ID are stream pseudo-columns. Snowflake
        # exposes them on a stream's rows, never on the base table, so the base
        # table's schema will not declare them. These references dangle in every
        # configuration, flag or no flag — a pre-existing quirk, not a casing one.
        and not field.upper().startswith("METADATA$")
    ]
    # Guard the guard: if the fixture stops producing column-level references this
    # test would pass while checking nothing.
    assert len(checked) >= 100, f"only {len(checked)} references checked"

    dangling = [
        (dataset, field) for dataset, field in checked if field not in schemas[dataset]
    ]
    assert not dangling, (
        f"{len(dangling)} field references have no matching schema field path. "
        f"First few: {dangling[:5]}"
    )

    # Same records, second property: a duplicate field path means two columns
    # collapsed onto one field, and the loser is dropped downstream.
    collisions = {
        dataset: paths
        for dataset, paths in schemas.items()
        if len(paths) != len(set(paths))
    }
    assert not collisions
