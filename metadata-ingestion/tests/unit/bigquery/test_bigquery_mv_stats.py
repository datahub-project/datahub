import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Iterable, List, Optional
from unittest.mock import patch

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryView
from datahub.metadata.com.linkedin.pegasus2avro.dataset import ViewProperties
from datahub.metadata.schema_classes import DatasetProfileClass
from datahub.utilities.ratelimiter import RateLimiter

PROJECT_ID = "test-project"
DATASET_NAME = "test-dataset"


def _make_mv(
    name: str = "mv1",
    rows_count: Optional[int] = None,
    size_in_bytes: Optional[int] = None,
) -> BigqueryView:
    now = datetime.now(tz=timezone.utc)
    return BigqueryView(
        name=name,
        created=now - timedelta(days=10),
        last_altered=None,
        comment="comment",
        view_definition="CREATE MATERIALIZED VIEW",
        materialized=True,
        size_in_bytes=size_in_bytes,
        rows_count=rows_count,
        labels=None,
    )


def _make_plain_view(name: str = "v1") -> BigqueryView:
    now = datetime.now(tz=timezone.utc)
    return BigqueryView(
        name=name,
        created=now - timedelta(days=10),
        last_altered=None,
        comment="comment",
        view_definition="CREATE VIEW",
        materialized=False,
        size_in_bytes=None,
        rows_count=None,
        labels=None,
    )


def _make_bq_table(num_rows=42, num_bytes=4096, modified=None):
    return SimpleNamespace(
        num_rows=num_rows,
        num_bytes=num_bytes,
        modified=modified or datetime.now(tz=timezone.utc),
    )


@pytest.fixture
def schema_gen():
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client"),
        patch.object(BigQueryV2Config, "get_projects_client"),
    ):
        config = BigQueryV2Config.model_validate({"project_id": PROJECT_ID})
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
        # gen_dataset_workunits needs identifiers/containers we don't want to mock here;
        # isolate view-level workunits (ViewProperties + DatasetProfile).
        source.bq_schema_extractor.gen_dataset_workunits = (  # type: ignore[method-assign]
            lambda *args, **kwargs: []
        )
        return source.bq_schema_extractor


def _aspects(gen: Iterable[MetadataWorkUnit]) -> List:
    aspects = []
    for wu in gen:
        metadata = wu.metadata
        if isinstance(metadata, MetadataChangeProposalWrapper):
            aspects.append(metadata.aspect)
    return aspects


def _profiles(aspects: List) -> List[DatasetProfileClass]:
    return [a for a in aspects if isinstance(a, DatasetProfileClass)]


def _enrich_and_emit(schema_gen, view, table_returned):
    with patch.object(
        schema_gen.schema_api, "get_table_metadata", return_value=table_returned
    ) as gt_mock:
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
        aspects = _aspects(
            schema_gen.gen_view_dataset_workunits(view, [], PROJECT_ID, DATASET_NAME)
        )
    return aspects, gt_mock


def test_mv_profile_emitted_with_profiling_disabled(schema_gen):
    # The reported bug: profiling.enabled defaults to False, yet MV stats must still show.
    assert schema_gen.config.is_profiling_enabled() is False
    view = _make_mv()
    with patch.object(
        schema_gen.schema_api, "get_table_metadata", return_value=_make_bq_table()
    ):
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
        aspects = _aspects(
            schema_gen.gen_view_dataset_workunits(view, [], PROJECT_ID, DATASET_NAME)
        )

    profiles = _profiles(aspects)
    assert len(profiles) == 1
    assert profiles[0].rowCount == 42
    assert profiles[0].sizeInBytes == 4096
    assert schema_gen.report.num_mv_stats_emitted == 1


def test_mv_profile_has_full_table_snapshot_partition_spec(schema_gen):
    view = _make_mv()
    aspects, _ = _enrich_and_emit(schema_gen, view, _make_bq_table())
    profiles = _profiles(aspects)
    assert len(profiles) == 1
    assert profiles[0].partitionSpec is not None
    assert profiles[0].partitionSpec.partition == "FULL_TABLE_SNAPSHOT"
    assert profiles[0].partitionSpec.type == "FULL_TABLE"


def test_mv_zero_rows_still_emits_profile(schema_gen):
    # `is not None`, not truthiness: a zero-row MV reports rowCount: 0.
    view = _make_mv()
    aspects, _ = _enrich_and_emit(schema_gen, view, _make_bq_table(num_rows=0))
    profiles = _profiles(aspects)
    assert len(profiles) == 1
    assert profiles[0].rowCount == 0


def test_plain_view_no_fetch_no_profile(schema_gen):
    view = _make_plain_view()
    with patch.object(schema_gen.schema_api, "get_table_metadata") as gt_mock:
        # Plain views are never enriched (the call site gates on materialized);
        # confirm gen_view_dataset_workunits emits no profile for them either.
        aspects = _aspects(
            schema_gen.gen_view_dataset_workunits(view, [], PROJECT_ID, DATASET_NAME)
        )
    gt_mock.assert_not_called()
    assert _profiles(aspects) == []
    assert schema_gen.report.num_mv_stats_emitted == 0


def test_legacy_stats_skips_fetch_but_emits_profile(schema_gen):
    # use_legacy_table_stats path already populated rows_count; no tables.get needed.
    view = _make_mv(rows_count=7, size_in_bytes=128)
    with patch.object(schema_gen.schema_api, "get_table_metadata") as gt_mock:
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
        aspects = _aspects(
            schema_gen.gen_view_dataset_workunits(view, [], PROJECT_ID, DATASET_NAME)
        )
    gt_mock.assert_not_called()
    assert schema_gen.report.num_mv_stats_skipped_legacy == 1
    assert schema_gen.report.num_mv_stats_fetched == 0
    profiles = _profiles(aspects)
    assert len(profiles) == 1
    assert profiles[0].rowCount == 7


def test_flag_disabled_no_fetch_no_profile(schema_gen):
    schema_gen.config.include_materialized_view_stats = False
    view = _make_mv()
    with patch.object(schema_gen.schema_api, "get_table_metadata") as gt_mock:
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
        aspects = _aspects(
            schema_gen.gen_view_dataset_workunits(view, [], PROJECT_ID, DATASET_NAME)
        )
    gt_mock.assert_not_called()
    assert _profiles(aspects) == []
    assert schema_gen.report.num_mv_stats_emitted == 0


def test_tables_get_failure_warns_and_view_still_emitted(schema_gen):
    view = _make_mv()
    # Let the real get_table_metadata run so its try/except handles the error,
    # records the warning, increments num_mv_stats_failed, and returns None.
    with patch.object(
        schema_gen.schema_api.bq_client, "get_table", side_effect=Exception("boom")
    ):
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
        aspects = _aspects(
            schema_gen.gen_view_dataset_workunits(view, [], PROJECT_ID, DATASET_NAME)
        )
    assert schema_gen.report.num_mv_stats_failed == 1
    assert schema_gen.report.num_mv_stats_fetched == 0
    # No profile, but the view itself (ViewProperties) is still emitted.
    assert _profiles(aspects) == []
    assert any(isinstance(a, ViewProperties) for a in aspects)


def test_mv_last_modified_populated_from_tables_get(schema_gen):
    modified = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
    view = _make_mv()
    _enrich_and_emit(schema_gen, view, _make_bq_table(modified=modified))
    assert view.last_altered == modified


def test_cap_skips_fetch_after_limit_and_warns_once(schema_gen):
    from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
        _MAX_MV_STATS_PER_DATASET,
    )

    schema_gen._mv_stats_fetch_count[f"{PROJECT_ID}.{DATASET_NAME}"] = (
        _MAX_MV_STATS_PER_DATASET
    )
    view = _make_mv()
    with patch.object(schema_gen.schema_api, "get_table_metadata") as gt_mock:
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
    gt_mock.assert_not_called()
    assert schema_gen.report.num_mv_stats_skipped_cap == 1
    # The warning is recorded exactly once per dataset.
    assert len(schema_gen.report.warnings) == 1


def test_profile_pattern_excluded_mv_skips_the_fetch(schema_gen):
    # profile_pattern gates the emit side; it must gate the fetch too, or an
    # excluded view still costs a tables.get whose result is thrown away.
    schema_gen.config.profile_pattern = AllowDenyPattern(deny=[".*"])
    view = _make_mv()
    with patch.object(schema_gen.schema_api, "get_table_metadata") as gt_mock:
        schema_gen._enrich_materialized_view_stats(view, PROJECT_ID, DATASET_NAME)
    gt_mock.assert_not_called()
    assert schema_gen.report.num_mv_stats_fetched == 0
    assert view.rows_count is None


def test_tables_get_failure_still_records_api_timing(schema_gen):
    # Perf accounting must survive the failure path: a permissions error should
    # not report zero API requests while burning the full timeout per view.
    api = schema_gen.schema_api
    with patch.object(api, "bq_client") as client:
        client.get_table.side_effect = Exception("permission denied")
        assert (
            api.get_table_metadata(PROJECT_ID, DATASET_NAME, "mv1", schema_gen.report)
            is None
        )
    assert schema_gen.report.num_mv_stats_failed == 1
    assert api.report.num_get_table_metadata_api_requests == 1


def test_process_views_enriches_only_materialized_views(schema_gen):
    # The other tests call _enrich_materialized_view_stats directly, which
    # leaves the routing that decides *which* views reach it untested.
    mv = _make_mv(name="mv1")
    plain = _make_plain_view(name="v1")
    with patch.object(schema_gen, "_enrich_materialized_view_stats") as enrich:
        list(
            schema_gen._process_views_for_dataset(
                project_id=PROJECT_ID,
                dataset_name=DATASET_NAME,
                views=[mv, plain],
                columns=None,
            )
        )
    assert [c.kwargs["view"].name for c in enrich.call_args_list] == ["mv1"]


def test_process_views_skips_enrichment_when_flag_disabled(schema_gen):
    schema_gen.config.include_materialized_view_stats = False
    with patch.object(schema_gen, "_enrich_materialized_view_stats") as enrich:
        list(
            schema_gen._process_views_for_dataset(
                project_id=PROJECT_ID,
                dataset_name=DATASET_NAME,
                views=[_make_mv()],
                columns=None,
            )
        )
    enrich.assert_not_called()


def test_no_rate_limiter_unless_configured(schema_gen):
    # A hardcoded limiter here throttled the default path to a fraction of its
    # own target, because RateLimiter sleeps holding a lock shared by every
    # dataset worker thread. Throttling is opt-in, like every other path here.
    assert schema_gen._mv_stats_rate_limiter is None


def test_rate_limiter_honours_requests_per_min():
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client"),
        patch.object(BigQueryV2Config, "get_projects_client"),
    ):
        config = BigQueryV2Config.model_validate(
            {"project_id": PROJECT_ID, "rate_limit": True, "requests_per_min": 60}
        )
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    limiter = source.bq_schema_extractor._mv_stats_rate_limiter
    assert limiter is not None
    assert (limiter.max_calls, limiter.period) == (60, 60)


def test_throttle_wait_is_not_reported_as_api_time(schema_gen):
    # Timing the throttle wait as BigQuery latency reported hundreds of seconds
    # of "API time" for instantaneous calls, pointing a reader at BigQuery when
    # the cost was entirely local.
    api = schema_gen.schema_api
    limiter = RateLimiter(max_calls=1, period=0.2)
    with patch.object(api, "bq_client") as client:
        client.get_table.return_value = SimpleNamespace(
            num_rows=1, num_bytes=1, modified=None
        )
        start = time.monotonic()
        for i in range(3):
            api.get_table_metadata(
                PROJECT_ID,
                DATASET_NAME,
                f"mv{i}",
                schema_gen.report,
                rate_limiter=limiter,
            )
        wall = time.monotonic() - start
    assert wall > 0.2, "limiter should actually have throttled"
    assert api.report.get_table_metadata_sec < wall / 2
