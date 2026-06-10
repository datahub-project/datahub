import dataclasses
import os
from unittest import mock

from datahub.ingestion.api.report import (
    EntityFilterReport,
    Report,
    SupportsAsObj,
    _cap_report_samples,
)
from datahub.ingestion.api.source import StructuredLogLevel, StructuredLogs
from datahub.ingestion.run.pipeline_config import FlagsConfig
from datahub.utilities.lossy_collections import LossySentinel


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = EntityFilterReport.field(type="view")


def test_entity_filter_report():
    report = MyReport()
    assert report.views.type == "view"
    assert isinstance(report, SupportsAsObj)

    report2 = MyReport()

    report.views.processed(entity="foo")
    report.views.dropped(entity="bar")

    assert (
        report.as_string() == "{'views': {'filtered': ['bar'], 'processed': ['foo']}}"
    )

    # Verify that the reports don't accidentally share any state.
    assert report2.as_string() == "{'views': {'filtered': [], 'processed': []}}"


# ---------------------------------------------------------------------------
# _cap_report_samples tests
# ---------------------------------------------------------------------------


def test_cap_report_samples_basic():
    sentinel = LossySentinel("... sampled of 50 total elements")
    obj = {
        "events_produced": 500,
        "failures": ["err1", "err2", "err3", "err4", "err5", sentinel],
        "warnings": ["w1", "w2", "w3", sentinel],
        "infos": ["i1", "i2"],
        "aspects": {"dataset": {"schemaMetadata": 100}},
    }
    caps = {"failures": 2, "warnings": 1, "infos": 5}
    capped = _cap_report_samples(obj, caps)

    # failures: 2 entries + sentinel preserved
    assert capped["failures"] == ["err1", "err2", sentinel]

    # warnings: 1 entry + sentinel preserved
    assert capped["warnings"] == ["w1", sentinel]

    # infos under cap — untouched
    assert capped["infos"] == ["i1", "i2"]

    # non-list fields untouched
    assert capped["events_produced"] == 500
    assert capped["aspects"] == {"dataset": {"schemaMetadata": 100}}


def test_cap_report_samples_zero_cap():
    """cap=0 should remove all entries but keep the sentinel."""
    sentinel = LossySentinel("... sampled of 30 total elements")
    obj = {"failures": ["e1", "e2", "e3", sentinel], "warnings": [], "infos": ["i1"]}
    caps = {"failures": 0, "warnings": 0, "infos": 0}
    capped = _cap_report_samples(obj, caps)

    # failures: all entries removed, sentinel preserved
    assert capped["failures"] == [sentinel]

    # empty list stays empty
    assert capped["warnings"] == []

    # single entry removed, no sentinel to preserve
    assert capped["infos"] == []


def test_cap_report_samples_no_sentinel():
    """Lists without a LossySentinel are just truncated."""
    obj = {"failures": ["e1", "e2", "e3"]}
    capped = _cap_report_samples(obj, {"failures": 1})
    assert capped["failures"] == ["e1"]


def test_cap_report_samples_key_not_in_caps():
    """Keys absent from the caps dict are never capped."""
    obj = {"failures": ["e1", "e2"], "other_list": ["a", "b", "c"]}
    caps = {"failures": 1}
    capped = _cap_report_samples(obj, caps)

    assert capped["failures"] == ["e1"]
    assert capped["other_list"] == ["a", "b", "c"]


def test_cap_report_samples_nested_dict_not_capped():
    """Dicts nested inside the report are passed through, not recursed into."""
    inner = {"nested_failures": ["x", "y", "z"]}
    obj = {"failures": ["e1"], "sub_report": inner}
    caps = {"failures": 10, "nested_failures": 1}
    capped = _cap_report_samples(obj, caps)
    assert capped["sub_report"] == inner


def test_cap_report_samples_exact_cap():
    """List with length == cap should NOT be capped."""
    obj = {"failures": ["e1", "e2", "e3"]}
    caps = {"failures": 3}
    capped = _cap_report_samples(obj, caps)
    assert capped["failures"] == ["e1", "e2", "e3"]


# ---------------------------------------------------------------------------
# Report.as_string integration with sample_caps
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class SimpleReport(Report):
    failures: list = dataclasses.field(default_factory=list)
    warnings: list = dataclasses.field(default_factory=list)
    count: int = 0


def test_as_string_with_caps_applies_capping():
    report = SimpleReport(
        failures=["e1", "e2", "e3", "e4", "e5"],
        warnings=["w1"],
        count=42,
    )
    result = report.as_string(sample_caps={"failures": 2, "warnings": 5})
    assert "e1" in result
    assert "e2" in result
    assert "e3" not in result  # truncated
    assert "w1" in result  # under cap, kept


def test_as_string_none_caps_shows_all():
    report = SimpleReport(
        failures=["e1", "e2", "e3"],
        count=10,
    )
    result = report.as_string(sample_caps=None)
    assert "e1" in result
    assert "e3" in result


# ---------------------------------------------------------------------------
# FlagsConfig env var integration
# ---------------------------------------------------------------------------


def test_flags_config_defaults():
    """FlagsConfig fields default to 10 when env vars are not set."""

    with mock.patch.dict(os.environ, {}, clear=True):
        flags = FlagsConfig()
    assert flags.progress_report_max_failures == 10
    assert flags.progress_report_max_warnings == 10
    assert flags.progress_report_max_infos == 10
    assert flags.report_failure_sample_size == 10
    assert flags.report_warning_sample_size == 10
    assert flags.report_info_sample_size == 10


def test_flags_config_from_env_vars():
    """FlagsConfig fields pick up env vars."""

    env = {
        "DATAHUB_PROGRESS_REPORT_MAX_FAILURES": "5",
        "DATAHUB_PROGRESS_REPORT_MAX_WARNINGS": "20",
        "DATAHUB_PROGRESS_REPORT_MAX_INFOS": "0",
        "DATAHUB_REPORT_FAILURE_SAMPLE_SIZE": "50",
        "DATAHUB_REPORT_WARNING_SAMPLE_SIZE": "25",
        "DATAHUB_REPORT_INFO_SAMPLE_SIZE": "15",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        flags = FlagsConfig()
    assert flags.progress_report_max_failures == 5
    assert flags.progress_report_max_warnings == 20
    assert flags.progress_report_max_infos == 0
    assert flags.report_failure_sample_size == 50
    assert flags.report_warning_sample_size == 25
    assert flags.report_info_sample_size == 15


def test_flags_config_recipe_overrides_env():
    """Explicit recipe values take precedence over env vars."""

    env = {
        "DATAHUB_PROGRESS_REPORT_MAX_FAILURES": "5",
        "DATAHUB_REPORT_FAILURE_SAMPLE_SIZE": "50",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        flags = FlagsConfig(
            progress_report_max_failures=99,
            report_failure_sample_size=200,
        )
    assert flags.progress_report_max_failures == 99
    assert flags.report_failure_sample_size == 200


# ---------------------------------------------------------------------------
# StructuredLogs.set_sample_sizes
# ---------------------------------------------------------------------------


def test_structured_logs_set_sample_sizes():
    """set_sample_sizes adjusts the underlying LossyDict max_elements."""

    logs = StructuredLogs()
    # defaults
    assert logs._entries[StructuredLogLevel.ERROR].max_elements == 10
    assert logs._entries[StructuredLogLevel.WARN].max_elements == 10

    logs.set_sample_sizes(failure_size=50, warning_size=25)
    assert logs._entries[StructuredLogLevel.ERROR].max_elements == 50
    assert logs._entries[StructuredLogLevel.WARN].max_elements == 25


def test_structured_logs_set_sample_sizes_partial():
    """set_sample_sizes with None leaves the original value."""

    logs = StructuredLogs()
    logs.set_sample_sizes(failure_size=99)
    assert logs._entries[StructuredLogLevel.ERROR].max_elements == 99
    assert logs._entries[StructuredLogLevel.WARN].max_elements == 10  # unchanged


def test_set_sample_sizes_after_entries_added_prunes():
    """set_sample_sizes resizes even when entries already exist."""
    logs = StructuredLogs()
    for i in range(5):
        logs.report_log(StructuredLogLevel.ERROR, f"err_{i}", context=f"ctx_{i}")
    assert len(logs._entries[StructuredLogLevel.ERROR]) == 5

    # Grow: all entries kept
    logs.set_sample_sizes(failure_size=50)
    assert logs._entries[StructuredLogLevel.ERROR].max_elements == 50
    assert len(logs._entries[StructuredLogLevel.ERROR]) == 5

    # Shrink: excess entries pruned
    logs.set_sample_sizes(failure_size=2)
    assert logs._entries[StructuredLogLevel.ERROR].max_elements == 2
    assert len(logs._entries[StructuredLogLevel.ERROR]) == 2


def test_cap_ignores_lossy_list_sentinel():
    """LossySentinel shouldn't count as a real entry."""
    sentinel = LossySentinel("... sampled of 30 total elements")
    obj = {
        "failures": [
            {"message": "connection timeout"},
            {"message": "permission denied"},
            {"message": "schema mismatch"},
            sentinel,
        ],
    }
    capped = _cap_report_samples(obj, {"failures": 3})
    assert capped["failures"] == obj["failures"]


def test_cap_with_sentinel_and_truncation():
    """When truncating, the LossySentinel is preserved."""
    sentinel = LossySentinel("... sampled of 50 total elements")
    obj = {
        "failures": [
            {"message": "connection timeout"},
            {"message": "permission denied"},
            {"message": "schema mismatch"},
            {"message": "null column"},
            sentinel,
        ],
    }
    capped = _cap_report_samples(obj, {"failures": 2})
    assert len(capped["failures"]) == 3  # 2 entries + sentinel
    assert capped["failures"][0]["message"] == "connection timeout"
    assert capped["failures"][1]["message"] == "permission denied"
    assert capped["failures"][2] is sentinel


def test_negative_sample_size_rejected():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        FlagsConfig(progress_report_max_failures=-1)
    with pytest.raises(ValidationError):
        FlagsConfig(report_failure_sample_size=-5)


# ---------------------------------------------------------------------------
# Pipeline retention sizing: max(report_size, progress_max)
# ---------------------------------------------------------------------------


def _run_pipeline_get_reports(flags: dict, num_each: int = 30) -> dict:
    """Helper: run a fast pipeline and return source report as_obj()."""
    import contextlib
    import io
    from typing import Iterable

    from datahub.configuration.common import ConfigModel
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.api.source import Source, SourceReport
    from datahub.ingestion.api.workunit import MetadataWorkUnit
    from datahub.ingestion.run.pipeline import Pipeline
    from datahub.metadata.schema_classes import DatasetPropertiesClass

    class _Cfg(ConfigModel):
        n: int = 30

    class _Src(Source):
        def __init__(self, config: _Cfg, ctx: PipelineContext):
            super().__init__(ctx)
            self.config = config
            self.report = SourceReport()

        @classmethod
        def create(cls, config_dict, ctx):
            return cls(_Cfg.model_validate(config_dict), ctx)

        def get_workunits(self) -> Iterable[MetadataWorkUnit]:
            for i in range(self.config.n):
                self.report.failure(message=f"F{i}", context=f"ctx-f-{i}")
                self.report.warning(message=f"W{i}", context=f"ctx-w-{i}")
                self.report.info(message=f"I{i}", context=f"ctx-i-{i}")
            mcp = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,t,PROD)",
                aspect=DatasetPropertiesClass(name="t"),
            )
            yield mcp.as_workunit()

        def get_report(self):
            return self.report

        def close(self):
            pass

    # Register the source temporarily
    from datahub.ingestion.source.source_registry import source_registry

    source_registry.register("_test_combo_src", _Src)
    try:
        recipe = {
            "source": {"type": "_test_combo_src", "config": {"n": num_each}},
            "sink": {"type": "console"},
            "flags": flags,
        }
        pipeline = Pipeline.create(recipe)
        with contextlib.redirect_stdout(io.StringIO()):
            pipeline.run()

        # Get the underlying LossyDict max_elements for verification
        logs = pipeline.source.get_report()._structured_logs
        retention = {
            "failure_max_elements": logs._entries[
                StructuredLogLevel.ERROR
            ].max_elements,
            "warning_max_elements": logs._entries[StructuredLogLevel.WARN].max_elements,
            "info_max_elements": logs._entries[StructuredLogLevel.INFO].max_elements,
        }
        sink_report = pipeline.sink.get_report()
        sink_retention = {
            "failure_max_elements": sink_report.failures.max_elements,
            "warning_max_elements": sink_report.warnings.max_elements,
        }
        return {
            "source": pipeline.source.get_report().as_obj(),
            "source_report": pipeline.source.get_report(),
            "sink": sink_report.as_obj(),
            "retention": retention,
            "sink_retention": sink_retention,
        }
    finally:
        source_registry._mapping.pop("_test_combo_src", None)


def _count_real_entries(report_obj: dict, key: str) -> int:
    """Count non-sentinel entries in a report list."""
    items = report_obj.get(key, [])
    return sum(1 for item in items if not isinstance(item, LossySentinel))


def test_retention_both_defaults():
    """Both at default 10: retain 10, final shows 10."""
    result = _run_pipeline_get_reports(flags={})
    assert result["retention"]["failure_max_elements"] == 10
    assert _count_real_entries(result["source"], "failures") == 10


def test_retention_report_size_higher():
    """report_size=20 > progress_max=10 (default): retain 20, final shows 20."""
    result = _run_pipeline_get_reports(flags={"report_failure_sample_size": 20})
    assert result["retention"]["failure_max_elements"] == 20
    assert _count_real_entries(result["source"], "failures") == 20


def test_retention_progress_max_higher():
    """progress_max=25 > report_size=10 (default): retain 25 so interim can show 25."""
    result = _run_pipeline_get_reports(flags={"progress_report_max_failures": 25})
    assert result["retention"]["failure_max_elements"] == 25


def test_retention_progress_max_higher_final_shows_report_size():
    """progress_max=25, report_size=5: retain 25, final displays only 5."""
    result = _run_pipeline_get_reports(
        flags={
            "progress_report_max_failures": 25,
            "report_failure_sample_size": 5,
        }
    )
    # Retained max(25, 5) = 25 so interim can show up to 25
    assert result["retention"]["failure_max_elements"] == 25
    # as_obj() returns all 25 retained (raw data)
    raw_failures = _count_real_entries(result["source"], "failures")
    assert raw_failures == 25
    # But as_string with the final caps shows only 5
    final_str = result["source_report"].as_string(
        sample_caps={"failures": 5, "warnings": 10, "infos": 10},
    )
    assert final_str.count("'message': 'F") == 5


def test_retention_independent_per_severity():
    """Each severity can have different retention sizes."""
    result = _run_pipeline_get_reports(
        flags={
            "report_failure_sample_size": 5,
            "progress_report_max_failures": 20,
            "report_warning_sample_size": 25,
            "progress_report_max_warnings": 3,
            "report_info_sample_size": 8,
            "progress_report_max_infos": 8,
        }
    )
    # failures: max(5, 20) = 20
    assert result["retention"]["failure_max_elements"] == 20
    # warnings: max(25, 3) = 25
    assert result["retention"]["warning_max_elements"] == 25
    # infos: max(8, 8) = 8
    assert result["retention"]["info_max_elements"] == 8


def test_sink_report_present_and_correct():
    """Sink report has the expected fields and records written count."""
    result = _run_pipeline_get_reports(flags={})
    sink = result["sink"]
    assert sink["total_records_written"] == 1
    assert "failures" in sink
    assert "warnings" in sink
    assert len(sink["failures"]) == 0
    assert len(sink["warnings"]) == 0


def test_sink_retention_follows_flags():
    """Sink LossyList sizing should respect recipe flags."""
    result = _run_pipeline_get_reports(
        flags={
            "report_failure_sample_size": 25,
            "report_warning_sample_size": 30,
            "progress_report_max_failures": 5,
            "progress_report_max_warnings": 50,
        }
    )
    # failures: max(25, 5) = 25
    assert result["sink_retention"]["failure_max_elements"] == 25
    # warnings: max(30, 50) = 50
    assert result["sink_retention"]["warning_max_elements"] == 50
