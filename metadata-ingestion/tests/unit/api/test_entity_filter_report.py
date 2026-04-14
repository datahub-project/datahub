import dataclasses
import os
from unittest import mock

from datahub.ingestion.api.report import EntityFilterReport, Report, SupportsAsObj


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
    from datahub.ingestion.api.report import _cap_report_samples

    obj = {
        "events_produced": 500,
        "failures": ["err1", "err2", "err3", "err4", "err5"],
        "warnings": ["w1", "w2", "w3"],
        "infos": ["i1", "i2"],
        "aspects": {"dataset": {"schemaMetadata": 100}},
    }
    caps = {"failures": 2, "warnings": 1, "infos": 5}
    capped = _cap_report_samples(obj, caps)

    # failures capped to 2 + summary message
    assert len(capped["failures"]) == 3
    assert capped["failures"][:2] == ["err1", "err2"]
    assert "interim report" in capped["failures"][-1]
    assert "2 of 5" in capped["failures"][-1]

    # warnings capped to 1 + summary message
    assert len(capped["warnings"]) == 2
    assert "interim report" in capped["warnings"][-1]

    # infos under cap — untouched
    assert capped["infos"] == ["i1", "i2"]

    # non-list fields untouched
    assert capped["events_produced"] == 500
    assert capped["aspects"] == {"dataset": {"schemaMetadata": 100}}


def test_cap_report_samples_zero_cap():
    """cap=0 should suppress all entries but still show the count."""
    from datahub.ingestion.api.report import _cap_report_samples

    obj = {"failures": ["e1", "e2", "e3"], "warnings": [], "infos": ["i1"]}
    caps = {"failures": 0, "warnings": 0, "infos": 0}
    capped = _cap_report_samples(obj, caps)

    # failures: all suppressed, one sentinel line
    assert len(capped["failures"]) == 1
    assert "0 of 3" in capped["failures"][0]
    assert "interim report" in capped["failures"][0]

    # empty list stays empty (len(0) > 0 is False)
    assert capped["warnings"] == []

    # single-element list: still capped since 1 > 0
    assert len(capped["infos"]) == 1
    assert "0 of 1" in capped["infos"][0]


def test_cap_report_samples_key_not_in_caps():
    """Keys absent from the caps dict are never capped."""
    from datahub.ingestion.api.report import _cap_report_samples

    obj = {"failures": ["e1", "e2"], "other_list": ["a", "b", "c"]}
    caps = {"failures": 1}
    capped = _cap_report_samples(obj, caps)

    assert len(capped["failures"]) == 2  # 1 + sentinel
    assert capped["other_list"] == ["a", "b", "c"]  # untouched


def test_cap_report_samples_empty_caps_dict():
    """Empty caps dict means nothing is capped."""
    from datahub.ingestion.api.report import _cap_report_samples

    obj = {"failures": ["e1", "e2", "e3"]}
    capped = _cap_report_samples(obj, {})
    assert capped["failures"] == ["e1", "e2", "e3"]


def test_cap_report_samples_nested_dict_not_capped():
    """Dicts nested inside the report are passed through, not recursed into."""
    from datahub.ingestion.api.report import _cap_report_samples

    inner = {"nested_failures": ["x", "y", "z"]}
    obj = {"failures": ["e1"], "sub_report": inner}
    caps = {"failures": 10, "nested_failures": 1}
    capped = _cap_report_samples(obj, caps)

    # The nested dict is untouched — capping is shallow
    assert capped["sub_report"] == inner


def test_cap_report_samples_exact_cap():
    """List with length == cap should NOT be capped (only > triggers it)."""
    from datahub.ingestion.api.report import _cap_report_samples

    obj = {"failures": ["e1", "e2", "e3"]}
    caps = {"failures": 3}
    capped = _cap_report_samples(obj, caps)
    assert capped["failures"] == ["e1", "e2", "e3"]


# ---------------------------------------------------------------------------
# Report.as_string integration with progress_sample_caps
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
    result = report.as_string(progress_sample_caps={"failures": 2, "warnings": 5})
    assert "interim report" in result
    assert "2 of 5" in result
    # warnings list (len 1) is under cap 5, so no sentinel
    assert result.count("interim report") == 1


def test_as_string_none_caps_shows_all():
    report = SimpleReport(
        failures=["e1", "e2", "e3"],
        count=10,
    )
    result = report.as_string(progress_sample_caps=None)
    assert "interim report" not in result
    assert "e1" in result
    assert "e3" in result


# ---------------------------------------------------------------------------
# FlagsConfig env var integration
# ---------------------------------------------------------------------------


def test_flags_config_defaults():
    """FlagsConfig fields default to 10 when env vars are not set."""
    from datahub.ingestion.run.pipeline_config import FlagsConfig

    with mock.patch.dict(os.environ, {}, clear=True):
        flags = FlagsConfig()
    assert flags.progress_report_max_failures == 10
    assert flags.progress_report_max_warnings == 10
    assert flags.progress_report_max_infos == 10


def test_flags_config_from_env_vars():
    """FlagsConfig fields pick up env vars."""
    from datahub.ingestion.run.pipeline_config import FlagsConfig

    env = {
        "DATAHUB_PROGRESS_REPORT_MAX_FAILURES": "5",
        "DATAHUB_PROGRESS_REPORT_MAX_WARNINGS": "20",
        "DATAHUB_PROGRESS_REPORT_MAX_INFOS": "0",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        flags = FlagsConfig()
    assert flags.progress_report_max_failures == 5
    assert flags.progress_report_max_warnings == 20
    assert flags.progress_report_max_infos == 0


def test_flags_config_recipe_overrides_env():
    """Explicit recipe values take precedence over env vars."""
    from datahub.ingestion.run.pipeline_config import FlagsConfig

    env = {"DATAHUB_PROGRESS_REPORT_MAX_FAILURES": "5"}
    with mock.patch.dict(os.environ, env, clear=True):
        flags = FlagsConfig(progress_report_max_failures=99)
    assert flags.progress_report_max_failures == 99
