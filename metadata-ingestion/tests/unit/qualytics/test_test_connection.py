"""Tests for QualyticsSource.test_connection.

This is a user-facing feature -- the Test Connection button in the DataHub UI and
`datahub ingest --test-source-connection` -- so what matters is that each failure mode
produces an actionable, correctly-scoped report rather than one opaque error.
"""

from typing import Any

from requests_mock import Mocker

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.qualytics.source import QualyticsSource

BASE = "https://acme.qualytics.io/api"


def _recipe(**overrides: Any) -> dict[str, Any]:
    return {"base_url": BASE, "token": "t", **overrides}


def _empty_page() -> dict[str, Any]:
    return {"items": [], "page": 1, "pages": 1, "size": 1, "total": 0}


def _spec() -> dict[str, Any]:
    return {"paths": {"/api/datastores": {}, "/api/containers": {}}}


def test_a_healthy_deployment_reports_connectivity_and_every_capability() -> None:
    with Mocker() as m:
        m.get(f"{BASE}/openapi.json", json=_spec())
        for path in ("datastores", "containers", "quality-checks", "field-profiles"):
            m.get(f"{BASE}/{path}", json=_empty_page())

        report = QualyticsSource.test_connection(_recipe())

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    # The keys, not only their values: all() over an empty report is vacuously true.
    assert set(report.capability_report) == {
        SourceCapability.DESCRIPTIONS,
        SourceCapability.DATA_PROFILING,
    }
    assert all(c.capable for c in report.capability_report.values())


def test_a_bad_token_fails_connectivity_and_skips_capability_probes() -> None:
    # Reporting five identical permission failures when the token itself is invalid
    # buries the actual cause.
    with Mocker() as m:
        m.get(f"{BASE}/datastores", status_code=401, json={"detail": "nope"})

        report = QualyticsSource.test_connection(_recipe())

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert "token" in (report.basic_connectivity.failure_reason or "")
    assert not report.capability_report


def test_base_url_missing_the_api_root_path_says_what_to_use_instead() -> None:
    # The likeliest recipe mistake: the deployment origin without its /api suffix.
    # Left undiagnosed it looks like a successful run that ingests nothing.
    origin = "https://acme.qualytics.io"
    with Mocker() as m:
        m.get(f"{origin}/datastores", status_code=404, text="Not Found")
        m.get(f"{origin}/openapi.json", json=_spec())

        report = QualyticsSource.test_connection(_recipe(base_url=origin))

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    reason = report.basic_connectivity.failure_reason or ""
    assert f"{origin}/api" in reason


def test_a_reachable_deployment_with_the_wrong_root_path_still_fails_clearly() -> None:
    # /datastores answers, but the spec says the API lives elsewhere -- e.g. base_url
    # points at a proxy that swallows unknown paths with a 200.
    with Mocker() as m:
        m.get(f"{BASE}/datastores", json=_empty_page())
        m.get(f"{BASE}/containers", json=_empty_page())
        m.get(f"{BASE}/openapi.json", json={"paths": {"/gateway/datastores": {}}})

        report = QualyticsSource.test_connection(_recipe())

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert "/gateway" in (report.basic_connectivity.failure_reason or "")


def test_one_unreadable_endpoint_fails_only_its_own_capability() -> None:
    # A token that can read containers but not quality checks should produce a report
    # that says exactly that, not a blanket failure.
    with Mocker() as m:
        m.get(f"{BASE}/openapi.json", json=_spec())
        m.get(f"{BASE}/datastores", json=_empty_page())
        m.get(f"{BASE}/containers", json=_empty_page())
        m.get(f"{BASE}/field-profiles", json=_empty_page())
        m.get(f"{BASE}/quality-checks", status_code=404, text="Not Found")

        report = QualyticsSource.test_connection(_recipe())

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    descriptions = report.capability_report[SourceCapability.DESCRIPTIONS]
    assert not descriptions.capable
    assert "quality checks" in (descriptions.failure_reason or "")


def test_disabled_features_are_not_probed() -> None:
    # Probing profiles when emit_profiles is off would report a failure the user has
    # already opted out of.
    with Mocker() as m:
        m.get(f"{BASE}/openapi.json", json=_spec())
        m.get(f"{BASE}/datastores", json=_empty_page())
        m.get(f"{BASE}/containers", json=_empty_page())
        m.get(f"{BASE}/quality-checks", json=_empty_page())

        report = QualyticsSource.test_connection(_recipe(emit_profiles=False))

    assert report.capability_report is not None
    assert SourceCapability.DATA_PROFILING not in report.capability_report


def test_an_invalid_recipe_is_reported_rather_than_raised() -> None:
    # test_connection is called with whatever the user typed; a bad base_url must come
    # back as a failure report, not an exception out of the UI handler.
    report = QualyticsSource.test_connection(
        {"base_url": "acme.qualytics.io", "token": "t"}
    )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert "scheme" in (report.basic_connectivity.failure_reason or "")
