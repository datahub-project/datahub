"""Unit tests for PowerBiAPI.fill_metadata_from_scan_result.

Covers the branches added to support cross-workspace scanning:
- Scan-side state/type filter populates the returned excluded_ids set.
- Scan results outside the request batch are skipped (debug log).
- fabric_artifacts is populated from the scan response (not the groups payload).
- Workspaces missing from the scan response surface a reporter warning.
- dataset_registry accumulates datasets across scan batches (the core
  invariant that enables cross-workspace reference resolution).
"""

from typing import Any, Dict, List, Optional, Set, cast
from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Workspace,
    new_powerbi_dataset,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI


def _mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def acquire_token_for_client(self, *args, **kwargs):
            return {"access_token": "dummy"}

    return MsalClient()


@pytest.fixture(autouse=True)
def _patch_msal():
    with mock.patch("msal.ConfidentialClientApplication", side_effect=_mock_msal_cca):
        yield


def _make_workspace(ws_id: str, name: str = "ws") -> Workspace:
    return Workspace(
        id=ws_id,
        name=name,
        type="Workspace",
        webUrl=f"https://app.powerbi.com/groups/{ws_id}",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _make_api(**config_overrides: Any) -> PowerBiAPI:
    config = PowerBiDashboardSourceConfig(
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
        **config_overrides,
    )
    reporter = PowerBiDashboardSourceReport()
    return PowerBiAPI(config=config, reporter=reporter)


def _scan_workspace_payload(
    ws_id: str,
    state: str = "Active",
    ws_type: str = "Workspace",
    name: str = "ws",
    fabric_lakehouses: bool = False,
    datasets: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        Constant.ID: ws_id,
        Constant.NAME: name,
        Constant.STATE: state,
        Constant.TYPE: ws_type,
        "datasets": datasets or [],
        "reports": [],
        "dashboards": [],
    }
    if fabric_lakehouses:
        payload["Lakehouse"] = [
            {
                Constant.ID: "lake-1",
                Constant.NAME: "MyLake",
                "extendedProperties": [],
            }
        ]
    return payload


def test_fill_metadata_returns_excluded_ids_for_scan_excluded_workspaces():
    """Workspaces excluded by the scan-side state/type check are returned in the
    excluded_ids set so callers can skip them in downstream phases."""
    api = _make_api()
    requested = [_make_workspace("WS-A"), _make_workspace("WS-B")]
    scan_response = {
        "workspaces": [
            _scan_workspace_payload("WS-A", state="Active", ws_type="Workspace"),
            _scan_workspace_payload("WS-B", state="Active", ws_type="PersonalGroup"),
        ]
    }
    with (
        mock.patch.object(api, "_get_scan_result", return_value=scan_response),
        mock.patch.object(api, "_get_workspace_datasets", return_value={}),
    ):
        excluded = api.fill_metadata_from_scan_result(workspaces=requested)

    assert excluded == {"WS-B"}, "PersonalGroup should be reported as excluded"
    ws_a, ws_b = requested
    assert ws_a.scan_result.get(Constant.ID) == "WS-A"
    assert ws_b.scan_result == {}, "Excluded workspace must not have scan_result set"


def test_fill_metadata_skips_unknown_workspace_ids_quietly(caplog):
    """Scan can surface workspace IDs outside the request batch (e.g. cross-
    workspace dependencies); these must be skipped with a debug log, not an
    error, and must not affect the return value."""
    api = _make_api()
    requested = [_make_workspace("WS-A")]
    scan_response = {
        "workspaces": [
            _scan_workspace_payload("WS-A"),
            _scan_workspace_payload("WS-EXTRA", name="bonus-from-scan"),
        ]
    }
    with (
        mock.patch.object(api, "_get_scan_result", return_value=scan_response),
        mock.patch.object(api, "_get_workspace_datasets", return_value={}),
        caplog.at_level("DEBUG", logger=PowerBiAPI.__module__),
    ):
        excluded = api.fill_metadata_from_scan_result(workspaces=requested)

    assert excluded == set(), "Bonus workspaces are NOT scan-excluded ones"
    assert any(
        "WS-EXTRA" in r.getMessage() and r.levelname == "DEBUG" for r in caplog.records
    ), "Unknown workspace id must be logged at DEBUG"


def test_fill_metadata_populates_fabric_artifacts_from_scan_response():
    """fabric_artifacts must be populated from the scan response (the groups
    payload has no Lakehouse / warehouses / SQLAnalyticsEndpoint keys)."""
    api = _make_api()
    requested = [_make_workspace("WS-A")]
    scan_response = {
        "workspaces": [
            _scan_workspace_payload("WS-A", fabric_lakehouses=True),
        ]
    }
    with (
        mock.patch.object(api, "_get_scan_result", return_value=scan_response),
        mock.patch.object(api, "_get_workspace_datasets", return_value={}),
    ):
        api.fill_metadata_from_scan_result(workspaces=requested)

    (ws_a,) = requested
    assert "lake-1" in ws_a.fabric_artifacts, (
        "Lakehouse from scan response must populate fabric_artifacts; "
        "without this DirectLake lineage is silently dropped"
    )
    assert ws_a.fabric_artifacts["lake-1"].artifact_type == "Lakehouse"


def test_fill_metadata_warns_per_workspace_when_scan_api_fails():
    """``_get_scan_result`` swallows HTTP errors (401/403/429/5xx) and returns
    None. Each requested workspace must surface an Incomplete Scan Metadata
    warning in the source report so the gap is discoverable; otherwise the
    failure is invisible outside raw logs."""
    api = _make_api()
    requested = [_make_workspace("WS-A"), _make_workspace("WS-B", name="Sales")]
    with mock.patch.object(api, "_get_scan_result", return_value=None):
        excluded = api.fill_metadata_from_scan_result(workspaces=requested)

    assert excluded == set(), (
        "No workspaces are excluded by a failed scan — they must still flow "
        "into Phase 2 with empty scan_result"
    )
    warnings_by_ctx = {
        ctx: w
        for w in api.reporter.warnings
        if w.title == "Incomplete Scan Metadata"
        for ctx in (w.context or [])
    }
    assert any("WS-A" in c for c in warnings_by_ctx), (
        f"WS-A must be flagged with Incomplete Scan Metadata; "
        f"got contexts={list(warnings_by_ctx)}"
    )
    assert any("WS-B" in c for c in warnings_by_ctx), (
        f"WS-B must be flagged with Incomplete Scan Metadata; "
        f"got contexts={list(warnings_by_ctx)}"
    )


def test_get_workspaces_does_not_set_fabric_artifacts_from_groups_payload():
    """Regression guard: fabric_artifacts must be left empty by get_workspaces
    because the groups payload has no Lakehouse keys; populating it here would
    overwrite scan-derived data with {} and silently drop DirectLake lineage."""
    api = _make_api()
    groups_payload: List[Dict[str, Any]] = [
        {
            Constant.ID: "WS-A",
            Constant.NAME: "ws-a",
            Constant.TYPE: "Workspace",
            Constant.STATE: "Active",
            "isReadOnly": False,
        }
    ]
    resolver = api._get_resolver()
    with mock.patch.object(resolver, "get_groups", return_value=groups_payload):
        workspaces = api.get_workspaces()

    assert len(workspaces) == 1
    assert workspaces[0].fabric_artifacts == {}, (
        "fabric_artifacts must not be set from groups payload; it must come "
        "from fill_metadata_from_scan_result"
    )


def test_fill_metadata_warns_when_requested_workspace_missing_from_scan():
    """If the scan API silently omits a requested workspace (admin-API
    permission issue, transient failure, etc.), downstream metadata will be
    incomplete. The source report must surface a warning rather than ingesting
    with empty data silently."""
    api = _make_api()
    requested = [_make_workspace("WS-A"), _make_workspace("WS-B", name="Sales")]
    scan_response = {
        "workspaces": [
            _scan_workspace_payload("WS-A"),
            # WS-B is deliberately omitted to simulate a partial scan response.
        ]
    }
    with (
        mock.patch.object(api, "_get_scan_result", return_value=scan_response),
        mock.patch.object(api, "_get_workspace_datasets", return_value={}),
    ):
        excluded = api.fill_metadata_from_scan_result(workspaces=requested)

    assert excluded == set(), (
        "Omitted workspaces must NOT be added to excluded_ids — they should "
        "still flow into Phase 2 so downstream code can do what it can."
    )
    report = api.reporter
    warning_titles = [w.title for w in report.warnings]
    assert "Incomplete Scan Metadata" in warning_titles, (
        f"Expected 'Incomplete Scan Metadata' warning for omitted WS-B, "
        f"got warning titles={warning_titles}"
    )
    matching = [
        w
        for w in report.warnings
        if w.title == "Incomplete Scan Metadata"
        and any("WS-B" in c for c in (w.context or []))
    ]
    assert matching, "Warning must identify the omitted workspace (WS-B) in its context"


def test_fill_metadata_dataset_registry_accumulates_across_batches():
    """Core invariant enabling cross-workspace reference resolution: datasets
    from all batches must accumulate in dataset_registry so Phase 2 can look up
    datasets owned by other workspaces."""
    api = _make_api()

    batch1 = [_make_workspace("WS-A")]
    batch2 = [_make_workspace("WS-B")]

    scan1 = {
        "workspaces": [
            _scan_workspace_payload(
                "WS-A",
                datasets=[
                    {
                        Constant.ID: "ds-a1",
                        Constant.NAME: "a1",
                        "tables": [],
                    }
                ],
            )
        ]
    }
    scan2 = {
        "workspaces": [
            _scan_workspace_payload(
                "WS-B",
                datasets=[
                    {
                        Constant.ID: "ds-b1",
                        Constant.NAME: "b1",
                        "tables": [],
                    },
                    {
                        Constant.ID: "ds-b2",
                        Constant.NAME: "b2",
                        "tables": [],
                    },
                ],
            )
        ]
    }

    scan_results = [scan1, scan2]

    def _fake_scan_result(_ids):
        return scan_results.pop(0)

    def _fake_get_workspace_datasets(workspace: Workspace) -> Dict[str, Any]:
        return {ds[Constant.ID]: ds for ds in workspace.scan_result.get("datasets", [])}

    with (
        mock.patch.object(api, "_get_scan_result", side_effect=_fake_scan_result),
        mock.patch.object(
            api, "_get_workspace_datasets", side_effect=_fake_get_workspace_datasets
        ),
    ):
        api.fill_metadata_from_scan_result(workspaces=batch1)
        registry_after_batch1 = set(api.dataset_registry.keys())
        api.fill_metadata_from_scan_result(workspaces=batch2)
        registry_after_batch2 = set(api.dataset_registry.keys())

    assert registry_after_batch1 == {"ds-a1"}, (
        f"Batch 1 should add only WS-A's dataset; got {registry_after_batch1}"
    )
    assert registry_after_batch2 == {"ds-a1", "ds-b1", "ds-b2"}, (
        "Batch 2 must ACCUMULATE on top of batch 1, not replace — this is the "
        "invariant that lets Phase 2 resolve cross-workspace dataset refs; "
        f"got {registry_after_batch2}"
    )


def test_fill_metadata_handles_active_scan_entry_without_id():
    """Defensive: if the scan API returns an Active workspace entry missing
    the id field (malformed response), the loop must not raise KeyError —
    it should skip the entry and continue processing the rest."""
    api = _make_api()
    requested = [_make_workspace("WS-A")]
    scan_response = {
        "workspaces": [
            # Malformed entry: no id field.
            {
                Constant.NAME: "no-id",
                Constant.STATE: "Active",
                Constant.TYPE: "Workspace",
                "datasets": [],
                "reports": [],
                "dashboards": [],
            },
            _scan_workspace_payload("WS-A"),
        ]
    }
    with (
        mock.patch.object(api, "_get_scan_result", return_value=scan_response),
        mock.patch.object(api, "_get_workspace_datasets", return_value={}),
    ):
        # Must not raise; well-formed entry must still be processed.
        api.fill_metadata_from_scan_result(workspaces=requested)
    (ws_a,) = requested
    assert ws_a.scan_result.get(Constant.ID) == "WS-A", (
        "Well-formed entry must still populate scan_result after the "
        "malformed entry was skipped"
    )


def test_fill_regular_metadata_detail_with_empty_scan_result_does_not_crash():
    """Regression guard: workspaces missing from the scan response reach
    Phase 2 with scan_result={}. The endorsement and app helpers must
    tolerate this — if any of them ever get refactored to bracket-access
    a key on scan_result, this test will catch the regression."""
    api = _make_api()
    workspace = _make_workspace("WS-MISSING", name="missing-from-scan")
    workspace.scan_result = {}
    resolver = api._get_resolver()

    with (
        mock.patch.object(api, "get_reports", return_value={}),
        mock.patch.object(resolver, "get_dashboards", return_value=[]),
    ):
        # Must not raise on any of: _get_dashboard_endorsements,
        # _get_report_endorsements, or _populate_app_details with scan_result={}.
        api.fill_regular_metadata_detail(workspace=workspace)

    assert workspace.dashboard_endorsements == {}
    assert workspace.report_endorsements == {}
    assert workspace.app is None, (
        "App must remain unset when scan_result={} (no app data available)"
    )


def test_fill_regular_metadata_detail_with_empty_scan_and_endorsements_enabled():
    """Same regression guard as above but with extract_endorsements_to_tags=True
    so the endorsement helpers actually execute against scan_result={}.
    The default config has extract_endorsements_to_tags=False, which would
    otherwise short-circuit those code paths from the test."""
    api = _make_api(extract_endorsements_to_tags=True)
    workspace = _make_workspace("WS-MISSING", name="missing-from-scan")
    workspace.scan_result = {}
    resolver = api._get_resolver()

    with (
        mock.patch.object(api, "get_reports", return_value={}),
        mock.patch.object(resolver, "get_dashboards", return_value=[]),
    ):
        api.fill_regular_metadata_detail(workspace=workspace)

    assert workspace.dashboard_endorsements == {}, (
        "Endorsement helpers must tolerate scan_result={} and return empty"
    )
    assert workspace.report_endorsements == {}
    assert workspace.app is None


def _make_source(
    workspaces_to_return: List[Workspace],
    excluded_ids: Set[str],
    **config_overrides: Any,
) -> PowerBiDashboardSource:
    """Build a PowerBiDashboardSource with a fully-mocked PowerBiAPI so we can
    drive get_workunits_internal end-to-end without hitting the network."""
    config_overrides.setdefault("scan_batch_size", 10)
    config = PowerBiDashboardSourceConfig(
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
        **config_overrides,
    )
    ctx = PipelineContext(run_id="test-run")
    source = PowerBiDashboardSource(config=config, ctx=ctx)
    source.validate_dataset_type_mapping = mock.MagicMock()  # type: ignore[method-assign]
    source.get_allowed_workspaces = mock.MagicMock(  # type: ignore[method-assign]
        return_value=workspaces_to_return
    )
    source.powerbi_client.fill_metadata_from_scan_result = mock.MagicMock(  # type: ignore[method-assign]
        return_value=excluded_ids
    )
    source.powerbi_client.fill_regular_metadata_detail = mock.MagicMock()  # type: ignore[method-assign]
    source.get_workspace_workunit = mock.MagicMock(return_value=iter([]))  # type: ignore[method-assign]
    return source


def test_get_workunits_internal_skips_scan_excluded_workspaces_in_phase_2():
    """When fill_metadata_from_scan_result reports that some workspaces were
    excluded by the scan API, those workspaces must NOT receive
    fill_regular_metadata_detail calls (the redundant API calls this PR
    eliminates)."""
    ws_a = _make_workspace("WS-A", name="kept")
    ws_b = _make_workspace("WS-B", name="excluded")
    ws_c = _make_workspace("WS-C", name="kept-too")
    source = _make_source(
        workspaces_to_return=[ws_a, ws_b, ws_c],
        excluded_ids={"WS-B"},
    )

    list(source.get_workunits_internal())

    fill_detail = cast(
        mock.MagicMock, source.powerbi_client.fill_regular_metadata_detail
    )
    processed = [call.kwargs["workspace"].id for call in fill_detail.call_args_list]
    assert processed == ["WS-A", "WS-C"], (
        f"Phase 2 must skip scan-excluded WS-B; got {processed}"
    )


def test_get_workunits_internal_processes_all_when_no_workspaces_excluded():
    """Baseline: when the scan excludes nothing, every allowed workspace must
    flow through Phase 2 unchanged."""
    workspaces = [
        _make_workspace("WS-A"),
        _make_workspace("WS-B"),
    ]
    source = _make_source(workspaces_to_return=workspaces, excluded_ids=set())

    list(source.get_workunits_internal())

    fill_detail = cast(
        mock.MagicMock, source.powerbi_client.fill_regular_metadata_detail
    )
    processed = [call.kwargs["workspace"].id for call in fill_detail.call_args_list]
    assert processed == ["WS-A", "WS-B"]


def test_get_workunits_internal_modified_since_branch_runs_per_workspace():
    """When modified_since is configured, each workspace must register a fresh
    job_id with the stale entity removal handler before yielding workunits.
    This exercises the modified_since branch of get_workunits_internal that
    wires per-workspace stateful ingestion checkpoints."""
    workspaces = [_make_workspace("WS-A"), _make_workspace("WS-B")]
    source = _make_source(
        workspaces_to_return=workspaces,
        excluded_ids=set(),
        modified_since="2024-01-01T00:00:00",
    )
    source.stale_entity_removal_handler = mock.MagicMock()  # type: ignore[assignment]
    source.state_provider = mock.MagicMock()  # type: ignore[assignment]
    source._apply_workunit_processors = mock.MagicMock(return_value=iter([]))  # type: ignore[method-assign]

    list(source.get_workunits_internal())

    set_job_id = cast(mock.MagicMock, source.stale_entity_removal_handler.set_job_id)
    job_ids = [call.args[0] for call in set_job_id.call_args_list]
    assert job_ids == ["WS-A", "WS-B"], (
        f"set_job_id must be called once per workspace; got {job_ids}"
    )
    assert source._apply_workunit_processors.call_count == 2


def test_get_workunits_internal_batches_phase_1_by_scan_batch_size():
    """Phase 1 must call fill_metadata_from_scan_result once per
    scan_batch_size chunk; the union of returned excluded IDs must drive the
    Phase 2 skip logic across all batches."""
    workspaces = [_make_workspace(f"WS-{i}") for i in range(5)]
    source = _make_source(
        workspaces_to_return=workspaces,
        excluded_ids=set(),
        scan_batch_size=2,
    )
    fill_scan = mock.MagicMock(side_effect=[{"WS-0"}, {"WS-3"}, set()])
    source.powerbi_client.fill_metadata_from_scan_result = fill_scan  # type: ignore[method-assign]

    list(source.get_workunits_internal())

    assert fill_scan.call_count == 3, (
        "scan_batch_size=2 over 5 workspaces must produce 3 batches"
    )
    fill_detail = cast(
        mock.MagicMock, source.powerbi_client.fill_regular_metadata_detail
    )
    processed = [call.kwargs["workspace"].id for call in fill_detail.call_args_list]
    assert processed == ["WS-1", "WS-2", "WS-4"], (
        f"Excluded IDs from any batch must be skipped in Phase 2; got {processed}"
    )


def test_get_workspace_datasets_tolerates_empty_scan_result():
    """Defensive: if a workspace ever reaches _get_workspace_datasets with
    an empty scan_result (e.g. future refactor calls it for scan-omitted
    workspaces), the method must return an empty dict rather than KeyError
    on the warning log line."""
    api = _make_api()
    workspace = _make_workspace("WS-X", name="empty-scan")
    workspace.scan_result = {}
    result = api._get_workspace_datasets(workspace=workspace)
    assert result == {}, "Must return empty dict, not raise"


def test_fill_metadata_filters_workspace_with_non_active_state():
    """The scan-side filter has two independent OR-conditions: state != Active
    and type not in workspace_type_filter. The state branch (e.g. Deleted /
    Orphaned) must also land the workspace in excluded_ids so Phase 2 skips
    it."""
    api = _make_api()
    requested = [_make_workspace("WS-A"), _make_workspace("WS-DEL")]
    scan_response = {
        "workspaces": [
            _scan_workspace_payload("WS-A", state="Active", ws_type="Workspace"),
            _scan_workspace_payload("WS-DEL", state="Deleted", ws_type="Workspace"),
        ]
    }
    with (
        mock.patch.object(api, "_get_scan_result", return_value=scan_response),
        mock.patch.object(api, "_get_workspace_datasets", return_value={}),
    ):
        excluded = api.fill_metadata_from_scan_result(workspaces=requested)

    assert excluded == {"WS-DEL"}, (
        "Non-Active state must be excluded just like wrong-type"
    )
    ws_active, ws_deleted = requested
    assert ws_active.scan_result.get(Constant.ID) == "WS-A"
    assert ws_deleted.scan_result == {}, (
        "Filtered (Deleted) workspace must not have scan_result populated"
    )


def test_fill_metadata_handles_workspaces_none_in_scan_response():
    """Defensive: if the admin scan returns a payload where ``workspaces`` is
    explicitly None (vs. missing or an empty list), the ``or []`` guard must
    prevent a TypeError on iteration. All requested workspaces then surface
    as missing-from-scan via the Incomplete Scan Metadata warning."""
    api = _make_api()
    requested = [_make_workspace("WS-A"), _make_workspace("WS-B")]
    scan_response: Dict[str, Any] = {"workspaces": None}
    with mock.patch.object(api, "_get_scan_result", return_value=scan_response):
        excluded = api.fill_metadata_from_scan_result(workspaces=requested)

    assert excluded == set(), "Nothing was excluded (no workspaces in payload)"
    warned_titles = {w.title for w in api.reporter.warnings}
    assert "Incomplete Scan Metadata" in warned_titles, (
        "Both requested workspaces must be reported as missing from scan; "
        f"got warnings: {warned_titles}"
    )


def test_get_workunits_internal_phase1_batch_failure_isolates_other_batches(caplog):
    """A scan-timeout (or any exception) in one Phase 1 batch must NOT abort
    the whole ingestion. Other batches must still be scanned, all workspaces
    must reach Phase 2, and the affected workspaces must be flagged in the
    source report so the gap is discoverable."""
    workspaces = [
        _make_workspace("WS-0"),
        _make_workspace("WS-1"),
        _make_workspace("WS-2"),
        _make_workspace("WS-3"),
    ]
    source = _make_source(
        workspaces_to_return=workspaces,
        excluded_ids=set(),
        scan_batch_size=2,
    )
    # Batch 1 (WS-0, WS-1) succeeds; batch 2 (WS-2, WS-3) raises.
    source.powerbi_client.fill_metadata_from_scan_result = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=[
            set(),
            ValueError("scan timeout"),
        ]
    )

    with caplog.at_level("WARNING"):
        list(source.get_workunits_internal())

    fill_detail = cast(
        mock.MagicMock, source.powerbi_client.fill_regular_metadata_detail
    )
    processed = [call.kwargs["workspace"].id for call in fill_detail.call_args_list]
    assert processed == ["WS-0", "WS-1", "WS-2", "WS-3"], (
        "All workspaces must reach Phase 2 even when a Phase 1 batch fails; "
        f"got {processed}"
    )

    # context is a LossyList[str] (one entry per workspace in the failed batch);
    # flatten across all "Incomplete Scan Metadata" warnings.
    warned_contexts = [
        ctx
        for w in source.reporter.warnings
        if w.title == "Incomplete Scan Metadata"
        for ctx in w.context
    ]
    assert any("WS-2" in c for c in warned_contexts), (
        f"WS-2 (in failed batch) must be flagged with Incomplete Scan Metadata; got {warned_contexts}"
    )
    assert any("WS-3" in c for c in warned_contexts), (
        f"WS-3 (in failed batch) must be flagged with Incomplete Scan Metadata; got {warned_contexts}"
    )
    assert not any("WS-0" in c for c in warned_contexts), (
        f"WS-0 (in successful batch) must NOT be flagged; got {warned_contexts}"
    )

    # Regression guard: the Phase 1 except block must not emit a separate
    # batch-level logger.warning on top of the per-workspace reporter.warning.
    # reporter.warning(exc=...) already records the traceback once per
    # workspace; an additional standalone logger.warning would produce N+1
    # overlapping log entries for a single failure.
    batch_level_lines = [
        r
        for r in caplog.records
        if r.name.startswith("datahub.ingestion.source.powerbi")
        and "Phase 1 scan batch failed" in r.getMessage()
        and not any(f"WS-{i}" in r.getMessage() for i in range(4))
    ]
    assert batch_level_lines == [], (
        "Phase 1 must not double-log: only per-workspace reporter.warning "
        f"entries are expected; got extra batch-level lines: "
        f"{[r.getMessage() for r in batch_level_lines]}"
    )


# ---------------------------------------------------------------------------
# Workspace.webUrl construction
# ---------------------------------------------------------------------------


def _mock_groups_response(api: PowerBiAPI, groups: List[Dict[str, Any]]) -> Any:
    """Patch the resolver get_groups() call used by PowerBiAPI.get_workspaces."""
    return mock.patch.object(api._get_resolver(), "get_groups", return_value=groups)


def test_get_workspaces_builds_commercial_workspace_url():
    """COMMERCIAL environment surfaces app.powerbi.com URLs by id."""
    api = _make_api()  # COMMERCIAL is the default
    groups = [
        {Constant.ID: "WS-1", Constant.NAME: "ws-one", Constant.TYPE: "Workspace"}
    ]
    with _mock_groups_response(api, groups):
        workspaces = api.get_workspaces()

    assert len(workspaces) == 1
    assert workspaces[0].webUrl == "https://app.powerbi.com/groups/WS-1"


def test_get_workspaces_builds_government_workspace_url():
    """GOVERNMENT environment must use the GCC base URL, not the commercial one."""
    api = _make_api(environment="GOVERNMENT")
    groups = [
        {Constant.ID: "WS-2", Constant.NAME: "ws-two", Constant.TYPE: "Workspace"}
    ]
    with _mock_groups_response(api, groups):
        workspaces = api.get_workspaces()

    assert workspaces[0].webUrl == "https://app.powerbigov.us/groups/WS-2"


def test_get_workspaces_omits_url_for_personal_workspace():
    """PersonalGroup workspaces are not addressable by id in the PowerBI UI;
    surface webUrl=None instead of a dead /groups/{guid} link."""
    api = _make_api()
    groups = [
        {Constant.ID: "WS-A", Constant.NAME: "shared", Constant.TYPE: "Workspace"},
        {Constant.ID: "PG-1", Constant.NAME: "mine", Constant.TYPE: "PersonalGroup"},
        {Constant.ID: "P-1", Constant.NAME: "legacy", Constant.TYPE: "Personal"},
    ]
    with _mock_groups_response(api, groups):
        workspaces = api.get_workspaces()

    by_id = {w.id: w for w in workspaces}
    assert by_id["WS-A"].webUrl == "https://app.powerbi.com/groups/WS-A"
    assert by_id["PG-1"].webUrl is None, (
        "PersonalGroup workspaces are not directly addressable; URL must be None"
    )
    assert by_id["P-1"].webUrl is None, (
        "Legacy Personal workspaces are not directly addressable; URL must be None"
    )


def test_new_powerbi_dataset_uses_raw_web_url_when_present():
    """Regular API responses include `webUrl` directly; we just append /details."""
    workspace = _make_workspace("WS-1")
    raw_instance = {
        "id": "DS-1",
        "name": "ds",
        "webUrl": "https://app.powerbi.com/groups/WS-1/datasets/DS-1",
    }

    dataset = new_powerbi_dataset(workspace, raw_instance)

    assert dataset.webUrl == "https://app.powerbi.com/groups/WS-1/datasets/DS-1/details"


def test_new_powerbi_dataset_imputes_web_url_from_workspace_when_missing():
    """Scan-result payloads omit `webUrl`; impute it from the parent workspace
    so dataset entities still link back to the PowerBI UI."""
    workspace = _make_workspace("WS-2")  # webUrl=https://app.powerbi.com/groups/WS-2
    raw_instance = {"id": "DS-2", "name": "ds"}  # no webUrl

    dataset = new_powerbi_dataset(workspace, raw_instance)

    assert dataset.webUrl == "https://app.powerbi.com/groups/WS-2/datasets/DS-2/details"


def test_new_powerbi_dataset_returns_none_when_workspace_url_missing():
    """Personal/legacy workspaces have webUrl=None; with no raw webUrl either
    we must surface None rather than a string like 'None/details'."""
    workspace = Workspace(
        id="PG-1",
        name="mine",
        type="PersonalGroup",
        webUrl=None,  # personal workspaces aren't UI-addressable
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )
    raw_instance = {"id": "DS-3", "name": "ds"}

    dataset = new_powerbi_dataset(workspace, raw_instance)

    assert dataset.webUrl is None


def test_new_powerbi_dataset_default_for_missing_fields_matches_dataclass_contract():
    """Pin the asymmetric default policy in ``new_powerbi_dataset``:

    - ``description`` is a required ``str`` on ``PowerBIDataset``, so a
      missing field must default to ``""`` (not ``None``, which would
      silently violate the type contract).
    - ``configuredBy`` and ``name`` are ``Optional[str]``, so missing must
      surface as ``None`` -- the owner-emit branch in
      ``powerbi.py`` short-circuits on ``if dataset.configuredBy:``.

    Catches regressions where someone collapses these to a uniform
    ``dict.get`` default pattern and breaks the contract.
    """
    workspace = _make_workspace("WS-4")

    populated = new_powerbi_dataset(
        workspace,
        {
            "id": "DS-4a",
            "name": "ds",
            "description": "library dataset",
            "configuredBy": "user@example.com",
        },
    )
    assert populated.description == "library dataset"
    assert populated.configuredBy == "user@example.com"
    assert populated.name == "ds"

    missing = new_powerbi_dataset(workspace, {"id": "DS-4b"})
    assert missing.description == "", (
        "missing description must default to empty string to satisfy "
        "PowerBIDataset.description: str (not Optional[str])"
    )
    assert missing.configuredBy is None, (
        "missing configuredBy must surface as None so the owner-emit "
        "branch in powerbi.py can short-circuit on `if dataset.configuredBy:`"
    )
    assert missing.name is None, (
        "missing name must surface as None to match PowerBIDataset.name: Optional[str]"
    )

    explicit_null = new_powerbi_dataset(
        workspace, {"id": "DS-4c", "name": "ds", "description": None}
    )
    assert explicit_null.description == "", (
        "explicit description=null must coerce to empty string; dict.get "
        "with a default does not apply when the key is present with None"
    )


def test_new_powerbi_dataset_extracts_dependent_artifact_id_from_relations():
    """``new_powerbi_dataset`` is the sole site that parses scan-result
    relations into ``dependent_on_artifact_id``. Pin the contract: first
    matching relation wins, missing relations leave the field None,
    relations without dependentOnArtifactId are skipped.
    """
    workspace = _make_workspace("WS-DL")

    no_relations = new_powerbi_dataset(workspace, {"id": "DS-DL-a", "name": "n"})
    assert no_relations.dependent_on_artifact_id is None

    empty_relations = new_powerbi_dataset(
        workspace, {"id": "DS-DL-b", "name": "n", Constant.RELATIONS: []}
    )
    assert empty_relations.dependent_on_artifact_id is None

    with_match = new_powerbi_dataset(
        workspace,
        {
            "id": "DS-DL-c",
            "name": "n",
            Constant.RELATIONS: [
                {"name": "noise"},  # no dependentOnArtifactId; skipped
                {Constant.DEPENDENT_ON_ARTIFACT_ID: "ART-1"},
                {Constant.DEPENDENT_ON_ARTIFACT_ID: "ART-2"},  # later match ignored
            ],
        },
    )
    assert with_match.dependent_on_artifact_id == "ART-1"


def test_regular_resolver_get_dataset_parameters_hits_parameters_endpoint():
    api = _make_api()  # admin_apis_only defaults to False -> RegularAPIResolver
    resolver = api._get_resolver()

    fake_response = mock.MagicMock()
    fake_response.json.return_value = {
        "value": [
            {"name": "Server", "currentValue": "host.example.com"},
            {"name": "Database", "currentValue": "library"},
        ]
    }
    fake_response.raise_for_status.return_value = None

    with mock.patch.object(
        resolver._request_session, "get", return_value=fake_response
    ) as mocked_get:
        params = resolver.get_dataset_parameters(workspace_id="WS-1", dataset_id="DS-1")

    assert params == {"Server": "host.example.com", "Database": "library"}
    called_url = mocked_get.call_args[0][0]
    assert called_url.endswith("/groups/WS-1/datasets/DS-1/parameters"), (
        f"expected /parameters endpoint, got: {called_url}"
    )


def test_regular_resolver_get_dataset_parameters_handles_empty_response():
    """Datasets with no parameters return an empty list under `value`."""
    api = _make_api()
    resolver = api._get_resolver()

    fake_response = mock.MagicMock()
    fake_response.json.return_value = {}  # no `value` key at all
    fake_response.raise_for_status.return_value = None

    with mock.patch.object(
        resolver._request_session, "get", return_value=fake_response
    ):
        params = resolver.get_dataset_parameters(workspace_id="WS-1", dataset_id="DS-1")

    assert params == {}


def test_get_workspace_datasets_builds_from_scan_result_without_extra_http():
    api = _make_api()
    workspace = _make_workspace("WS-5", name="ws-five")
    workspace.scan_result = {
        Constant.ID: "WS-5",
        Constant.NAME: "ws-five",
        "datasets": [
            {
                "id": "DS-5",
                "name": "scan-only-dataset",
                "description": "from scan",
                "configuredBy": "owner@example.com",
                "tables": [],
            }
        ],
    }

    # No HTTP for parameters either (kept isolated; the parameters fetch is
    # already wrapped in try/except in _get_workspace_datasets).
    with mock.patch.object(
        api._get_resolver(), "get_dataset_parameters", return_value={}
    ) as mocked_params:
        dataset_map = api._get_workspace_datasets(workspace)

    assert set(dataset_map.keys()) == {"DS-5"}
    dataset = dataset_map["DS-5"]
    assert dataset.name == "scan-only-dataset"
    assert dataset.description == "from scan"
    assert dataset.configuredBy == "owner@example.com"
    # webUrl is imputed from workspace.webUrl since scan results omit it
    assert dataset.webUrl == "https://app.powerbi.com/groups/WS-5/datasets/DS-5/details"
    mocked_params.assert_called_once_with(workspace_id="WS-5", dataset_id="DS-5")
