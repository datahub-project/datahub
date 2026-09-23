"""Typed shapes for the Mode Analytics REST API payloads.

These state, in one place, what the connector expects each Mode API object to
look like, instead of leaving those expectations implicit across scattered
``payload.get("field")`` calls.

Fields are transcribed from Mode's API reference (linked per class) and
reconciled against responses observed in the wild. The two disagree in both
directions, so neither alone is authoritative:

* The reference marks every Query field *required*, yet real responses omit
  several of them.
* The reference documents ``Report.imported_datasets``, which the reports
  *listing* does not send; the listing sends ``has_imported_datasets`` instead,
  which the reference does not document at all.

Hence **every field is optional**. A missing value means *unknown* -- it must
never be silently read as ``0`` or ``[]``. Two outages came from doing exactly
that: gating chart fetching on a ``chart_count`` that Query objects do not
have, and reading ``imported_datasets`` off a listing that does not send it.
Both defaulted an absent field to empty and failed without a warning.

Each class keeps the untouched payload in ``raw`` for the handful of callers
that walk the response generically, and ``from_api`` ignores unknown keys so a
new Mode field cannot break ingestion.
"""

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, TypeVar

_T = TypeVar("_T", bound="_ModeObject")


@dataclass
class _ModeObject:
    """Base for Mode API payloads: tolerant construction, raw kept."""

    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls: type[_T], payload: Mapping[str, Any]) -> _T:
        """Build from a response object, ignoring fields we do not model."""
        known = {f.name for f in dataclasses.fields(cls)} - {"raw"}
        return cls(
            raw=dict(payload),
            **{k: v for k, v in payload.items() if k in known},
        )


@dataclass
class ModeQuery(_ModeObject):
    """A query within a Mode report.

    https://mode.com/developer/api-reference/analytics/queries/

    There is deliberately no ``chart_count``: the reference does not define
    one, and no observed response has ever carried one. Whether a report has
    charts is :attr:`ModeReport.chart_count`.
    """

    id: Optional[int] = None
    token: Optional[str] = None
    raw_query: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    name: Optional[str] = None
    last_run_id: Optional[int] = None
    data_source_id: Optional[int] = None
    # Visual Explorer analyses -- NOT charts. Gating chart fetching on this
    # dropped charts for every query that had no explorations.
    explorations_count: Optional[int] = None
    report_imports_count: Optional[int] = None
    dbt_metric_id: Optional[str] = None
    dbt_metric_token: Optional[str] = None
    mapping_id: Optional[int] = None
    _links: Optional[Dict[str, Any]] = None


@dataclass
class ModeReport(_ModeObject):
    """A Mode report, and also a Mode "dataset" (the API returns datasets as
    reports, under ``_embedded.reports``).

    https://mode.com/developer/api-reference/analytics/reports/

    The reports listing and the single-report endpoint do not return the same
    fields; where they are known to differ it is noted on the field.
    """

    token: Optional[str] = None
    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    published_at: Optional[str] = None
    edited_at: Optional[str] = None
    theme_id: Optional[int] = None
    color_mappings: Optional[Dict[str, Any]] = None
    type: Optional[str] = None
    last_successful_sync_at: Optional[str] = None
    last_saved_at: Optional[str] = None
    archived: Optional[bool] = None
    space_token: Optional[str] = None
    account_id: Optional[int] = None
    account_username: Optional[str] = None
    public: Optional[bool] = None
    full_width: Optional[bool] = None
    manual_run_disabled: Optional[bool] = None
    drill_anywhere_enabled: Optional[bool] = None
    run_privately: Optional[bool] = None
    drilldowns_enabled: Optional[bool] = None
    layout: Optional[str] = None
    is_embedded: Optional[bool] = None
    is_signed: Optional[bool] = None
    # Documented, but NOT sent by the reports listing on at least one live
    # workspace, which sends has_imported_datasets instead. Absent here means
    # "ask the single-report endpoint", never "this report imports nothing".
    imported_datasets: Optional[List[Dict[str, Any]]] = None
    shared: Optional[bool] = None
    expected_runtime: Optional[float] = None
    last_successfully_run_at: Optional[str] = None
    last_run_at: Optional[str] = None
    web_preview_image: Optional[str] = None
    last_successful_run_token: Optional[str] = None
    github_link: Optional[str] = None
    query_count: Optional[int] = None
    max_query_count: Optional[int] = None
    # Charts belong to a report, not to its queries.
    chart_count: Optional[int] = None
    runs_count: Optional[int] = None
    schedules_count: Optional[int] = None
    query_preview: Optional[str] = None
    view_count: Optional[int] = None
    thoughtspot_published_at: Optional[str] = None
    _links: Optional[Dict[str, Any]] = None

    # Observed in live responses, absent from the API reference.
    has_imported_datasets: Optional[bool] = None
    has_schedules_with_invalid_intervals: Optional[bool] = None
    flamingo_signature: Optional[str] = None


@dataclass
class ModeChart(_ModeObject):
    """A chart built on a Mode query.

    https://mode.com/developer/api-reference/analytics/charts/
    """

    view: Optional[Dict[str, Any]] = None
    view_version: Optional[int] = None
    view_vegas: Optional[Dict[str, Any]] = None
    token: Optional[str] = None
    created_at: Optional[str] = None
    color_palette_token: Optional[str] = None
    _links: Optional[Dict[str, Any]] = None

    # Observed in live responses, absent from the API reference.
    updated_at: Optional[str] = None
    switch_view_token: Optional[str] = None
