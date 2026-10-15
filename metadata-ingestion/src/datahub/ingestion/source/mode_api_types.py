"""Typed shapes for the Mode Analytics REST API payloads.

These state in one place what the connector expects each Mode object to
contain, rather than leaving it implicit across scattered ``payload.get()``
calls.

Only fields the connector actually reads are modelled. The full payload stays
available on :attr:`_ModeObject.raw` for callers that need it, so there is no
reason to mirror fields nobody consumes.

Mode's API reference is not a reliable description of what arrives, in either
direction, so these definitions follow observed responses:

* Fields the reference marks **required** are routinely missing. Real
  ``/queries`` responses have been seen without ``dbt_metric_token``,
  ``mapping_id``, ``report_imports_count`` or ``dbt_metric_id``.
* ``Report.imported_datasets`` is documented but is not sent by the reports
  listing, which sends an undocumented ``has_imported_datasets`` instead.

Hence every field is optional and a missing value means *unknown*. Reading one
as ``0`` or ``[]`` is what silently dropped charts and dataset links.
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

    Note the absence of ``chart_count``: it is a :class:`ModeReport` field, and
    no Query response has ever carried one. Nor is ``explorations_count``
    modelled -- it counts Visual Explorer analyses, not charts, and reading it
    as a chart count is a bug this connector has already shipped twice.
    """

    id: Optional[int] = None
    token: Optional[str] = None
    raw_query: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    name: Optional[str] = None
    last_run_id: Optional[int] = None
    data_source_id: Optional[int] = None
    _links: Optional[Dict[str, Any]] = None


@dataclass
class ModeReport(_ModeObject):
    """A Mode report, and also a Mode "dataset" -- the API returns datasets as
    report objects, under ``_embedded.reports``.

    https://mode.com/developer/api-reference/analytics/reports/
    """

    id: Optional[int] = None
    token: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[str] = None
    edited_at: Optional[str] = None
    last_saved_at: Optional[str] = None
    last_run_at: Optional[str] = None
    archived: Optional[bool] = None
    view_count: Optional[int] = None
    # Charts belong to a report, not to its queries.
    chart_count: Optional[int] = None
    # Documented, but not sent by the reports listing on at least one live
    # workspace, which sends has_imported_datasets instead. Absent means "ask
    # the single-report endpoint", not "this report imports nothing".
    imported_datasets: Optional[List[Dict[str, Any]]] = None
    # Undocumented, observed on the reports listing.
    has_imported_datasets: Optional[bool] = None
    _links: Optional[Dict[str, Any]] = None


@dataclass
class ModeChart(_ModeObject):
    """A chart built on a Mode query.

    https://mode.com/developer/api-reference/analytics/charts/
    """

    token: Optional[str] = None
    created_at: Optional[str] = None
    view: Optional[Dict[str, Any]] = None
    view_vegas: Optional[Dict[str, Any]] = None
    # Undocumented, observed on real chart payloads.
    updated_at: Optional[str] = None
    _links: Optional[Dict[str, Any]] = None
