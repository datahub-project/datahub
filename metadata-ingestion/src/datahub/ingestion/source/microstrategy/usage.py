"""Usage statistics extraction from the MicroStrategy Platform Analytics cube.

MicroStrategy has no per-object usage REST endpoint; execution telemetry lives
in the Platform Analytics project, surfaced through its aggregate cube. This
module queries that cube through the standard cube instance APIs and folds the
rows into daily per-object usage buckets keyed by object GUID, which the
source joins back to the dashboards and reports it ingested.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from datahub.ingestion.source.microstrategy.client import MicroStrategyClient
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import MSTR_OBJECT_TYPE_REPORT
from datahub.ingestion.source.microstrategy.models import Project
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport

logger = logging.getLogger(__name__)

# Object names in the shipped Platform Analytics aggregate cube. GUIDs are
# stable across shipped versions in practice but undocumented as a contract,
# so everything is resolved by name from the live cube definition.
_DATE_ATTRIBUTE_NAME = "Date"
_PROJECT_ATTRIBUTE_NAME = "Project"
_OBJECT_ATTRIBUTE_NAME = "Object"
_USER_ATTRIBUTE_NAME = "User"
_EXECUTIONS_METRIC_NAMES = ("Num Executions", "Count Actions")
_GUID_FORM_NAME = "GUID"
_NAME_FORM_NAME = "Name"
_ID_FORM_NAME = "ID"
_USER_FORM_NAMES = ("Login", "Name")

# Intelligent cube (776) and super cube (779) subtypes; the quick search for
# the cube name can also match plain reports, which must be skipped.
_CUBE_SUBTYPES = {"776", "779"}

# Date form values are rendered strings whose format follows the
# Intelligence Server locale; month-first is the shipped default.
_DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p")


@dataclass
class UsageQueryPlan:
    """Attribute/form/metric ids resolved by name from the cube definition."""

    date_attribute_id: str
    date_form_id: str
    project_attribute_id: str
    project_form_id: str
    object_attribute_id: str
    object_guid_form_id: str
    object_name_form_id: str
    user_attribute_id: str
    user_form_id: str
    metric_id: str
    metric_name: str


@dataclass
class UsageRow:
    date_text: str
    project_guid: str
    object_guid: str
    object_name: str
    user: str
    count: int


@dataclass
class UsageBucket:
    """One object's usage for one day, aggregated across telemetry rows."""

    object_guid: str
    project_guid: str
    object_name: str
    bucket_start_ms: int
    view_count: int = 0
    user_counts: Dict[str, int] = field(default_factory=dict)


def _find_by_name(items: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    target = name.strip().lower()
    for item in items:
        if str(item.get("name") or "").strip().lower() == target:
            return item
    return None


def _form_id(attribute: Dict[str, Any], *names: str) -> Optional[str]:
    forms = attribute.get("forms")
    if not isinstance(forms, list):
        return None
    for name in names:
        form = _find_by_name([f for f in forms if isinstance(f, dict)], name)
        if form and form.get("id"):
            return str(form["id"])
    return None


def resolve_usage_query_plan(
    cube_definition: Dict[str, Any],
) -> Tuple[Optional[UsageQueryPlan], Optional[str]]:
    """Resolve the query plan from a cube definition, or (None, reason)."""
    definition = cube_definition.get("definition")
    if not isinstance(definition, dict):
        definition = cube_definition
    available = definition.get("availableObjects")
    if not isinstance(available, dict):
        return None, "cube definition has no availableObjects"
    attributes = [a for a in available.get("attributes") or [] if isinstance(a, dict)]
    metrics = [m for m in available.get("metrics") or [] if isinstance(m, dict)]

    date_attr = _find_by_name(attributes, _DATE_ATTRIBUTE_NAME)
    project_attr = _find_by_name(attributes, _PROJECT_ATTRIBUTE_NAME)
    object_attr = _find_by_name(attributes, _OBJECT_ATTRIBUTE_NAME)
    user_attr = _find_by_name(attributes, _USER_ATTRIBUTE_NAME)
    metric = None
    for metric_name in _EXECUTIONS_METRIC_NAMES:
        metric = _find_by_name(metrics, metric_name)
        if metric:
            break

    missing = [
        label
        for label, found in (
            (f"attribute {_DATE_ATTRIBUTE_NAME!r}", date_attr),
            (f"attribute {_PROJECT_ATTRIBUTE_NAME!r}", project_attr),
            (f"attribute {_OBJECT_ATTRIBUTE_NAME!r}", object_attr),
            (f"attribute {_USER_ATTRIBUTE_NAME!r}", user_attr),
            (f"metric {' or '.join(map(repr, _EXECUTIONS_METRIC_NAMES))}", metric),
        )
        if found is None
    ]
    if missing:
        return None, f"cube is missing {', '.join(missing)}"
    assert date_attr and project_attr and object_attr and user_attr and metric

    date_form = _form_id(date_attr, _ID_FORM_NAME)
    project_form = _form_id(project_attr, _GUID_FORM_NAME)
    object_guid_form = _form_id(object_attr, _GUID_FORM_NAME)
    object_name_form = _form_id(object_attr, _NAME_FORM_NAME)
    user_form = _form_id(user_attr, *_USER_FORM_NAMES)
    missing_forms = [
        label
        for label, found in (
            ("Date ID form", date_form),
            ("Project GUID form", project_form),
            ("Object GUID form", object_guid_form),
            ("Object Name form", object_name_form),
            ("User Login/Name form", user_form),
        )
        if not found
    ]
    if missing_forms:
        return None, f"cube is missing {', '.join(missing_forms)}"
    assert (
        date_form and project_form and object_guid_form and object_name_form
    ) and user_form

    return (
        UsageQueryPlan(
            date_attribute_id=str(date_attr["id"]),
            date_form_id=date_form,
            project_attribute_id=str(project_attr["id"]),
            project_form_id=project_form,
            object_attribute_id=str(object_attr["id"]),
            object_guid_form_id=object_guid_form,
            object_name_form_id=object_name_form,
            user_attribute_id=str(user_attr["id"]),
            user_form_id=user_form,
            metric_id=str(metric["id"]),
            metric_name=str(metric.get("name") or ""),
        ),
        None,
    )


def build_usage_query_body(plan: UsageQueryPlan, since_date: str) -> Dict[str, Any]:
    return {
        "requestedObjects": {
            "attributes": [
                {"id": plan.date_attribute_id, "forms": [{"id": plan.date_form_id}]},
                {
                    "id": plan.project_attribute_id,
                    "forms": [{"id": plan.project_form_id}],
                },
                {
                    "id": plan.object_attribute_id,
                    "forms": [
                        {"id": plan.object_guid_form_id},
                        {"id": plan.object_name_form_id},
                    ],
                },
                {"id": plan.user_attribute_id, "forms": [{"id": plan.user_form_id}]},
            ],
            "metrics": [{"id": plan.metric_id}],
        },
        "viewFilter": {
            "operator": "GreaterEqual",
            "operands": [
                {
                    "type": "form",
                    "attribute": {"id": plan.date_attribute_id},
                    "form": {"id": plan.date_form_id},
                },
                {"type": "constant", "dataType": "Date", "value": since_date},
            ],
        },
    }


def parse_usage_page(
    page: Dict[str, Any],
) -> Tuple[List[UsageRow], Optional[int]]:
    """Decode one page of cube grid data into rows; returns (rows, total)."""
    grid = page.get("definition", {}).get("grid", {})
    grid_rows = grid.get("rows")
    data = page.get("data", {})
    header_rows = data.get("headers", {}).get("rows")
    metric_rows = data.get("metricValues", {}).get("raw")
    paging = data.get("paging", {})
    total = paging.get("total") if isinstance(paging, dict) else None
    if not isinstance(total, int):
        total = None
    if (
        not isinstance(grid_rows, list)
        or not isinstance(header_rows, list)
        or not isinstance(metric_rows, list)
    ):
        return [], total

    elements_by_attribute: List[List[Dict[str, Any]]] = []
    for grid_row in grid_rows:
        if not isinstance(grid_row, dict):
            continue
        elements = grid_row.get("elements")
        elements_by_attribute.append(
            [e for e in elements if isinstance(e, dict)]
            if isinstance(elements, list)
            else []
        )
    if len(elements_by_attribute) < 4:
        return [], total

    def form_values(attribute_index: int, element_index: Any) -> List[str]:
        elements = elements_by_attribute[attribute_index]
        if not isinstance(element_index, int) or element_index >= len(elements):
            return []
        values = elements[element_index].get("formValues")
        return [str(v) for v in values] if isinstance(values, list) else []

    rows: List[UsageRow] = []
    for row_index, header in enumerate(header_rows):
        if not isinstance(header, list) or len(header) < 4:
            continue
        date_values = form_values(0, header[0])
        project_values = form_values(1, header[1])
        object_values = form_values(2, header[2])
        user_values = form_values(3, header[3])
        metric_values = metric_rows[row_index] if row_index < len(metric_rows) else None
        count = 0
        if isinstance(metric_values, list) and metric_values:
            try:
                count = int(round(float(metric_values[0])))
            except (TypeError, ValueError):
                count = 0
        if count <= 0:
            continue
        rows.append(
            UsageRow(
                date_text=date_values[0] if date_values else "",
                project_guid=project_values[0] if project_values else "",
                object_guid=object_values[0] if object_values else "",
                object_name=object_values[1] if len(object_values) > 1 else "",
                user=user_values[0] if user_values else "",
                count=count,
            )
        )
    return rows, total


def parse_usage_date_ms(date_text: str) -> Optional[int]:
    """Rendered date form value -> UTC day-bucket start in epoch millis."""
    text = date_text.strip()
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        day = datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)
        return int(day.timestamp() * 1000)
    return None


class MicroStrategyUsageExtractor:
    def __init__(
        self,
        client: MicroStrategyClient,
        config: MicroStrategyConfig,
        report: MicroStrategyReport,
    ):
        self.client = client
        self.config = config
        self.report = report

    def find_platform_analytics_project(
        self,
        projects: List[Project],
    ) -> Optional[Project]:
        target = self.config.platform_analytics_project_name.strip().lower()
        for project in projects:
            if project.name.strip().lower() == target:
                return project
        return None

    def fetch_usage_buckets(self, pa_project_id: str) -> List[UsageBucket]:
        cube_id = self._find_usage_cube_id(pa_project_id)
        if not cube_id:
            self.report.warning(
                title="Platform Analytics Usage Cube Not Found",
                message=(
                    "Skipping usage statistics because the configured cube was "
                    "not found in the Platform Analytics project."
                ),
                context=f"usage_cube_name={self.config.usage_cube_name!r}",
            )
            return []

        cube_definition = self.client.get_cube_definition(pa_project_id, cube_id)
        plan, reason = resolve_usage_query_plan(cube_definition)
        if plan is None:
            self.report.warning(
                title="Platform Analytics Cube Shape Not Recognized",
                message=(
                    "Skipping usage statistics because the usage cube does not "
                    "expose the expected attributes/metrics by name."
                ),
                context=f"cube_id={cube_id}, reason={reason}",
            )
            return []
        logger.info(
            "Querying usage cube %s with metric %r (lookback %d days)",
            cube_id,
            plan.metric_name,
            self.config.usage_lookback_days,
        )

        since = (
            datetime.now(timezone.utc) - timedelta(days=self.config.usage_lookback_days)
        ).date()
        body = build_usage_query_body(plan, since.isoformat())
        rows = self._fetch_all_rows(pa_project_id, cube_id, body)
        self.report.report_usage_rows_scanned(len(rows))
        return self._aggregate(rows)

    def _find_usage_cube_id(self, pa_project_id: str) -> Optional[str]:
        target = self.config.usage_cube_name.strip().lower()
        for item in self.client.search_objects_by_name(
            pa_project_id,
            MSTR_OBJECT_TYPE_REPORT,
            self.config.usage_cube_name,
        ):
            if str(item.get("subtype")) not in _CUBE_SUBTYPES:
                continue
            if str(item.get("name") or "").strip().lower() == target:
                object_id = item.get("id")
                if isinstance(object_id, str) and object_id:
                    return object_id
        return None

    def _fetch_all_rows(
        self,
        pa_project_id: str,
        cube_id: str,
        body: Dict[str, Any],
    ) -> List[UsageRow]:
        page_size = self.config.page_size
        first_page = self.client.execute_cube_query(
            pa_project_id,
            cube_id,
            body,
            limit=page_size,
        )
        instance_id = first_page.get("instanceId") or first_page.get("id")
        rows, total = parse_usage_page(first_page)
        all_rows = list(rows)
        offset = page_size
        while (
            isinstance(instance_id, str)
            and instance_id
            and total is not None
            and offset < total
        ):
            page = self.client.get_cube_instance_page(
                pa_project_id,
                cube_id,
                instance_id,
                limit=page_size,
                offset=offset,
            )
            page_rows, _ = parse_usage_page(page)
            if not page_rows:
                break
            all_rows.extend(page_rows)
            offset += page_size
            logger.info("Usage cube pagination progress: %d/%d rows", offset, total)
        return all_rows

    def _aggregate(self, rows: List[UsageRow]) -> List[UsageBucket]:
        buckets: Dict[Tuple[str, int], UsageBucket] = {}
        for row in rows:
            if not row.object_guid:
                continue
            bucket_start_ms = parse_usage_date_ms(row.date_text)
            if bucket_start_ms is None:
                self.report.report_usage_date_unparsed()
                logger.debug("Unparseable usage date value: %r", row.date_text)
                continue
            key = (row.object_guid.upper(), bucket_start_ms)
            bucket = buckets.get(key)
            if bucket is None:
                bucket = UsageBucket(
                    object_guid=row.object_guid.upper(),
                    project_guid=row.project_guid.upper(),
                    object_name=row.object_name,
                    bucket_start_ms=bucket_start_ms,
                )
                buckets[key] = bucket
            bucket.view_count += row.count
            user = row.user.strip()
            if user:
                bucket.user_counts[user] = bucket.user_counts.get(user, 0) + row.count
        return sorted(
            buckets.values(),
            key=lambda b: (b.object_guid, b.bucket_start_ms),
        )
