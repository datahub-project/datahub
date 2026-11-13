"""Dataset usage history: get_dataset_usage_history."""

from datetime import datetime, timezone
from typing import Annotated, List, Literal, TypedDict

from datahub.metadata.urns import DatasetUrn, Urn
from fastmcp.exceptions import ToolError
from pydantic import Field

from .. import arg_validation
from ..read_backend import get_read_backend, require_existing
from ..version_requirements import read_only

TimeRange = Literal["DAY", "WEEK", "MONTH", "QUARTER"]


class UsagePoint(TypedDict):
    date: str
    value: int


def _bucket_date(bucket: float) -> str:
    return datetime.fromtimestamp(bucket / 1000, tz=timezone.utc).date().isoformat()


def _points(buckets: object) -> List[UsagePoint]:
    points: List[UsagePoint] = []
    for bucket in buckets if isinstance(buckets, list) else []:
        if not isinstance(bucket, dict) or not isinstance(
            bucket.get("bucket"), (int, float)
        ):
            continue
        value = (bucket.get("metrics") or {}).get("totalSqlQueries")
        if isinstance(value, int):
            points.append({"date": _bucket_date(bucket["bucket"]), "value": value})
    return sorted(points, key=lambda point: point["date"])


@read_only
def get_dataset_usage_history(
    urn: Annotated[str, Field(validation_alias=arg_validation.URN_ALIASES)],
    time_range: TimeRange = "MONTH",
) -> dict:
    """Get query-volume history for a dataset as UTC-dated points.

    Use this tool when the user asks how a dataset's query volume changes over
    time. Resolve the dataset URN first. The returned points can be summarized
    directly or passed unchanged to a host-supported line chart.

    Args:
        urn: Full dataset URN.
        time_range: DAY, WEEK, MONTH, or QUARTER. Defaults to MONTH.

    Returns:
        Dataset URN, requested range, total query count, and dated query-count
        points suitable for a line chart.
    """
    tool = "get_dataset_usage_history"
    urn = arg_validation.require_urn(urn, tool=tool)
    if not isinstance(Urn.from_string(urn), DatasetUrn):
        raise ToolError(f"{tool}: usage is recorded for datasets only, got {urn}.")

    backend = get_read_backend()
    # GMS answers dataset(urn:) with an object even for a URN it has never seen,
    # whose empty usage would read as "never queried".
    require_existing(backend, urn)
    dataset = backend.dataset_usage(urn, time_range)
    if dataset is None:
        raise ToolError(f"Dataset not found: {urn}")
    usage_stats = dataset.get("usageStats")
    if not isinstance(usage_stats, dict):
        raise ToolError(f"Dataset usage is unavailable: {urn}")

    aggregations = usage_stats.get("aggregations")
    total = (
        aggregations.get("totalSqlQueries") if isinstance(aggregations, dict) else None
    )
    return {
        "urn": urn,
        "timeRange": time_range,
        "totalQueryCount": total if isinstance(total, int) else 0,
        "points": _points(usage_stats.get("buckets")),
    }
