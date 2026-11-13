from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Sequence, Union

from typing_extensions import TypeAlias

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.metadata.urns import MetricUrn
from datahub.sdk._semantic_shared import as_input_list
from datahub.sdk.entity import Entity
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.sdk.metric import Metric

MetricInputType: TypeAlias = Union[str, MetricUrn, "Metric"]
UpstreamMetricsInputType: TypeAlias = Union[MetricInputType, Sequence[MetricInputType]]

_METRIC_AS_DATASET_INPUT = (
    "Metric URNs cannot be set as dataset inputs. Use set_upstream_metrics() instead."
)


def _is_metric_urn_like(value: object) -> bool:
    if isinstance(value, MetricUrn):
        return True
    if isinstance(value, str):
        return value.startswith("urn:li:metric:")
    if isinstance(value, models.UpstreamClass):
        dataset = value.dataset
        return isinstance(dataset, MetricUrn) or (
            isinstance(dataset, str) and dataset.startswith("urn:li:metric:")
        )
    urn = getattr(value, "urn", None)
    return isinstance(urn, MetricUrn)


def _reject_metric_as_dataset_input(value: object) -> None:
    """Keep a metric URN from being stored as a dataset upstream."""
    if _is_metric_urn_like(value):
        raise SdkUsageError(_METRIC_AS_DATASET_INPUT)


def _metric_urn_from_input(value: object, index: int) -> MetricUrn:
    if isinstance(value, MetricUrn):
        return value
    if isinstance(value, str):
        try:
            return MetricUrn.from_string(value)
        except InvalidUrnError as e:
            raise SdkUsageError(
                f"upstream_metrics[{index}] is not a metric URN: {value}"
            ) from e
    urn = getattr(value, "urn", None)
    if isinstance(urn, MetricUrn):
        return urn
    raise SdkUsageError(f"upstream_metrics[{index}] is not a metric URN: {value!r}")


def _parse_upstream_metrics_input(
    upstream_metrics: UpstreamMetricsInputType,
) -> models.UpstreamMetricsClass:
    """Build a full-replace upstreamMetrics aspect. Raises before any write."""
    edges: List[models.EdgeClass] = []
    seen: set[str] = set()
    for index, item in enumerate(as_input_list(upstream_metrics)):
        metric_urn = _metric_urn_from_input(item, index)
        key = str(metric_urn)
        if key in seen:
            continue
        seen.add(key)
        edges.append(models.EdgeClass(destinationUrn=key))
    return models.UpstreamMetricsClass(metrics=edges)


class HasUpstreamMetrics(Entity):
    """Metrics this entity reads, stored on the upstreamMetrics aspect.

    Omit the constructor argument and the aspect is not emitted, so an upsert
    does not clear edges that updateLineage already stored. ``[]`` emits an
    empty list, which clears those edges. The server rejects a dataset that
    lists a metric which already lists that dataset.
    """

    __slots__ = ()

    @property
    def upstream_metrics(self) -> Optional[List[MetricUrn]]:
        """None when the aspect is absent; [] after an explicit clear."""
        aspect = self._get_aspect(models.UpstreamMetricsClass)
        if aspect is None:
            return None
        return [MetricUrn.from_string(edge.destinationUrn) for edge in aspect.metrics]

    def set_upstream_metrics(self, upstream_metrics: UpstreamMetricsInputType) -> None:
        """Replace the full set of metrics this entity reads. Pass [] to clear.

        A dataset↔metric cycle is rejected by the server when the metric
        already lists this dataset. This method does not read the graph.
        """
        self._set_aspect(_parse_upstream_metrics_input(upstream_metrics))
