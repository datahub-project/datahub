import logging
import re
from dataclasses import dataclass

from datahub.api.entities.datajob import DataJob
from datahub.configuration.common import AllowDenyPattern
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)

BIGQUERY_PLATFORM = "bigquery"


def _filter_key(dataset_urn: DatasetUrn) -> str:
    # `<platform>:<name>` (e.g. `file:/tmp/x`, `bigquery:proj._anon.t`) is far
    # easier to write patterns for than the full URN with its parentheses.
    return f"{dataset_urn.get_data_platform_urn().platform_name}:{dataset_urn.name}"


@dataclass(frozen=True)
class DatasetFilter:
    pattern: AllowDenyPattern
    bigquery_temp_table_dataset_prefix: str
    capture_bigquery_temp_datasets: bool = False

    def is_noop(self) -> bool:
        return self.pattern.is_allow_all() and (
            self.capture_bigquery_temp_datasets
            or not self.bigquery_temp_table_dataset_prefix
        )

    def allowed(self, dataset_urn: DatasetUrn) -> bool:
        if self._is_bigquery_temp(dataset_urn):
            return False
        return self.pattern.allowed(_filter_key(dataset_urn))

    def _is_bigquery_temp(self, dataset_urn: DatasetUrn) -> bool:
        prefix = self.bigquery_temp_table_dataset_prefix
        if self.capture_bigquery_temp_datasets or not prefix:
            return False
        if dataset_urn.get_data_platform_urn().platform_name != BIGQUERY_PLATFORM:
            return False
        # `<project>.<dataset>.<table>` from OpenLineage and SQL parsing, but
        # `<project>/<dataset>/<table>` from `bigquery://` Airflow Assets.
        parts = re.split(r"[./]", dataset_urn.name)
        return len(parts) >= 3 and parts[-2].startswith(prefix)


def _allowed(urn: str, dataset_filter: DatasetFilter) -> bool:
    try:
        if urn.startswith("urn:li:schemaField:"):
            urn = SchemaFieldUrn.from_string(urn).parent
        return dataset_filter.allowed(DatasetUrn.from_string(urn))
    except InvalidUrnError:
        logger.debug(f"Dataset filter keeping unparseable URN: {urn}")
        return True


def _prune_fine_grained_lineage(
    fgl: FineGrainedLineageClass, dataset_filter: DatasetFilter
) -> bool:
    """Prune denied endpoints in place; False means the edge must be dropped."""
    downstreams = [u for u in fgl.downstreams or [] if _allowed(u, dataset_filter)]
    if fgl.downstreams and not downstreams:
        return False

    upstreams = [u for u in fgl.upstreams or [] if _allowed(u, dataset_filter)]
    # An edge whose every source was filtered out would claim the column has no
    # upstream at all, which is wrong; drop it instead.
    if fgl.upstreams and not upstreams:
        return False

    if fgl.upstreams:
        fgl.upstreams = upstreams
    if fgl.downstreams:
        fgl.downstreams = downstreams
    return True


def apply_dataset_filter(datajob: DataJob, dataset_filter: DatasetFilter) -> None:
    """Drop inlets, outlets and column lineage of datasets the filter rejects.

    Patterns are matched against `<platform>:<name>` of each dataset; BigQuery
    tables in hidden (temp-prefixed) datasets are always rejected.
    """
    if dataset_filter.is_noop():
        return

    inlets = [u for u in datajob.inlets if dataset_filter.allowed(u)]
    outlets = [u for u in datajob.outlets if dataset_filter.allowed(u)]

    dropped = (len(datajob.inlets) - len(inlets)) + (
        len(datajob.outlets) - len(outlets)
    )
    if dropped:
        logger.debug(
            f"Dataset filter dropped {dropped} inlet/outlet(s) from {datajob.urn}"
        )

    datajob.inlets = inlets
    datajob.outlets = outlets

    datajob.fine_grained_lineages = [
        fgl
        for fgl in datajob.fine_grained_lineages
        if _prune_fine_grained_lineage(fgl, dataset_filter)
    ]
