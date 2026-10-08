import logging
from dataclasses import dataclass
from typing import List, Optional

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

    def is_noop(self) -> bool:
        return (
            self.pattern.is_allow_all() and not self.bigquery_temp_table_dataset_prefix
        )

    def allowed(self, dataset_urn: DatasetUrn) -> bool:
        if self._is_bigquery_temp(dataset_urn):
            return False
        return self.pattern.allowed(_filter_key(dataset_urn))

    def _is_bigquery_temp(self, dataset_urn: DatasetUrn) -> bool:
        prefix = self.bigquery_temp_table_dataset_prefix
        if not prefix:
            return False
        if dataset_urn.get_data_platform_urn().platform_name != BIGQUERY_PLATFORM:
            return False
        # `<project>.<dataset>.<table>`
        parts = dataset_urn.name.split(".")
        return len(parts) >= 3 and parts[-2].startswith(prefix)


def _allowed(urn: str, dataset_filter: DatasetFilter) -> bool:
    # Fine-grained lineage endpoints are usually schemaField URNs; match them on
    # their parent dataset. Anything unparseable is kept rather than silently lost.
    try:
        if urn.startswith("urn:li:schemaField:"):
            urn = SchemaFieldUrn.from_string(urn).parent
        return dataset_filter.allowed(DatasetUrn.from_string(urn))
    except InvalidUrnError:
        return True


def _filter_fine_grained_lineage(
    fgl: FineGrainedLineageClass, dataset_filter: DatasetFilter
) -> Optional[FineGrainedLineageClass]:
    downstreams = [u for u in fgl.downstreams or [] if _allowed(u, dataset_filter)]
    if fgl.downstreams and not downstreams:
        return None

    upstreams = [u for u in fgl.upstreams or [] if _allowed(u, dataset_filter)]
    # An edge whose every source was filtered out would claim the column has no
    # upstream at all, which is wrong; drop it instead.
    if fgl.upstreams and not upstreams:
        return None

    fgl.upstreams = upstreams
    fgl.downstreams = downstreams
    return fgl


def apply_dataset_filter(datajob: DataJob, dataset_filter: DatasetFilter) -> None:
    """Drop inlets, outlets and column lineage of datasets the filter rejects.

    Patterns are matched against `<platform>:<name>` of each dataset; BigQuery
    tables in hidden (temp-prefixed) datasets are always rejected.

    Applied once the task's lineage is fully assembled, so it covers every
    source: OpenLineage, SQL parsing, manual inlets/outlets and Airflow Assets.
    The run instance clones its inlets/outlets from the DataJob, so it is
    filtered too.
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

    fine_grained: List[FineGrainedLineageClass] = []
    for fgl in datajob.fine_grained_lineages:
        kept = _filter_fine_grained_lineage(fgl, dataset_filter)
        if kept is not None:
            fine_grained.append(kept)
    datajob.fine_grained_lineages = fine_grained
