import logging
from typing import List, Optional

from datahub.api.entities.datajob import DataJob
from datahub.configuration.common import AllowDenyPattern
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)


def _filter_key(dataset_urn: DatasetUrn) -> str:
    # `<platform>:<name>` (e.g. `file:/tmp/x`, `bigquery:proj._anon.t`) is far
    # easier to write patterns for than the full URN with its parentheses.
    return f"{dataset_urn.get_data_platform_urn().platform_name}:{dataset_urn.name}"


def _allowed(urn: str, pattern: AllowDenyPattern) -> bool:
    # Fine-grained lineage endpoints are usually schemaField URNs; match them on
    # their parent dataset. Anything unparseable is kept rather than silently lost.
    try:
        if urn.startswith("urn:li:schemaField:"):
            urn = SchemaFieldUrn.from_string(urn).parent
        return pattern.allowed(_filter_key(DatasetUrn.from_string(urn)))
    except InvalidUrnError:
        return True


def _filter_fine_grained_lineage(
    fgl: FineGrainedLineageClass, pattern: AllowDenyPattern
) -> Optional[FineGrainedLineageClass]:
    downstreams = [u for u in fgl.downstreams or [] if _allowed(u, pattern)]
    if fgl.downstreams and not downstreams:
        return None

    upstreams = [u for u in fgl.upstreams or [] if _allowed(u, pattern)]
    # An edge whose every source was filtered out would claim the column has no
    # upstream at all, which is wrong; drop it instead.
    if fgl.upstreams and not upstreams:
        return None

    fgl.upstreams = upstreams
    fgl.downstreams = downstreams
    return fgl


def apply_dataset_filter(datajob: DataJob, pattern: AllowDenyPattern) -> None:
    """Drop inlets, outlets and column lineage whose dataset the pattern denies.

    Patterns are matched against `<platform>:<name>` of each dataset.

    Applied once the task's lineage is fully assembled, so it covers every
    source: OpenLineage, SQL parsing, manual inlets/outlets and Airflow Assets.
    The run instance clones its inlets/outlets from the DataJob, so it is
    filtered too.
    """
    if pattern.is_allow_all():
        return

    inlets = [u for u in datajob.inlets if pattern.allowed(_filter_key(u))]
    outlets = [u for u in datajob.outlets if pattern.allowed(_filter_key(u))]

    dropped = (len(datajob.inlets) - len(inlets)) + (
        len(datajob.outlets) - len(outlets)
    )
    if dropped:
        logger.debug(
            f"dataset_filter_str dropped {dropped} inlet/outlet(s) from {datajob.urn}"
        )

    datajob.inlets = inlets
    datajob.outlets = outlets

    fine_grained: List[FineGrainedLineageClass] = []
    for fgl in datajob.fine_grained_lineages:
        kept = _filter_fine_grained_lineage(fgl, pattern)
        if kept is not None:
            fine_grained.append(kept)
    datajob.fine_grained_lineages = fine_grained
