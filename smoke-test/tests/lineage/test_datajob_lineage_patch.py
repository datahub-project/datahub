import logging
from typing import Generator, List

import pytest

from datahub.emitter.mce_builder import make_data_job_urn, make_dataset_urn
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_datajob_input_output_to_patch,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    MetadataChangeProposalClass,
)
from datahub.specific.datajob import DataJobPatchBuilder
from tests.utilities.domains import Domain
from tests.utils import unique_suffix, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.INGESTION)


def _dataset_urn(name: str) -> str:
    return make_dataset_urn(platform="mssql", name=name, env="PROD")


def _input_dataset_edges(graph_client: DataHubGraph, job_urn: str) -> List[str]:
    aspect = graph_client.get_aspect(job_urn, DataJobInputOutputClass)
    assert aspect is not None
    return [edge.destinationUrn for edge in aspect.inputDatasetEdges or []]


@pytest.fixture(scope="module")
def procedure_job_urn(graph_client: DataHubGraph) -> Generator[str, None, None]:
    """A dataJob standing in for an ingested stored procedure."""
    suffix = unique_suffix()
    job_urn = make_data_job_urn(
        orchestrator="mssql",
        flow_id=f"my_db.my_schema.stored_procedures_{suffix}",
        job_id="my_proc",
    )

    yield job_urn

    try:
        graph_client.hard_delete_entity(job_urn)
    except Exception:
        logger.warning("cleanup failed for %s", job_urn, exc_info=True)


def test_procedure_lineage_patch_preserves_manual_edges(
    graph_client: DataHubGraph, procedure_job_urn: str
) -> None:
    """Re-stating parsed lineage the way the MSSQL source does must keep manual edges.

    Drives `convert_datajob_input_output_to_patch` (what the source emits under
    `incremental_lineage`) rather than a hand-built patch, so a regression to a full
    upsert fails this test. The upsert case is covered by a unit test; here we need
    GMS to do the actual merge.
    """
    suffix = unique_suffix()
    manual_upstream = _dataset_urn(f"my_db.my_schema.manual_{suffix}")
    parsed_upstream = _dataset_urn(f"my_db.my_schema.parsed_{suffix}")

    # Seed a manual edge. The UI writes these through the updateLineage mutation,
    # which read-modify-writes the whole aspect and stamps the edge with an actor and
    # time. Patching straight to the same inputDatasetEdges path skips those stamps,
    # which this test doesn't assert on -- it only needs the edge to be present.
    graph_client.emit_mcp(
        next(
            iter(
                DataJobPatchBuilder(procedure_job_urn)
                .add_input_dataset(manual_upstream)
                .build()
            )
        )
    )
    wait_for_writes_to_sync()
    assert manual_upstream in _input_dataset_edges(graph_client, procedure_job_urn)

    # Ingestion re-states what it parsed, through the source's own conversion.
    parsed_aspect = DataJobInputOutputClass(
        inputDatasets=[parsed_upstream],
        outputDatasets=[],
        inputDatajobs=[],
    )
    workunits = convert_datajob_input_output_to_patch(
        procedure_job_urn, parsed_aspect, None
    )
    assert len(workunits) == 1
    patch_mcp = workunits[0].metadata
    assert isinstance(patch_mcp, MetadataChangeProposalClass)
    graph_client.emit_mcp(patch_mcp)
    wait_for_writes_to_sync()

    edges = _input_dataset_edges(graph_client, procedure_job_urn)
    assert manual_upstream in edges
    assert parsed_upstream in edges
