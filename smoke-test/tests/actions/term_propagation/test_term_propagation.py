import logging
import uuid
from typing import List, Tuple

import pytest
import tenacity

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from tests.utilities.domains import Domain
from tests.utilities.metadata_operations import add_term, remove_term
from tests.utils import delete_urns, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

PLATFORM = "urn:li:dataPlatform:hive"
FIELD_PATH = "first_name"


def _dataset_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:hive,{name},PROD)"


def _schema_metadata(schema_name: str) -> models.SchemaMetadataClass:
    return models.SchemaMetadataClass(
        schemaName=schema_name,
        platform=PLATFORM,
        version=0,
        hash="",
        platformSchema=models.OtherSchemaClass(rawSchema=""),
        fields=[
            models.SchemaFieldClass(
                fieldPath=FIELD_PATH,
                type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                nativeDataType="string",
            )
        ],
    )


def _emit_lineage_pair(graph_client, test_id: str, suffix: str) -> Tuple[str, str]:
    """Create source -> downstream datasets joined by dataset-level lineage only.

    No fine-grained (column) lineage is emitted, so the source field has no
    ``DownstreamOf`` edges of its own. The term must reach the downstream via the
    parent dataset's lineage -- which is exactly the path the bug broke.
    """
    source = _dataset_urn(f"term_prop_{test_id}_{suffix}_src")
    downstream = _dataset_urn(f"term_prop_{test_id}_{suffix}_down")

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=source, aspect=_schema_metadata(f"{suffix}_src")
        )
    )
    for mcp in MetadataChangeProposalWrapper.construct_many(
        entityUrn=downstream,
        aspects=[
            _schema_metadata(f"{suffix}_down"),
            models.UpstreamLineageClass(
                upstreams=[
                    models.UpstreamClass(
                        dataset=source, type=models.DatasetLineageTypeClass.COPY
                    )
                ]
            ),
        ],
    ):
        graph_client.emit(mcp)
    wait_for_writes_to_sync(mcp_only=True)
    return source, downstream


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_delay(90),
)
def _assert_term_on_dataset(graph_client, dataset_urn: str, term_urn: str) -> None:
    terms = graph_client.get_aspect(dataset_urn, models.GlossaryTermsClass)
    assert terms is not None and any(t.urn == term_urn for t in terms.terms), (
        f"term {term_urn} was not propagated to downstream dataset {dataset_urn}"
    )


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_delay(90),
)
def _assert_term_absent_from_dataset(
    graph_client, dataset_urn: str, term_urn: str
) -> None:
    terms = graph_client.get_aspect(dataset_urn, models.GlossaryTermsClass)
    assert terms is None or all(t.urn != term_urn for t in terms.terms), (
        f"term {term_urn} was not removed from downstream dataset {dataset_urn}"
    )


@pytest.fixture(scope="function")
def test_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="function")
def glossary_term(graph_client, test_id: str):
    term_urn = f"urn:li:glossaryTerm:TermProp{test_id}"
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=term_urn,
            aspect=models.GlossaryTermInfoClass(
                name=f"TermProp{test_id}",
                definition="smoke test term for term propagation",
                termSource="INTERNAL",
            ),
        )
    )
    wait_for_writes_to_sync(mcp_only=True)
    yield term_urn
    delete_urns(graph_client, [term_urn])


def test_column_level_term_propagates_to_downstream_dataset(
    graph_client, auth_session, test_id: str, glossary_term: str
) -> None:
    """Regression for the silent no-op: a term applied to a COLUMN must reach the
    downstream dataset. Before the fix the action queried the schemaField URN for
    downstreams (always empty here) and propagated nothing."""
    source, downstream = _emit_lineage_pair(graph_client, test_id, "col")
    urns: List[str] = [source, downstream]
    try:
        assert add_term(
            auth_session,
            source,
            glossary_term,
            sub_resource=FIELD_PATH,
            sub_resource_type="DATASET_FIELD",
        )
        _assert_term_on_dataset(graph_client, downstream, glossary_term)
    finally:
        delete_urns(graph_client, urns)


def test_dataset_level_term_propagates_to_downstream_dataset(
    graph_client, auth_session, test_id: str, glossary_term: str
) -> None:
    """Guard that the already-working dataset-level path is unaffected by the fix."""
    source, downstream = _emit_lineage_pair(graph_client, test_id, "ds")
    urns: List[str] = [source, downstream]
    try:
        assert add_term(auth_session, source, glossary_term)
        _assert_term_on_dataset(graph_client, downstream, glossary_term)
    finally:
        delete_urns(graph_client, urns)


def test_column_level_term_removal_propagates_to_downstream_dataset(
    graph_client, auth_session, test_id: str, glossary_term: str
) -> None:
    """A term removed from a COLUMN must also be removed from the downstream dataset,
    so propagated terms don't accumulate and go stale."""
    source, downstream = _emit_lineage_pair(graph_client, test_id, "rm")
    urns: List[str] = [source, downstream]
    try:
        assert add_term(
            auth_session,
            source,
            glossary_term,
            sub_resource=FIELD_PATH,
            sub_resource_type="DATASET_FIELD",
        )
        _assert_term_on_dataset(graph_client, downstream, glossary_term)

        assert remove_term(
            auth_session,
            source,
            glossary_term,
            sub_resource=FIELD_PATH,
            sub_resource_type="DATASET_FIELD",
        )
        _assert_term_absent_from_dataset(graph_client, downstream, glossary_term)
    finally:
        delete_urns(graph_client, urns)
