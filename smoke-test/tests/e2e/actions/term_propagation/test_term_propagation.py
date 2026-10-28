import logging
from typing import Iterator, List, Tuple

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from tests.e2e.utils import (
    delete_urns,
    unique_dataset_urn,
    unique_suffix,
    wait_for_writes_to_sync,
    with_test_retry,
)
from utilities.domains import Domain
from utilities.metadata_operations import add_term, remove_term

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

PLATFORM = "hive"
FIELD_PATH = "first_name"
# The action writes asynchronously after the term event is consumed, so allow
# longer than the default three attempts.
PROPAGATION_ATTEMPTS = 6


def _schema_metadata(schema_name: str) -> models.SchemaMetadataClass:
    return models.SchemaMetadataClass(
        schemaName=schema_name,
        platform=f"urn:li:dataPlatform:{PLATFORM}",
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


def _emit_lineage_pair(graph_client: DataHubGraph, prefix: str) -> Tuple[str, str]:
    """Create source -> downstream datasets joined by dataset-level lineage only.

    No fine-grained (column) lineage is emitted, so the source field has no
    ``DownstreamOf`` edges of its own and the term has to reach the downstream
    through the parent dataset's lineage.
    """
    source = unique_dataset_urn(f"{prefix}_src", platform=PLATFORM)
    downstream = unique_dataset_urn(f"{prefix}_down", platform=PLATFORM)

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=source, aspect=_schema_metadata(f"{prefix}_src")
        )
    )
    for mcp in MetadataChangeProposalWrapper.construct_many(
        entityUrn=downstream,
        aspects=[
            _schema_metadata(f"{prefix}_down"),
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


def _cleanup(graph_client: DataHubGraph, urns: List[str]) -> None:
    try:
        delete_urns(graph_client, urns)
    except Exception:
        logger.warning("cleanup failed for %s", urns, exc_info=True)


@with_test_retry(max_attempts=PROPAGATION_ATTEMPTS)
def _assert_term_on_dataset(
    graph_client: DataHubGraph, dataset_urn: str, term_urn: str
) -> None:
    terms = graph_client.get_aspect(dataset_urn, models.GlossaryTermsClass)
    assert terms is not None and any(t.urn == term_urn for t in terms.terms), (
        f"term {term_urn} was not propagated to downstream dataset {dataset_urn}"
    )


@with_test_retry(max_attempts=PROPAGATION_ATTEMPTS)
def _assert_term_absent_from_dataset(
    graph_client: DataHubGraph, dataset_urn: str, term_urn: str
) -> None:
    terms = graph_client.get_aspect(dataset_urn, models.GlossaryTermsClass)
    assert terms is None or all(t.urn != term_urn for t in terms.terms), (
        f"term {term_urn} was not removed from downstream dataset {dataset_urn}"
    )


def _term_entries(
    graph_client: DataHubGraph, dataset_urn: str, term_urn: str
) -> List[models.GlossaryTermAssociationClass]:
    terms = graph_client.get_aspect(dataset_urn, models.GlossaryTermsClass)
    return [t for t in (terms.terms if terms else []) if t.urn == term_urn]


@with_test_retry(max_attempts=PROPAGATION_ATTEMPTS)
def _assert_term_entry_count(
    graph_client: DataHubGraph, dataset_urn: str, term_urn: str, expected: int
) -> None:
    entries = _term_entries(graph_client, dataset_urn, term_urn)
    assert len(entries) == expected, (
        f"expected {expected} entries of {term_urn} on {dataset_urn}, got {entries}"
    )


@pytest.fixture(scope="function")
def glossary_term(graph_client: DataHubGraph) -> Iterator[str]:
    name = f"TermProp{unique_suffix()}"
    term_urn = f"urn:li:glossaryTerm:{name}"
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=term_urn,
            aspect=models.GlossaryTermInfoClass(
                name=name,
                definition="smoke test term for term propagation",
                termSource="INTERNAL",
            ),
        )
    )
    wait_for_writes_to_sync(mcp_only=True)
    yield term_urn
    _cleanup(graph_client, [term_urn])


def test_column_level_term_propagates_to_downstream_dataset(
    graph_client: DataHubGraph, auth_session, glossary_term: str
) -> None:
    """Regression for the silent no-op: a term applied to a column must reach the
    downstream dataset. Before the fix the action queried the schemaField URN for
    downstreams (always empty here) and propagated nothing."""
    source, downstream = _emit_lineage_pair(graph_client, "term_prop_col")
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
        # Downstream first, so a late write from the action can't recreate it
        # after its source is gone.
        _cleanup(graph_client, [downstream, source])


def test_dataset_level_term_propagates_to_downstream_dataset(
    graph_client: DataHubGraph, auth_session, glossary_term: str
) -> None:
    source, downstream = _emit_lineage_pair(graph_client, "term_prop_ds")
    try:
        assert add_term(auth_session, source, glossary_term)
        _assert_term_on_dataset(graph_client, downstream, glossary_term)
    finally:
        _cleanup(graph_client, [downstream, source])


def test_column_level_term_removal_propagates_to_downstream_dataset(
    graph_client: DataHubGraph, auth_session, glossary_term: str
) -> None:
    source, downstream = _emit_lineage_pair(graph_client, "term_prop_rm")
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
        _cleanup(graph_client, [downstream, source])


def test_propagated_removal_keeps_user_applied_downstream_term(
    graph_client: DataHubGraph, auth_session, glossary_term: str
) -> None:
    """A propagated removal must only delete the entry the action wrote; a term a
    user applied directly to the downstream dataset must survive it."""
    source, downstream = _emit_lineage_pair(graph_client, "term_prop_keep")
    try:
        assert add_term(auth_session, downstream, glossary_term)
        _assert_term_entry_count(graph_client, downstream, glossary_term, 1)

        assert add_term(
            auth_session,
            source,
            glossary_term,
            sub_resource=FIELD_PATH,
            sub_resource_type="DATASET_FIELD",
        )
        # The user's entry plus the action's attributed entry.
        _assert_term_entry_count(graph_client, downstream, glossary_term, 2)

        assert remove_term(
            auth_session,
            source,
            glossary_term,
            sub_resource=FIELD_PATH,
            sub_resource_type="DATASET_FIELD",
        )
        _assert_term_entry_count(graph_client, downstream, glossary_term, 1)
        (remaining,) = _term_entries(graph_client, downstream, glossary_term)
        assert remaining.attribution is None or not (
            remaining.attribution.source or ""
        ).startswith("urn:li:dataHubAction:")
    finally:
        _cleanup(graph_client, [downstream, source])
