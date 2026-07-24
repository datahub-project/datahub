"""Integration coverage for Dataplex glossary-term reconciliation.

Drives the real ``DataplexGlossaryProcessor`` (both the glossary-listing phase and
the term-asset association phase) against a mocked ``BusinessGlossaryServiceClient``
and a mocked ``DataplexPlatformResourceRepository``, proving that:

* a term reconciled to a DataHub-authored glossary term (managed_by_datahub=True)
  has its native ``dataplex.{...}`` entity suppressed and is emitted on the linked
  asset under the original DataHub URN, and
* an externally-authored term is emitted as a native entity, appears on the linked
  asset under its native URN, and is recorded as an unmanaged platform resource.

This is intentionally a processor-level test (real reconciliation code, mocked
GCP client + platform-resource repository) rather than a full-pipeline golden
test, to keep it focused and non-brittle.
"""

from typing import List, Set, Tuple
from unittest.mock import MagicMock, Mock

from google.cloud import dataplex_v1

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_external_entities import (
    GLOSSARY_TERMS_ASPECT_KEY,
    DataplexAspectId,
    DataplexAspectPlatformResource,
)
from datahub.ingestion.source.dataplex.dataplex_glossary import (
    DataplexGlossaryProcessor,
    DataplexGlossaryReport,
    _term_urn_id,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.metadata.schema_classes import GlossaryTermsClass
from datahub.metadata.urns import GlossaryTermUrn

PROJECT_ID = "my-project"
GLOSSARY_LOCATION = "global"
GLOSSARY_ID = "g1"
TERM_ID = "pii"
ASSET_ENTRY_NAME = (
    "projects/my-project/locations/us/entryGroups/@bigquery/entries/"
    "bigquery:my-project.dataset.customers"
)
ASSET_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.dataset.customers,PROD)"
)
MANAGED_DATAHUB_URN = "urn:li:glossaryTerm:pii"
NATIVE_TERM_URN = str(
    GlossaryTermUrn(_term_urn_id(PROJECT_ID, GLOSSARY_LOCATION, GLOSSARY_ID, TERM_ID))
)


def _make_config() -> DataplexConfig:
    return DataplexConfig(
        project_ids=[PROJECT_ID],
        entries_locations=["us"],
        glossary_locations=[GLOSSARY_LOCATION],
        include_glossaries=True,
        include_glossary_term_associations=True,
    )


def _make_ctx() -> DataplexContext:
    ctx = DataplexContext(config=_make_config(), credentials=None)
    ctx.project_numbers = {PROJECT_ID: "123456789"}
    ctx.entry_data = [
        EntryDataTuple(
            dataplex_entry_short_name="customers",
            dataplex_entry_name=ASSET_ENTRY_NAME,
            dataplex_location="us",
            dataplex_entry_type_short_name="bigquery-table",
            dataplex_entry_fqn="bigquery:my-project.dataset.customers",
            datahub_platform="bigquery",
            datahub_dataset_name="my-project.dataset.customers",
            datahub_dataset_urn=ASSET_DATASET_URN,
        )
    ]
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "entryLinks": [
            {
                "entryLinkType": (
                    "projects/655216118709/locations/global/entryLinkTypes/definition"
                ),
                "entryReferences": [
                    {"type": "SOURCE", "name": ASSET_ENTRY_NAME},
                    {"type": "TARGET", "name": "term-entry-path"},
                ],
            }
        ]
    }
    ctx.authed_session = Mock()
    ctx.authed_session.get.return_value = mock_response
    return ctx


def _make_glossary_client() -> Mock:
    glossary_client = Mock(spec=dataplex_v1.BusinessGlossaryServiceClient)

    glossary = dataplex_v1.Glossary()
    glossary.name = (
        f"projects/{PROJECT_ID}/locations/{GLOSSARY_LOCATION}/glossaries/{GLOSSARY_ID}"
    )
    glossary.display_name = "G1"

    term = dataplex_v1.GlossaryTerm()
    term.name = f"{glossary.name}/terms/{TERM_ID}"
    term.parent = glossary.name
    term.display_name = "PII"

    glossary_client.list_glossaries.return_value = [glossary]
    glossary_client.list_glossary_categories.return_value = []
    glossary_client.list_glossary_terms.return_value = [term]
    return glossary_client


def _entity_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    urns = set()
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.entityUrn is not None
        urns.add(wu.metadata.entityUrn)
    return urns


def _run_processor(
    repo: MagicMock,
) -> Tuple[List[MetadataWorkUnit], List[MetadataWorkUnit]]:
    ctx = _make_ctx()
    processor = DataplexGlossaryProcessor(
        ctx=ctx,
        glossary_client=_make_glossary_client(),
        report=DataplexGlossaryReport(),
        source_report=Mock(),
        platform_resource_repository=repo,
    )
    glossary_workunits = list(processor.process_glossaries([PROJECT_ID], max_workers=1))
    assoc_workunits = list(
        processor.process_term_associations([PROJECT_ID], max_workers=1)
    )
    return glossary_workunits, assoc_workunits


def _glossary_terms_aspect_urns(assoc_workunits: List[MetadataWorkUnit]) -> List[str]:
    glossary_terms_wu = next(
        wu
        for wu in assoc_workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "glossaryTerms"
    )
    assert isinstance(glossary_terms_wu.metadata, MetadataChangeProposalWrapper)
    aspect = glossary_terms_wu.metadata.aspect
    assert isinstance(aspect, GlossaryTermsClass)
    return [t.urn for t in aspect.terms]


def test_managed_term_reconciles_to_datahub_urn_and_suppresses_native_entity() -> None:
    """Scenario A: term is DataHub-authored (managed_by_datahub=True)."""
    repo = MagicMock()
    entity_id = DataplexAspectId(
        aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
        entry_name=ASSET_ENTRY_NAME,
        field_key=MANAGED_DATAHUB_URN,
    )
    repo.search_entity_by_urn.return_value = entity_id
    repo.get_entity_from_datahub.return_value = DataplexAspectPlatformResource(
        managed_by_datahub=True,
        datahub_urn=MANAGED_DATAHUB_URN,
        aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
        entry_name=ASSET_ENTRY_NAME,
        field_key=MANAGED_DATAHUB_URN,
    )

    glossary_workunits, assoc_workunits = _run_processor(repo)

    # The asset's glossaryTerms aspect carries the original DataHub URN, not the
    # native Dataplex one.
    term_urns = _glossary_terms_aspect_urns(assoc_workunits)
    assert term_urns == [MANAGED_DATAHUB_URN]
    assert NATIVE_TERM_URN not in term_urns

    # The native GlossaryTerm entity is suppressed (no MCP for it at all).
    assert NATIVE_TERM_URN not in _entity_urns(glossary_workunits)

    # No new platform resource was written for an already-managed term.
    assert not any(wu.id.startswith("platform_resource-") for wu in assoc_workunits)


def test_external_term_keeps_native_urn_and_records_unmanaged_resource() -> None:
    """Scenario B: term is externally-authored (no reconciliation match)."""
    repo = MagicMock()
    repo.search_entity_by_urn.return_value = None

    glossary_workunits, assoc_workunits = _run_processor(repo)

    # The asset's glossaryTerms aspect carries the native Dataplex URN.
    term_urns = _glossary_terms_aspect_urns(assoc_workunits)
    assert term_urns == [NATIVE_TERM_URN]

    # The native GlossaryTerm entity IS emitted.
    assert NATIVE_TERM_URN in _entity_urns(glossary_workunits)

    # One unmanaged platform resource is recorded per linked asset.
    assert any(wu.id.startswith("platform_resource-") for wu in assoc_workunits)
