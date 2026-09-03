"""Unit tests for Dataplex Business Glossary ingestion."""

import urllib.parse
from typing import List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
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
    GlossaryTermRef,
    _category_node_urn_id,
    _glossary_node_urn_id,
    _parse_parent_urn,
    _resource_id,
    _term_urn_id,
)
from datahub.metadata.schema_classes import GlossaryTermsClass
from datahub.metadata.urns import GlossaryNodeUrn, GlossaryTermUrn

# ---------------------------------------------------------------------------
# URN helpers
# ---------------------------------------------------------------------------


class TestUrnHelpers:
    def test_glossary_urn_id(self) -> None:
        result = _glossary_node_urn_id("my-project", "global", "my-glossary")
        assert result == "dataplex.my-project.global.my-glossary"

    def test_category_urn_id(self) -> None:
        result = _category_node_urn_id("my-project", "global", "my-glossary", "cat-1")
        assert result == "dataplex.my-project.global.my-glossary.cat-1"

    def test_term_urn_id(self) -> None:
        result = _term_urn_id("my-project", "global", "my-glossary", "term-1")
        assert result == "dataplex.my-project.global.my-glossary.term-1"

    @pytest.mark.parametrize(
        "resource_name, expected",
        [
            (
                "projects/my-project/locations/global/glossaries/my-glossary",
                "my-glossary",
            ),
            (
                "projects/my-project/locations/global/glossaries/g1/terms/t1",
                "t1",
            ),
        ],
    )
    def test_resource_id_extracts_last_segment(
        self, resource_name: str, expected: str
    ) -> None:
        assert _resource_id(resource_name) == expected


# ---------------------------------------------------------------------------
# Parent URN parsing
# ---------------------------------------------------------------------------


class TestParseParentUrn:
    @pytest.mark.parametrize(
        "parent, expected_urn_id",
        [
            (
                "projects/my-project/locations/global/glossaries/my-glossary",
                _glossary_node_urn_id("my-project", "global", "my-glossary"),
            ),
            (
                "projects/my-project/locations/global/glossaries/my-glossary/categories/cat-1",
                _category_node_urn_id("my-project", "global", "my-glossary", "cat-1"),
            ),
        ],
    )
    def test_valid_parent_resolves_to_glossary_node_urn(
        self, parent: str, expected_urn_id: str
    ) -> None:
        result = _parse_parent_urn(parent, "my-project", "global", "my-glossary")
        assert result == str(GlossaryNodeUrn(expected_urn_id))

    def test_invalid_parent_raises(self) -> None:
        with pytest.raises(ValueError, match="Unexpected"):
            _parse_parent_urn(
                "projects/my-project/locations/global/something/unknown",
                "my-project",
                "global",
                "my-glossary",
            )


# ---------------------------------------------------------------------------
# Processor: glossary ingestion
# ---------------------------------------------------------------------------


@pytest.fixture
def config() -> DataplexConfig:
    return DataplexConfig(
        project_ids=["my-project"],
        glossary_locations=["global"],
    )


@pytest.fixture
def ctx(config: DataplexConfig) -> DataplexContext:
    return DataplexContext(config=config, credentials=None)


@pytest.fixture
def glossary_client() -> Mock:
    return Mock(spec=dataplex_v1.BusinessGlossaryServiceClient)


@pytest.fixture
def source_report() -> Mock:
    return Mock()


@pytest.fixture
def processor(
    ctx: DataplexContext, glossary_client: Mock, source_report: Mock
) -> DataplexGlossaryProcessor:
    return DataplexGlossaryProcessor(
        ctx=ctx,
        glossary_client=glossary_client,
        report=DataplexGlossaryReport(),
        source_report=source_report,
    )


def _make_glossary(
    name: str = "projects/my-project/locations/global/glossaries/g1",
    display_name: str = "G1",
) -> dataplex_v1.Glossary:
    g = dataplex_v1.Glossary()
    g.name = name
    g.display_name = display_name
    return g


def _make_category(
    name: str,
    parent: str,
    display_name: str = "Cat",
) -> dataplex_v1.GlossaryCategory:
    c = dataplex_v1.GlossaryCategory()
    c.name = name
    c.parent = parent
    c.display_name = display_name
    return c


def _make_term(
    name: str,
    parent: str,
    display_name: str = "Term",
) -> dataplex_v1.GlossaryTerm:
    t = dataplex_v1.GlossaryTerm()
    t.name = name
    t.parent = parent
    t.display_name = display_name
    return t


class TestProcessGlossaries:
    def test_emits_glossary_node_category_and_term(
        self,
        processor: DataplexGlossaryProcessor,
        glossary_client: Mock,
    ) -> None:
        glossary = _make_glossary()
        glossary_client.list_glossaries.return_value = [glossary]

        cat_name = "projects/my-project/locations/global/glossaries/g1/categories/c1"
        term_name = "projects/my-project/locations/global/glossaries/g1/terms/t1"
        glossary_client.list_glossary_categories.return_value = [
            _make_category(
                name=cat_name,
                parent="projects/my-project/locations/global/glossaries/g1",
                display_name="Finance",
            )
        ]
        glossary_client.list_glossary_terms.return_value = [
            _make_term(
                name=term_name,
                parent=cat_name,
                display_name="Revenue",
            )
        ]

        workunits = list(processor.process_glossaries(["my-project"], max_workers=1))
        assert len(workunits) > 0

        # Verify the emitted terms list was populated
        assert len(processor._emitted_terms) == 1
        term_ref = processor._emitted_terms[0]
        assert term_ref.project_id == "my-project"
        assert term_ref.location == "global"
        assert term_ref.glossary_id == "g1"
        assert term_ref.term_id == "t1"

    def test_term_directly_under_glossary(
        self,
        processor: DataplexGlossaryProcessor,
        glossary_client: Mock,
    ) -> None:
        """Term whose parent is the glossary directly (no category)."""
        glossary = _make_glossary()
        glossary_client.list_glossaries.return_value = [glossary]
        glossary_client.list_glossary_categories.return_value = []
        glossary_client.list_glossary_terms.return_value = [
            _make_term(
                name="projects/my-project/locations/global/glossaries/g1/terms/t1",
                parent="projects/my-project/locations/global/glossaries/g1",
                display_name="Direct Term",
            )
        ]

        list(processor.process_glossaries(["my-project"], max_workers=1))

        # Term is emitted and its parent resolves to the glossary GlossaryNode URN.
        assert len(processor._emitted_terms) == 1

    def test_empty_glossary_emits_only_node(
        self,
        processor: DataplexGlossaryProcessor,
        glossary_client: Mock,
    ) -> None:
        glossary = _make_glossary()
        glossary_client.list_glossaries.return_value = [glossary]
        glossary_client.list_glossary_categories.return_value = []
        glossary_client.list_glossary_terms.return_value = []

        workunits = list(processor.process_glossaries(["my-project"], max_workers=1))

        assert len(workunits) > 0
        assert len(processor._emitted_terms) == 0


# ---------------------------------------------------------------------------
# Processor: term-asset associations
# ---------------------------------------------------------------------------

# A Dataplex entry resource name as the Catalog API returns it during the entries
# stage: the entry-group project is the project ID.
_ASSET_ENTRY_NAME = (
    "projects/my-project/locations/us-central1/entryGroups/@bigquery/entries/"
    "bigquery.googleapis.com/projects/my-project/datasets/ds1/tables/table1"
)
# The same entry as lookupEntryLinks returns it: the entry-group project is the
# project NUMBER. Only the leading segment differs -- the inner asset path stays
# project-ID based. Matching the two is what _normalize_entry_project_id does.
_ASSET_ENTRY_NAME_API_FORM = (
    "projects/123456789/locations/us-central1/entryGroups/@bigquery/entries/"
    "bigquery.googleapis.com/projects/my-project/datasets/ds1/tables/table1"
)
_ASSET_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.ds1.table1,PROD)"
)


def _definition_link_response(
    source_entry_name: str = _ASSET_ENTRY_NAME_API_FORM,
) -> Mock:
    """A lookupEntryLinks 200 carrying one definition link to ``source_entry_name``."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "entryLinks": [
            {
                "entryLinkType": (
                    "projects/655216118709/locations/global/entryLinkTypes/definition"
                ),
                "entryReferences": [
                    {"type": "SOURCE", "name": source_entry_name},
                    {"type": "TARGET", "name": "term-path"},
                ],
            }
        ]
    }
    return response


class TestProcessTermAssociations:
    def _setup_processor_with_terms(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """Seed the processor with one emitted term and one entry in ctx."""
        processor._emitted_terms = [
            GlossaryTermRef(
                project_id="my-project",
                location="global",
                glossary_id="g1",
                term_id="t1",
            )
        ]
        ctx.project_numbers = {"my-project": "123456789"}
        ctx.entry_name_to_urn = {_ASSET_ENTRY_NAME: _ASSET_DATASET_URN}

    def test_association_resolved_from_entry_data(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        self._setup_processor_with_terms(processor, ctx)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {"type": "SOURCE", "name": _ASSET_ENTRY_NAME_API_FORM},
                        {"type": "TARGET", "name": "term-path"},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(processor.process_term_associations(max_workers=1))

        assert len(workunits) > 0
        assert processor._report.term_associations_emitted == 1

    def test_unknown_entry_skipped_counted_and_warned(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
        source_report: Mock,
    ) -> None:
        self._setup_processor_with_terms(processor, ctx)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {
                            "type": "SOURCE",
                            "name": "projects/other/locations/us/entries/unknown-entry",
                        },
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(processor.process_term_associations(max_workers=1))

        assert len(workunits) == 0
        assert processor._report.term_associations_emitted == 0
        # Counted rather than only logged at DEBUG: links resolving but never binding
        # is otherwise indistinguishable from a glossary with no links at all.
        assert processor._report.term_links_unmatched == 1
        assert processor._report.term_links_matched == 0
        assert source_report.warning.call_args.kwargs["title"] == (
            "No Dataplex term links matched an ingested asset"
        )

    def test_lookup_entry_links_returns_empty_on_error_status(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        ctx.authed_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 404
        ctx.authed_session.get.return_value = mock_response

        result = processor._lookup_entry_links("my-project", "us-central1", "term-path")
        assert result == []

    def test_non_definition_links_ignored(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        self._setup_processor_with_terms(processor, ctx)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    # synonym link — should be ignored
                    "entryLinkType": "projects/dataplex-types/locations/global/entryLinkTypes/synonym",
                    "entryReferences": [
                        {"type": "SOURCE", "name": _ASSET_ENTRY_NAME_API_FORM},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(processor.process_term_associations(max_workers=1))

        assert len(workunits) == 0

    def test_asset_linked_to_multiple_terms_emits_one_mcp_with_all_terms(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """Asset linked to two terms must receive both in a single MCP, not two separate ones."""
        processor._emitted_terms = [
            GlossaryTermRef(
                project_id="my-project",
                location="global",
                glossary_id="g1",
                term_id="t1",
            ),
            GlossaryTermRef(
                project_id="my-project",
                location="global",
                glossary_id="g1",
                term_id="t2",
            ),
        ]
        ctx.project_numbers = {"my-project": "123456789"}
        ctx.entry_name_to_urn = {_ASSET_ENTRY_NAME: _ASSET_DATASET_URN}

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {"type": "SOURCE", "name": _ASSET_ENTRY_NAME_API_FORM},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(processor.process_term_associations(max_workers=1))

        # One MCP emitted (one asset), not two (which would overwrite each other).
        assert processor._report.term_associations_emitted == 1
        assert len(workunits) > 0

    def test_reconciled_term_substitutes_datahub_urn(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """A DataHub-authored term (datahub_term_urn set) must appear on the asset
        under its original DataHub URN, and must not be recorded as an external link."""
        self._setup_processor_with_terms(processor, ctx)
        processor._emitted_terms[0].datahub_term_urn = "urn:li:glossaryTerm:pii"
        repo = MagicMock()
        processor._platform_resource_repository = repo

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {"type": "SOURCE", "name": _ASSET_ENTRY_NAME_API_FORM},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(processor.process_term_associations(max_workers=1))

        term_urns = _glossary_term_urns(workunits)
        assert term_urns == ["urn:li:glossaryTerm:pii"]
        # DataHub-authored term: no external-ownership marker emitted.
        assert not _platform_resource_wus(workunits)

    def test_external_term_uses_native_urn_and_records_link(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """An externally-authored term (datahub_term_urn unset) must appear under its
        native Dataplex URN, and the link must be recorded as unmanaged."""
        self._setup_processor_with_terms(processor, ctx)
        repo = MagicMock()
        processor._platform_resource_repository = repo
        native_term_urn = str(
            GlossaryTermUrn(_term_urn_id("my-project", "global", "g1", "t1"))
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {"type": "SOURCE", "name": _ASSET_ENTRY_NAME_API_FORM},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(processor.process_term_associations(max_workers=1))

        term_urns = _glossary_term_urns(workunits)
        assert term_urns == [native_term_urn]
        # External term: an unmanaged platform-resource marker is emitted (workunit).
        assert _platform_resource_wus(workunits)

    def test_container_asset_receives_terms(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """A term linked to a BigQuery dataset resolves to a DataHub Container, which
        must be tagged like any other asset."""
        self._setup_processor_with_terms(processor, ctx)
        container_urn = "urn:li:container:8fd1a4c0b2e34f5a9c7d6e1b0a2f3c4d"
        ctx.entry_name_to_urn = {_ASSET_ENTRY_NAME: container_urn}

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = _definition_link_response()
            workunits = list(processor.process_term_associations(max_workers=1))

        assert _glossary_term_urns(workunits) == [
            "urn:li:glossaryTerm:dataplex.my-project.global.g1.t1"
        ]
        assert [
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.aspectName == "glossaryTerms"
        ] == [container_urn]
        assert processor._report.term_associations_emitted == 1

    def test_only_glossary_terms_aspect_emitted(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """Associating a term must write glossaryTerms and nothing else -- emitting via
        an SDK entity would also write dataPlatformInstance (Dataset) or
        containerProperties (Container), clobbering what the entries stage wrote."""
        self._setup_processor_with_terms(processor, ctx)

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = _definition_link_response()
            workunits = list(processor.process_term_associations(max_workers=1))

        assert [
            wu.metadata.aspectName
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ] == ["glossaryTerms"]

    def test_lookup_called_once_at_term_location_with_project_number(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """The entry path must carry the project NUMBER and the term's own location in
        both halves, and the API must be called once per term rather than once per
        configured entries_location. Getting either wrong makes Dataplex reject the
        request at the edge with a 403."""
        self._setup_processor_with_terms(processor, ctx)
        ctx.config.entries_locations = ["us", "us-central1", "us-east5"]

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = _definition_link_response()
            list(processor.process_term_associations(max_workers=1))

        assert mock_session.get.call_count == 1
        expected_entry = (
            "projects/123456789/locations/global/entryGroups/@dataplex/entries/"
            "projects/123456789/locations/global/glossaries/g1/terms/t1"
        )
        assert mock_session.get.call_args[0][0] == (
            "https://dataplex.googleapis.com/v1/projects/my-project"
            "/locations/global:lookupEntryLinks"
            f"?entry={urllib.parse.quote(expected_entry, safe='')}"
        )

    def test_missing_project_number_warns_and_skips_term(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
        source_report: Mock,
    ) -> None:
        """An unresolved project number must not raise inside the worker thread."""
        self._setup_processor_with_terms(processor, ctx)
        ctx.project_numbers = {}

        with patch.object(ctx, "authed_session") as mock_session:
            workunits = list(processor.process_term_associations(max_workers=1))

        assert workunits == []
        assert mock_session.get.call_count == 0
        assert source_report.warning.call_args.kwargs["title"] == (
            "Missing GCP project number"
        )


# ---------------------------------------------------------------------------
# Processor: term reconciliation
# ---------------------------------------------------------------------------


def _glossary_term_urns(workunits: List[MetadataWorkUnit]) -> List[str]:
    """Extract the term URNs from the single glossaryTerms workunit (with narrowing)."""
    for wu in workunits:
        mcp = wu.metadata
        if isinstance(mcp, MetadataChangeProposalWrapper) and mcp.aspectName == (
            "glossaryTerms"
        ):
            aspect = mcp.aspect
            assert isinstance(aspect, GlossaryTermsClass)
            return [t.urn for t in aspect.terms]
    raise AssertionError("no glossaryTerms workunit emitted")


def _platform_resource_wus(
    workunits: List[MetadataWorkUnit],
) -> List[MetadataWorkUnit]:
    """The emitted managed_by_datahub=false platform-resource marker workunits."""
    return [wu for wu in workunits if wu.id.startswith("platform_resource-")]


def _make_processor(repo: Optional[Mock]) -> DataplexGlossaryProcessor:
    return DataplexGlossaryProcessor(
        ctx=MagicMock(),
        glossary_client=MagicMock(),
        report=DataplexGlossaryReport(),
        source_report=MagicMock(),
        platform_resource_repository=repo,
    )


class TestReconcileTerm:
    def test_reconcile_term_returns_original_when_managed(self) -> None:
        repo = MagicMock()
        native = "urn:li:glossaryTerm:dataplex.p.global.g.pii"
        repo.search_entity_by_urn.return_value = DataplexAspectId(
            aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
            entry_name="e",
            field_key="urn:li:glossaryTerm:pii",
        )
        repo.get_entity_from_datahub.return_value = DataplexAspectPlatformResource(
            datahub_urn="urn:li:glossaryTerm:pii",
            managed_by_datahub=True,
            aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
            entry_name="e",
            field_key="urn:li:glossaryTerm:pii",
        )
        proc = _make_processor(repo)
        assert proc._reconcile_term(native) == "urn:li:glossaryTerm:pii"

    def test_reconcile_term_returns_none_when_unmanaged_or_missing(self) -> None:
        repo = MagicMock()
        repo.search_entity_by_urn.return_value = None
        assert _make_processor(repo)._reconcile_term("urn:li:glossaryTerm:x") is None
        # No repository at all -> no reconciliation.
        assert _make_processor(None)._reconcile_term("urn:li:glossaryTerm:x") is None

    def test_reconcile_term_swallows_lookup_errors_and_warns(self) -> None:
        repo = MagicMock()
        repo.search_entity_by_urn.side_effect = Exception("boom")
        source_report = MagicMock()
        proc = DataplexGlossaryProcessor(
            ctx=MagicMock(),
            glossary_client=MagicMock(),
            report=DataplexGlossaryReport(),
            source_report=source_report,
            platform_resource_repository=repo,
        )
        result = proc._reconcile_term("urn:li:glossaryTerm:whatever")
        assert result is None
        assert source_report.warning.called

    def test_reconcile_term_returns_none_when_found_but_unmanaged(self) -> None:
        native = "urn:li:glossaryTerm:dataplex.p.global.g.native"
        repo = MagicMock()
        repo.search_entity_by_urn.return_value = DataplexAspectId(
            aspect_key=GLOSSARY_TERMS_ASPECT_KEY, entry_name="e", field_key=native
        )
        repo.get_entity_from_datahub.return_value = DataplexAspectPlatformResource(
            datahub_urn=native,
            managed_by_datahub=False,
            aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
            entry_name="e",
            field_key=native,
        )
        assert _make_processor(repo)._reconcile_term(native) is None


class TestRecordExternalLink:
    def test_records_external_link_when_not_managed(self) -> None:
        repo = MagicMock()
        proc = _make_processor(repo)
        wus = list(
            proc._record_external_link(
                entry_name="projects/p/locations/l/entryGroups/@bigquery/entries/e",
                native_term_urn="urn:li:glossaryTerm:dataplex.p.global.g.pii",
            )
        )
        assert len(wus) >= 1
        assert all(wu.id.startswith("platform_resource-") for wu in wus)
        # Emitted as workunits, not written directly to the graph.
        repo.create.assert_not_called()

    def test_no_external_write_without_repo(self) -> None:
        proc = _make_processor(None)
        # No repository -> no workunits, and must not raise.
        wus = list(
            proc._record_external_link(
                entry_name="e", native_term_urn="urn:li:glossaryTerm:x"
            )
        )
        assert wus == []


class TestNormalizeEntryProjectId:
    def test_maps_project_number_prefix_to_id(self) -> None:
        proc = _make_processor(None)
        proc._ctx.project_numbers = {"my-project": "123456789"}
        result = proc._normalize_entry_project_id(
            "projects/123456789/locations/us/entryGroups/@bigquery/entries/foo"
        )
        assert result == (
            "projects/my-project/locations/us/entryGroups/@bigquery/entries/foo"
        )

    def test_returns_unchanged_when_no_number_matches(self) -> None:
        proc = _make_processor(None)
        proc._ctx.project_numbers = {"my-project": "123456789"}
        entry = "projects/my-project/locations/us/entryGroups/@bigquery/entries/foo"
        assert proc._normalize_entry_project_id(entry) == entry
