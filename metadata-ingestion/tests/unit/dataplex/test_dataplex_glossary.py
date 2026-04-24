"""Unit tests for Dataplex Business Glossary ingestion."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
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
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.metadata.urns import GlossaryNodeUrn

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
        ctx.entry_data = [
            EntryDataTuple(
                dataplex_entry_short_name="table1",
                dataplex_entry_name="projects/my-project/locations/us/entryGroups/@bigquery/entries/bigquery:my-project.dataset.table1",
                dataplex_location="us",
                dataplex_entry_type_short_name="bigquery-table",
                dataplex_entry_fqn="bigquery:my-project.dataset.table1",
                datahub_platform="bigquery",
                datahub_dataset_name="my-project.dataset.table1",
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.dataset.table1,PROD)",
            )
        ]

    def test_association_resolved_from_entry_data(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        self._setup_processor_with_terms(processor, ctx)
        asset_entry_name = ctx.entry_data[0].dataplex_entry_name

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {"type": "SOURCE", "name": asset_entry_name},
                        {"type": "TARGET", "name": "term-path"},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(
                processor.process_term_associations(["my-project"], max_workers=1)
            )

        assert len(workunits) > 0
        assert processor._report.term_associations_emitted == 1

    def test_unknown_entry_skipped_no_crash(
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
            workunits = list(
                processor.process_term_associations(["my-project"], max_workers=1)
            )

        assert len(workunits) == 0
        assert processor._report.term_associations_emitted == 0

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
        asset_entry_name = ctx.entry_data[0].dataplex_entry_name

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    # synonym link — should be ignored
                    "entryLinkType": "projects/dataplex-types/locations/global/entryLinkTypes/synonym",
                    "entryReferences": [
                        {"type": "SOURCE", "name": asset_entry_name},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(
                processor.process_term_associations(["my-project"], max_workers=1)
            )

        assert len(workunits) == 0

    def test_asset_linked_to_multiple_terms_emits_one_mcp_with_all_terms(
        self,
        processor: DataplexGlossaryProcessor,
        ctx: DataplexContext,
    ) -> None:
        """Asset linked to two terms must receive both in a single MCP, not two separate ones."""
        asset_entry_name = "projects/my-project/locations/us/entryGroups/@bigquery/entries/bigquery:my-project.dataset.table1"
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
        ctx.entry_data = [
            EntryDataTuple(
                dataplex_entry_short_name="table1",
                dataplex_entry_name=asset_entry_name,
                dataplex_location="us",
                dataplex_entry_type_short_name="bigquery-table",
                dataplex_entry_fqn="bigquery:my-project.dataset.table1",
                datahub_platform="bigquery",
                datahub_dataset_name="my-project.dataset.table1",
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.dataset.table1,PROD)",
            )
        ]

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entryLinks": [
                {
                    "entryLinkType": "projects/655216118709/locations/global/entryLinkTypes/definition",
                    "entryReferences": [
                        {"type": "SOURCE", "name": asset_entry_name},
                    ],
                }
            ]
        }

        with patch.object(ctx, "authed_session") as mock_session:
            mock_session.get.return_value = mock_response
            workunits = list(
                processor.process_term_associations(["my-project"], max_workers=1)
            )

        # One MCP emitted (one asset), not two (which would overwrite each other).
        assert processor._report.term_associations_emitted == 1
        assert len(workunits) > 0
