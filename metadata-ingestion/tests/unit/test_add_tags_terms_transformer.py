# ABOUTME: Tests for AddTags and AddTerms transformers that work on datasets, charts, and dashboards.
# ABOUTME: Validates tag and term addition across multiple entity types.

from typing import Any
from unittest import mock

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.add_tags import (
    PatternAddTags,
    SimpleAddTags,
)
from datahub.ingestion.transformer.add_terms import (
    PatternAddTerms,
    SimpleAddTerms,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    GlossaryTermsClass,
)


@pytest.fixture
def mock_time():
    with mock.patch("time.time") as mock_time:
        mock_time.return_value = 1625266033.123
        yield mock_time


def make_chart_mcp(
    entity_urn: str = "urn:li:chart:(looker,dashboard_elements.1)",
    aspect: Any = None,
) -> MetadataChangeProposalWrapper:
    if aspect is None:
        aspect = models.StatusClass(removed=False)
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=aspect,
    )


def make_dashboard_mcp(
    entity_urn: str = "urn:li:dashboard:(looker,dashboards.1)",
    aspect: Any = None,
) -> MetadataChangeProposalWrapper:
    if aspect is None:
        aspect = models.StatusClass(removed=False)
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=aspect,
    )


def make_dataset_mcp(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
    aspect: Any = None,
) -> MetadataChangeProposalWrapper:
    if aspect is None:
        aspect = models.StatusClass(removed=False)
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=aspect,
    )


class TestSimpleAddTags:
    def test_add_tags_to_chart(self, mock_time: Any) -> None:
        chart_mcp = make_chart_mcp()

        transformer = SimpleAddTags.create(
            {
                "tag_urns": [
                    "urn:li:tag:test-tag1",
                    "urn:li:tag:test-tag2",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(chart_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: original MCP, tags MCP for chart, tag key MCPs (2), EndOfStream
        assert len(outputs) == 5

        # Check the tags MCP was generated for the chart
        tags_mcp = outputs[1].record
        assert isinstance(tags_mcp, MetadataChangeProposalWrapper)
        assert tags_mcp.entityUrn == chart_mcp.entityUrn
        assert isinstance(tags_mcp.aspect, GlobalTagsClass)
        assert len(tags_mcp.aspect.tags) == 2
        tag_urns = {tag.tag for tag in tags_mcp.aspect.tags}
        assert tag_urns == {"urn:li:tag:test-tag1", "urn:li:tag:test-tag2"}

    def test_add_tags_to_dashboard(self, mock_time: Any) -> None:
        dashboard_mcp = make_dashboard_mcp()

        transformer = SimpleAddTags.create(
            {
                "tag_urns": [
                    "urn:li:tag:dashboard-tag",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(dashboard_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: original MCP, tags MCP for dashboard, tag key MCP, EndOfStream
        assert len(outputs) == 4

        tags_mcp = outputs[1].record
        assert isinstance(tags_mcp, MetadataChangeProposalWrapper)
        assert tags_mcp.entityUrn == dashboard_mcp.entityUrn
        assert isinstance(tags_mcp.aspect, GlobalTagsClass)
        assert len(tags_mcp.aspect.tags) == 1
        assert tags_mcp.aspect.tags[0].tag == "urn:li:tag:dashboard-tag"

    def test_add_tags_to_dataset(self, mock_time: Any) -> None:
        dataset_mcp = make_dataset_mcp()

        transformer = SimpleAddTags.create(
            {
                "tag_urns": [
                    "urn:li:tag:dataset-tag",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(dataset_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: original MCP, tags MCP for dataset, tag key MCP, EndOfStream
        assert len(outputs) == 4

        tags_mcp = outputs[1].record
        assert isinstance(tags_mcp, MetadataChangeProposalWrapper)
        assert tags_mcp.entityUrn == dataset_mcp.entityUrn
        assert isinstance(tags_mcp.aspect, GlobalTagsClass)
        assert len(tags_mcp.aspect.tags) == 1
        assert tags_mcp.aspect.tags[0].tag == "urn:li:tag:dataset-tag"

    def test_add_tags_to_multiple_entity_types(self, mock_time: Any) -> None:
        chart_mcp = make_chart_mcp()
        dashboard_mcp = make_dashboard_mcp()
        dataset_mcp = make_dataset_mcp()

        transformer = SimpleAddTags.create(
            {
                "tag_urns": [
                    "urn:li:tag:shared-tag",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(chart_mcp, metadata={}),
                    RecordEnvelope(dashboard_mcp, metadata={}),
                    RecordEnvelope(dataset_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: 3 original MCPs + 3 tags MCPs + 1 tag key MCP + EndOfStream = 8
        assert len(outputs) == 8

        # Check each entity got tags
        tag_mcps = [
            o.record
            for o in outputs
            if isinstance(o.record, MetadataChangeProposalWrapper)
            and isinstance(o.record.aspect, GlobalTagsClass)
        ]
        assert len(tag_mcps) == 3

        tagged_urns = {mcp.entityUrn for mcp in tag_mcps}
        assert tagged_urns == {
            chart_mcp.entityUrn,
            dashboard_mcp.entityUrn,
            dataset_mcp.entityUrn,
        }


class TestPatternAddTags:
    def test_pattern_add_tags_to_chart(self, mock_time: Any) -> None:
        chart_mcp = make_chart_mcp(entity_urn="urn:li:chart:(looker,sales_chart)")

        transformer = PatternAddTags.create(
            {
                "tag_pattern": {
                    "rules": {
                        ".*sales.*": ["urn:li:tag:sales"],
                    }
                }
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(chart_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Find the tags MCP
        tags_mcps = [
            o.record
            for o in outputs
            if isinstance(o.record, MetadataChangeProposalWrapper)
            and isinstance(o.record.aspect, GlobalTagsClass)
        ]
        assert len(tags_mcps) == 1
        assert tags_mcps[0].aspect.tags[0].tag == "urn:li:tag:sales"


class TestSimpleAddTerms:
    def test_add_terms_to_chart(self, mock_time: Any) -> None:
        chart_mcp = make_chart_mcp()

        transformer = SimpleAddTerms.create(
            {
                "term_urns": [
                    "urn:li:glossaryTerm:Revenue",
                    "urn:li:glossaryTerm:Finance",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(chart_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: original MCP, terms MCP for chart, EndOfStream
        assert len(outputs) == 3

        terms_mcp = outputs[1].record
        assert isinstance(terms_mcp, MetadataChangeProposalWrapper)
        assert terms_mcp.entityUrn == chart_mcp.entityUrn
        assert isinstance(terms_mcp.aspect, GlossaryTermsClass)
        assert len(terms_mcp.aspect.terms) == 2
        term_urns = {term.urn for term in terms_mcp.aspect.terms}
        assert term_urns == {
            "urn:li:glossaryTerm:Revenue",
            "urn:li:glossaryTerm:Finance",
        }

    def test_add_terms_to_dashboard(self, mock_time: Any) -> None:
        dashboard_mcp = make_dashboard_mcp()

        transformer = SimpleAddTerms.create(
            {
                "term_urns": [
                    "urn:li:glossaryTerm:KPI",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(dashboard_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: original MCP, terms MCP for dashboard, EndOfStream
        assert len(outputs) == 3

        terms_mcp = outputs[1].record
        assert isinstance(terms_mcp, MetadataChangeProposalWrapper)
        assert terms_mcp.entityUrn == dashboard_mcp.entityUrn
        assert isinstance(terms_mcp.aspect, GlossaryTermsClass)
        assert len(terms_mcp.aspect.terms) == 1
        assert terms_mcp.aspect.terms[0].urn == "urn:li:glossaryTerm:KPI"

    def test_add_terms_to_dataset(self, mock_time: Any) -> None:
        dataset_mcp = make_dataset_mcp()

        transformer = SimpleAddTerms.create(
            {
                "term_urns": [
                    "urn:li:glossaryTerm:PII",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(dataset_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Expect: original MCP, terms MCP for dataset, EndOfStream
        assert len(outputs) == 3

        terms_mcp = outputs[1].record
        assert isinstance(terms_mcp, MetadataChangeProposalWrapper)
        assert terms_mcp.entityUrn == dataset_mcp.entityUrn
        assert isinstance(terms_mcp.aspect, GlossaryTermsClass)
        assert len(terms_mcp.aspect.terms) == 1
        assert terms_mcp.aspect.terms[0].urn == "urn:li:glossaryTerm:PII"


class TestPatternAddTerms:
    def test_pattern_add_terms_to_dashboard(self, mock_time: Any) -> None:
        dashboard_mcp = make_dashboard_mcp(
            entity_urn="urn:li:dashboard:(looker,finance_dashboard)"
        )

        transformer = PatternAddTerms.create(
            {
                "term_pattern": {
                    "rules": {
                        ".*finance.*": ["urn:li:glossaryTerm:Finance"],
                    }
                }
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(dashboard_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Find the terms MCP
        terms_mcps = [
            o.record
            for o in outputs
            if isinstance(o.record, MetadataChangeProposalWrapper)
            and isinstance(o.record.aspect, GlossaryTermsClass)
        ]
        assert len(terms_mcps) == 1
        assert terms_mcps[0].aspect.terms[0].urn == "urn:li:glossaryTerm:Finance"


class TestAddTagsIgnoresUnsupportedEntities:
    def test_ignores_datajob(self, mock_time: Any) -> None:
        datajob_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task)",
            aspect=models.StatusClass(removed=False),
        )

        transformer = SimpleAddTags.create(
            {
                "tag_urns": [
                    "urn:li:tag:test-tag",
                ]
            },
            PipelineContext(run_id="test"),
        )

        outputs = list(
            transformer.transform(
                [
                    RecordEnvelope(datajob_mcp, metadata={}),
                    RecordEnvelope(EndOfStream(), metadata={}),
                ]
            )
        )

        # Should only have original MCP and EndOfStream, no tags added
        assert len(outputs) == 2
        assert outputs[0].record == datajob_mcp
        assert isinstance(outputs[1].record, EndOfStream)
