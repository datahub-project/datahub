from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from requests.models import HTTPError

pytest.importorskip("unstructured")

from datahub.configuration.common import GraphError, OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.embeddings import ServerEmbeddingRunError
from datahub.ingestion.source.datahub_entity_embeddings.config import (
    DataHubEntityEmbeddingsSourceConfig,
)
from datahub.ingestion.source.datahub_entity_embeddings.source import (
    DataHubEntityEmbeddingsSource,
    entity_platform,
)
from datahub.ingestion.source.datahub_entity_embeddings.text_builder import (
    parse_registry,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    EmbeddingConfig,
    ServerEmbeddingConfig,
    ServerSemanticSearchConfig,
)
from datahub.metadata.schema_classes import SemanticContentClass
from tests.unit.datahub_entity_embeddings.registry_fixtures import REGISTRY

SOURCE_MODULE = "datahub.ingestion.source.datahub_entity_embeddings.source"
CHUNKING_MODULE = "datahub.ingestion.source.unstructured.chunking_source"
GEMINI = ServerEmbeddingConfig(
    provider="vertex_ai",
    model_id="gemini-embedding-001",
    model_embedding_key="gemini_embedding_001",
    vertex_project_id="test-project",
)


def dataset(
    name: str, description: str = "A dataset", platform: str = "bigquery"
) -> Dict[str, Any]:
    return {
        "urn": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)",
        "datasetProperties": {"value": {"name": name, "description": description}},
    }


def container(name: str, platform: str) -> Dict[str, Any]:
    return {
        "urn": f"urn:li:container:{name}",
        "containerProperties": {"value": {"name": name}},
        "dataPlatformInstance": {
            "value": {"platform": f"urn:li:dataPlatform:{platform}"}
        },
    }


def dashboard(dashboard_id: str, tool: str) -> Dict[str, Any]:
    return {
        "urn": f"urn:li:dashboard:({tool},{dashboard_id})",
        "dashboardInfo": {"value": {"title": dashboard_id}},
    }


def with_sibling(entity: Dict[str, Any], sibling_urn: str) -> Dict[str, Any]:
    entity["siblings"] = {"value": {"siblings": [sibling_urn]}}
    return entity


def domain(name: str) -> Dict[str, Any]:
    return {
        "urn": f"urn:li:domain:{name}",
        "domainProperties": {"value": {"name": name}},
    }


def workunit(urn: str) -> MetadataWorkUnit:
    """What the chunking source emits for an entity: a non-primary semanticContent."""
    return MetadataWorkUnit(
        id=f"{urn}-semanticContent",
        mcp=MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=SemanticContentClass(embeddings={})
        ),
        is_primary_source=False,
    )


def skip_marker(urn: str, reason: str) -> MetadataWorkUnit:
    return MetadataWorkUnit(
        id=f"{urn}-semanticContent-skip",
        mcp=MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SemanticContentClass(embeddings={}, skipReason=reason),
        ),
        is_primary_source=False,
    )


class Harness:
    """A source wired to fake GMS responses; embedding is replaced by a stub."""

    def __init__(
        self,
        pages: Dict[str, List[List[Dict[str, Any]]]],
        enabled: Optional[List[str]] = None,
        state: Optional[Dict[str, str]] = None,
        **config: Any,
    ):
        self.graph = MagicMock(spec=DataHubGraph)
        self.graph.get_entity_specifications.return_value = REGISTRY
        self.scroll_calls: List[Dict[str, Any]] = []
        self.pages = {t: list(p) for t, p in pages.items()}
        self.graph.scroll_entities_raw.side_effect = self._scroll
        self.graph.get_entities_raw.return_value = {}
        self.graph.get_entity_raw.return_value = {"aspects": {}}
        self.enabled = enabled if enabled is not None else ["document", "dataset"]
        self.state = state or {}
        self.recorded: Dict[str, str] = {}

        ctx = PipelineContext(run_id="test-run", pipeline_name="entity-embeddings")
        ctx.graph = self.graph
        config.setdefault("locking", {"enabled": False})
        config.setdefault("stateful_ingestion", {"enabled": False})
        with patch(f"{SOURCE_MODULE}.DocumentChunkingSource") as chunking_cls:
            self.chunking = chunking_cls.return_value
            self.chunking.get_model_embedding_key.return_value = (
                GEMINI.model_embedding_key
            )
            self.chunking.process_elements_inline.side_effect = (
                lambda document_urn, elements, **_: iter([workunit(document_urn)])
            )
            self.chunking.build_skip_marker_workunit.side_effect = skip_marker
            self.source = DataHubEntityEmbeddingsSource(
                ctx, DataHubEntityEmbeddingsSourceConfig.model_validate(config)
            )
        # Like the real chunking source: the config it was given, with the embedding
        # settings resolved from the server.
        self.chunking.config = chunking_cls.call_args.kwargs["config"]
        self.chunking.config.embedding = EmbeddingConfig.from_server(GEMINI)
        handler = MagicMock()
        handler.is_checkpointing_enabled.return_value = True
        handler.get_document_hash.side_effect = self.state.get
        handler.update_document_state.side_effect = lambda urn, content_hash, _ts: (
            self.recorded.__setitem__(urn, content_hash)
        )
        self.current_state = MagicMock()
        self.current_state.document_state = {
            u: {"content_hash": h} for u, h in self.state.items()
        }
        handler.get_current_state.return_value = self.current_state
        self.source.state_handler = handler

    def _scroll(
        self, entity_name: str, aspects: List[str], count: int
    ) -> Iterator[Dict[str, Any]]:
        self.scroll_calls.append({"entities": [entity_name], "aspects": aspects})
        for page in self.pages.get(entity_name, []):
            yield from page

    def run(self) -> List[str]:
        return [wu.get_urn() for wu in self.source_workunits()]

    def source_workunits(self) -> List[MetadataWorkUnit]:
        server = ServerSemanticSearchConfig(enabled=True, enabled_entities=self.enabled)
        with patch(f"{SOURCE_MODULE}.get_semantic_search_config", return_value=server):
            return list(self.source.get_workunits_internal())

    def workunits(self) -> List[MetadataWorkUnit]:
        """Workunits as the pipeline receives them, after the workunit processors."""
        server = ServerSemanticSearchConfig(enabled=True, enabled_entities=self.enabled)
        with patch(f"{SOURCE_MODULE}.get_semantic_search_config", return_value=server):
            return list(self.source.get_workunits())


@pytest.mark.parametrize(
    "urn, aspects, expected",
    [
        (
            "urn:li:container:c1",
            {"dataPlatformInstance": {"platform": "urn:li:dataPlatform:pubsub"}},
            "pubsub",
        ),
        ("urn:li:dataset:(urn:li:dataPlatform:kafka,t,PROD)", {}, "kafka"),
        (
            "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,table)",
            {"dataPlatformInstance": {"platform": "urn:li:dataPlatform:vertexai"}},
            "vertexai",
        ),
        ("urn:li:tag:pii", {}, None),
        ("urn:li:container:c2", {"dataPlatformInstance": {}}, None),
        ("urn:li:chart:(looker,123)", {}, "looker"),
        ("urn:li:dashboard:(urn:li:dataPlatform:tableau,d1)", {}, "tableau"),
        ("urn:li:dataFlow:(airflow,dag,prod)", {}, "airflow"),
        ("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,prod),task)", {}, "airflow"),
    ],
)
def test_entity_platform(
    urn: str, aspects: Dict[str, Any], expected: Optional[str]
) -> None:
    assert entity_platform(urn, aspects) == expected


class TestResolveEntityTypes:
    def resolve(
        self, enabled: List[str], **config: Any
    ) -> DataHubEntityEmbeddingsSource:
        harness = Harness({}, enabled=enabled, **config)
        harness.source._specs = parse_registry(REGISTRY)
        harness.source.report.entity_types_embedded = (
            harness.source.resolve_entity_types(enabled)
        )
        return harness.source

    def test_auto_uses_server_enabled_types(self):
        source = self.resolve(["document", "dataset", "tag"])
        assert source.report.entity_types_embedded == ["dataset"]
        skipped = source.report.entity_types_skipped
        assert "datahub-documents" in skipped["document"]
        assert "semanticContent" in skipped["tag"]

    def test_operational_search_groups_are_opt_in(self):
        source = self.resolve(["dataset", "dataProcessInstance"])
        assert source.report.entity_types_embedded == ["dataset"]
        assert (
            "search group" in source.report.entity_types_skipped["dataProcessInstance"]
        )

        source = self.resolve(
            ["dataset", "dataProcessInstance"], search_groups=["primary", "timeseries"]
        )
        assert source.report.entity_types_embedded == ["dataProcessInstance", "dataset"]

    def test_types_without_a_search_group_are_eligible(self):
        # The base registry assigns no search groups since search V3.
        assert parse_registry(REGISTRY)["dataset"].search_group is None
        source = self.resolve(
            ["dataset", "dataProcessInstance"], search_groups=["timeseries"]
        )
        assert source.report.entity_types_embedded == ["dataProcessInstance", "dataset"]

    def test_empty_entity_types_are_rejected(self):
        with pytest.raises(ValueError, match="entity_types"):
            DataHubEntityEmbeddingsSourceConfig.model_validate({"entity_types": []})

    def test_explicit_types_must_be_enabled_on_server(self):
        source = self.resolve(["dataset"], entity_types=["dataset", "chart", "unknown"])
        assert source.report.entity_types_embedded == ["dataset"]
        assert "not enabled" in source.report.entity_types_skipped["chart"]
        assert "registry" in source.report.entity_types_skipped["unknown"]

    def test_exclusions_and_case_insensitive_names(self):
        source = self.resolve(["DATASET"], exclude_entity_types=["Dataset"])
        assert source.report.entity_types_embedded == []
        assert "exclude" in source.report.entity_types_skipped["dataset"]


class TestRun:
    def test_embeds_new_entities_and_records_state(self):
        harness = Harness({"dataset": [[dataset("a"), dataset("b")], [dataset("c")]]})
        urns = harness.run()
        assert len(urns) == 3
        assert set(harness.recorded) == set(urns)
        assert harness.source.report.num_entities_embedded == {"dataset": 3}
        body = harness.scroll_calls[0]
        assert body["entities"] == ["dataset"]
        assert "datasetProperties" in body["aspects"]
        assert "siblings" in body["aspects"]
        assert "semanticContent" not in body["aspects"]

    def test_emits_only_semantic_content(self):
        # The source writes to entities that other sources own, so nothing else
        # may be emitted for them, e.g. a browsePathsV2 for root-looking containers.
        harness = Harness(
            {"container": [[container("warehouse", "bigquery")]]},
            enabled=["container"],
        )
        emitted = [
            wu.metadata.aspectName
            for wu in harness.workunits()
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]
        assert emitted == ["semanticContent"]

    def test_skips_unchanged_entities(self):
        first = Harness({"dataset": [[dataset("a"), dataset("b")]]})
        first.run()
        second = Harness(
            {"dataset": [[dataset("a"), dataset("b", "Changed description")]]},
            state=first.recorded,
        )
        urns = second.run()
        assert urns == [dataset("b")["urn"]]
        assert second.source.report.num_entities_unchanged == {"dataset": 1}

    def test_force_reprocess(self):
        first = Harness({"dataset": [[dataset("a")]]})
        first.run()
        second = Harness(
            {"dataset": [[dataset("a")]]},
            state=first.recorded,
            incremental={"force_reprocess": True},
        )
        assert len(second.run()) == 1

    def test_failed_forced_reembed_is_retried_next_run(self):
        first = Harness({"dataset": [[dataset("a")]]})
        first.run()
        urn = dataset("a")["urn"]
        second = Harness(
            {"dataset": [[dataset("a")]]},
            state=first.recorded,
            incremental={"force_reprocess": True},
        )
        second.chunking.process_elements_inline.side_effect = RuntimeError("down")
        assert second.run() == []
        assert urn not in second.current_state.document_state

    def test_prunes_state_of_entities_no_longer_present(self):
        gone = "urn:li:dataset:(urn:li:dataPlatform:bigquery,gone,PROD)"
        harness = Harness({"dataset": [[dataset("a")]]}, state={gone: "x"})
        harness.run()
        assert gone not in harness.current_state.document_state
        assert harness.source.report.num_state_entries_pruned == 1

    def test_malformed_scroll_response_fails_without_pruning(self):
        known = dataset("a")["urn"]
        harness = Harness({"dataset": [[dataset("a")]]}, state={known: "x"})
        harness.graph.scroll_entities_raw.side_effect = GraphError("no entities list")
        assert harness.run() == []
        assert harness.source.report.failures
        assert known in harness.current_state.document_state

    def test_entity_limit_stops_cleanly_without_pruning(self):
        gone = "urn:li:dataset:(urn:li:dataPlatform:bigquery,gone,PROD)"
        harness = Harness(
            {"dataset": [[dataset("a"), dataset("b"), dataset("c")]]},
            state={gone: "x"},
            max_entities_per_run=2,
        )
        assert len(harness.run()) == 2
        assert harness.source.report.entity_limit_reached
        assert not harness.source.report.failures
        assert gone in harness.current_state.document_state

    def test_time_budget_stops_cleanly(self):
        harness = Harness(
            {"dataset": [[dataset("a"), dataset("b")]]}, time_budget_seconds=60
        )
        harness.source._started_at -= 61
        assert harness.run() == []
        assert harness.source.report.time_budget_reached
        assert not harness.source.report.failures

    def test_failed_entities_are_retried_next_run(self):
        harness = Harness({"dataset": [[dataset("a"), dataset("b")]]})
        failing = dataset("a")["urn"]

        def embed(document_urn, elements, **_):
            if document_urn == failing:
                raise RuntimeError("provider error")
            return iter([workunit(document_urn)])

        harness.chunking.process_elements_inline.side_effect = embed
        urns = harness.run()
        assert urns == [dataset("b")["urn"]]
        assert failing not in harness.recorded
        assert harness.source.report.num_entities_failed == {"dataset": 1}
        assert not harness.source.report.failures

    def test_aborts_after_consecutive_failures(self):
        harness = Harness(
            {"dataset": [[dataset(str(i)) for i in range(5)]]},
            max_consecutive_failures=2,
        )
        harness.chunking.process_elements_inline.side_effect = RuntimeError("down")
        assert harness.run() == []
        assert harness.source.report.num_entities_failed == {"dataset": 2}
        assert harness.source.report.failures

    def test_abort_keeps_the_state_of_entities_already_embedded(self):
        # The checkpoint still commits after an abort, so these are not re-embedded.
        gone = "urn:li:dataset:(urn:li:dataPlatform:bigquery,gone,PROD)"
        harness = Harness(
            {"dataset": [[dataset("a"), dataset("b"), dataset("c")]]},
            state={gone: "x"},
            max_consecutive_failures=2,
        )
        embedded = dataset("a")["urn"]

        def embed(document_urn, elements, **_):
            if document_urn != embedded:
                raise RuntimeError("down")
            return iter([workunit(document_urn)])

        harness.chunking.process_elements_inline.side_effect = embed
        assert harness.run() == [embedded]
        assert harness.source.report.failures
        assert embedded in harness.recorded
        assert gone in harness.current_state.document_state

    def test_server_embedding_errors_abort_the_run(self):
        # The server refused the run, so no other entity can succeed either.
        gone = "urn:li:dataset:(urn:li:dataPlatform:bigquery,gone,PROD)"
        harness = Harness(
            {"dataset": [[dataset("a"), dataset("b"), dataset("c")]]},
            state={gone: "x"},
        )
        harness.chunking.process_elements_inline.side_effect = ServerEmbeddingRunError(
            "Semantic search embeddings are not offered"
        )
        assert harness.run() == []
        assert harness.chunking.process_elements_inline.call_count == 1
        report = harness.source.report
        assert report.num_entities_failed == {"dataset": 1}
        assert "Server embeddings unavailable" in str(report.failures)
        assert not report.warnings
        assert gone in harness.current_state.document_state

    def test_skip_markers_do_not_hide_a_failing_provider(self):
        # Skip markers never call the provider, so they don't reset the count.
        def empty(name: str) -> Dict[str, Any]:
            return {"urn": f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{name},PROD)"}

        harness = Harness(
            {"dataset": [[dataset("a"), empty("e1"), dataset("b"), empty("e2")]]},
            max_consecutive_failures=2,
        )
        harness.chunking.process_elements_inline.side_effect = RuntimeError("down")
        harness.run()
        assert harness.source.report.num_entities_failed == {"dataset": 2}
        assert harness.source.report.num_entities_empty == {"dataset": 1}
        assert harness.source.report.failures

    def test_passes_the_source_text_hash(self):
        harness = Harness({"dataset": [[dataset("a")]]})
        harness.run()
        kwargs = harness.chunking.process_elements_inline.call_args.kwargs
        assert len(kwargs["source_text_sha256"]) == 64

    def test_not_indexable_entities_are_recorded(self):
        # The chunking source returns a skip marker when the text yields no chunks;
        # that only changes with the text, so it isn't retried on every run.
        harness = Harness({"dataset": [[dataset("a"), dataset("b")]]})
        not_indexable = dataset("a")["urn"]
        harness.chunking.process_elements_inline.side_effect = (
            lambda document_urn, elements, **_: iter(
                [skip_marker(document_urn, "NO_INDEXABLE_CONTENT")]
                if document_urn == not_indexable
                else [workunit(document_urn)]
            )
        )
        harness.run()
        report = harness.source.report
        assert not_indexable in harness.recorded
        assert report.num_entities_not_indexable == {"dataset": 1}
        assert report.num_entities_embedded == {"dataset": 1}
        assert not report.num_entities_failed

    def test_empty_text_emits_a_skip_marker(self):
        # The marker drops this model's embeddings, so an entity whose text became
        # empty is no longer matched on its old text.
        empty = {"urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,empty,PROD)"}
        harness = Harness({"dataset": [[empty]]})
        aspects = [
            wu.get_aspect_of_type(SemanticContentClass)
            for wu in harness.source_workunits()
        ]
        assert [a.skipReason if a else None for a in aspects] == ["EMPTY_TEXT"]
        assert empty["urn"] in harness.recorded
        assert harness.source.report.num_entities_empty == {"dataset": 1}
        harness.chunking.process_elements_inline.assert_not_called()

    def test_entities_without_output_count_as_failures(self):
        harness = Harness(
            {"dataset": [[dataset(str(i)) for i in range(5)]]},
            max_consecutive_failures=2,
        )
        harness.chunking.process_elements_inline.side_effect = (
            lambda document_urn, elements, **_: iter([])
        )
        assert harness.run() == []
        assert harness.recorded == {}
        assert harness.source.report.num_entities_failed == {"dataset": 2}
        assert harness.source.report.failures

    def test_missing_registry_privilege_is_named(self):
        harness = Harness({"dataset": [[dataset("a")]]})
        error = OperationalError("Unable to get metadata from DataHub", {})
        error.__cause__ = HTTPError(response=MagicMock(status_code=403))
        harness.graph.get_entity_specifications.side_effect = error
        assert harness.run() == []
        assert "MANAGE_SYSTEM_OPERATIONS" in str(harness.source.report.failures)
        assert harness.scroll_calls == []

    def test_no_embedding_model_fails_fast(self):
        harness = Harness({"dataset": [[dataset("a")]]})
        harness.chunking.get_model_embedding_key.return_value = None
        assert harness.run() == []
        assert harness.source.report.failures
        assert harness.scroll_calls == []

    def test_platform_pattern_applies_to_every_type_with_a_platform(self):
        pubsub_dataset = dataset("events", platform="pubsub")
        pubsub_container = container("topics", "pubsub")
        harness = Harness(
            {
                "dataset": [[dataset("a"), pubsub_dataset]],
                "container": [[container("warehouse", "bigquery"), pubsub_container]],
                "domain": [[domain("sales")]],
            },
            enabled=["dataset", "container", "domain"],
            state={pubsub_dataset["urn"]: "x"},
            platform_pattern={"deny": ["pubsub"]},
        )
        urns = harness.run()
        assert set(urns) == {
            dataset("a")["urn"],
            container("warehouse", "bigquery")["urn"],
            domain("sales")["urn"],
        }
        report = harness.source.report
        assert report.num_entities_filtered == {"dataset": 1, "container": 1}
        assert report.num_entities_scanned == {
            "dataset": 2,
            "container": 2,
            "domain": 1,
        }
        # Filtered entities are forgotten, so allowing the platform later embeds them.
        assert pubsub_dataset["urn"] not in harness.current_state.document_state
        container_body = next(
            b for b in harness.scroll_calls if b["entities"] == ["container"]
        )
        assert "dataPlatformInstance" in container_body["aspects"]
        domain_body = next(
            b for b in harness.scroll_calls if b["entities"] == ["domain"]
        )
        assert "dataPlatformInstance" not in domain_body["aspects"]

    def test_platform_pattern_applies_to_platform_keyed_urns(self):
        # Dashboard URNs carry a bare platform name instead of a dataPlatform URN.
        harness = Harness(
            {"dashboard": [[dashboard("d1", "looker"), dashboard("d2", "tableau")]]},
            enabled=["dashboard"],
            platform_pattern={"deny": ["looker"]},
        )
        assert harness.run() == [dashboard("d2", "tableau")["urn"]]
        assert harness.source.report.num_entities_filtered == {"dashboard": 1}

    def test_platform_pattern_allow_list(self):
        harness = Harness(
            {"dataset": [[dataset("a"), dataset("b", platform="dbt")]]},
            platform_pattern={"allow": ["dbt"]},
        )
        assert harness.run() == [dataset("b", platform="dbt")["urn"]]

    def test_resolves_references_once(self):
        tagged = dataset("a")
        tagged["globalTags"] = {"value": {"tags": [{"tag": "urn:li:tag:t1"}]}}
        tagged["domains"] = {"value": {"domains": ["urn:li:domain:d1"]}}
        other = dataset("b")
        other["domains"] = {"value": {"domains": ["urn:li:domain:d1"]}}
        harness = Harness({"dataset": [[tagged, other]]})
        harness.graph.get_entity_raw.return_value = {
            "aspects": {"domainProperties": {"value": {"name": "Sales"}}}
        }
        harness.run()
        domain_calls = [
            c
            for c in harness.graph.get_entity_raw.call_args_list
            if c.args[0] == "urn:li:domain:d1"
        ]
        assert len(domain_calls) == 1
        elements_text = str(harness.chunking.process_elements_inline.call_args_list[0])
        assert "Sales" in elements_text

    def test_siblings_are_fetched_once_per_page(self):
        dbt_a = "urn:li:dataset:(urn:li:dataPlatform:dbt,a,PROD)"
        dbt_b = "urn:li:dataset:(urn:li:dataPlatform:dbt,b,PROD)"
        harness = Harness(
            {
                "dataset": [
                    [
                        with_sibling(dataset("a"), dbt_a),
                        with_sibling(dataset("b"), dbt_b),
                        dataset("c"),
                    ]
                ]
            }
        )
        harness.graph.get_entities_raw.return_value = {
            dbt_a: {
                "urn": dbt_a,
                "datasetProperties": {"value": {"description": "dbt model a"}},
                "siblings": {"value": {"siblings": [dataset("a")["urn"]]}},
            },
        }
        assert len(harness.run()) == 3
        harness.graph.get_entities_raw.assert_called_once()
        entity_type, urns, aspects = harness.graph.get_entities_raw.call_args.args
        assert (entity_type, urns) == ("dataset", [dbt_a, dbt_b])
        assert "datasetProperties" in aspects
        first_text = str(harness.chunking.process_elements_inline.call_args_list[0])
        assert "dbt model a" in first_text

    def test_entities_with_unavailable_siblings_are_retried_next_run(self):
        # Embedding them without the sibling's text would replace their embeddings
        # with ones built from less text.
        sibling = "urn:li:dataset:(urn:li:dataPlatform:dbt,a,PROD)"
        with_dbt = with_sibling(dataset("a"), sibling)
        harness = Harness(
            {"dataset": [[with_dbt, dataset("b")]]}, state={with_dbt["urn"]: "old"}
        )
        harness.graph.get_entities_raw.side_effect = HTTPError("timeout")
        assert harness.run() == [dataset("b")["urn"]]
        report = harness.source.report
        assert report.num_entities_failed == {"dataset": 1}
        assert "Siblings could not be fetched" in str(report.warnings)
        assert not report.failures
        assert harness.current_state.document_state[with_dbt["urn"]] == {
            "content_hash": "old"
        }

    def test_siblings_are_not_fetched_when_disabled(self):
        sibling = "urn:li:dataset:(urn:li:dataPlatform:dbt,a,PROD)"
        harness = Harness(
            {"dataset": [[with_sibling(dataset("a"), sibling)]]},
            text={"include_siblings": False},
        )
        assert len(harness.run()) == 1
        harness.graph.get_entities_raw.assert_not_called()
        assert "siblings" not in harness.scroll_calls[0]["aspects"]


def test_content_hash_follows_the_server_embedding_model():
    # Vectors from different models aren't comparable, so a model change on the
    # server must re-embed every entity.
    def content_hash(embedding: ServerEmbeddingConfig) -> str:
        ctx = PipelineContext(run_id="test-run", pipeline_name="entity-embeddings")
        ctx.graph = MagicMock(spec=DataHubGraph)
        config = DataHubEntityEmbeddingsSourceConfig.model_validate(
            {"locking": {"enabled": False}, "stateful_ingestion": {"enabled": False}}
        )
        server = ServerSemanticSearchConfig(
            enabled=True, enabled_entities=["dataset"], embedding_config=embedding
        )
        with patch(
            f"{CHUNKING_MODULE}.get_semantic_search_config", return_value=server
        ):
            source = DataHubEntityEmbeddingsSource(ctx, config)
        return source._content_hash("# Dataset: orders\n")

    other_model = GEMINI.model_copy(
        update={
            "model_id": "text-embedding-005",
            "model_embedding_key": "text_embedding_005",
        }
    )
    assert content_hash(GEMINI) != content_hash(other_model)
