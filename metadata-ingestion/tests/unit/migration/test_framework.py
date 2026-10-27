"""Unit tests for the migration framework stages (fetch, transform, discovery)."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datahub.cli.migrate import _load_mapping_pairs
from datahub.cli.migration_utils import get_incoming_relationships
from datahub.ingestion.graph.openapi import Relationship, RelationshipScrollResult
from datahub.metadata.schema_classes import DataPlatformInstanceClass
from datahub.migration.fetch import fetch_instance_urns, fetch_platform_urns
from datahub.migration.transform import make_urn_builder, pairs_from_transform

DS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,{name},PROD)"
DS_A = DS.format(name="db.a")
DS_B = DS.format(name="db.b")


# --- Stage 1: fetch ---


class TestFetch:
    def test_platform_fetch_skips_entities_that_already_have_instance(self) -> None:
        """dataplatform2instance only migrates entities lacking an instance, so a
        source that already has a dataPlatformInstance.instance is filtered out."""
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [DS_A, DS_B]

        def get_aspect(entity_urn: str, aspect_type: object) -> object:
            if entity_urn == DS_A:
                return DataPlatformInstanceClass(
                    platform="urn:li:dataPlatform:snowflake",
                    instance="urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,inst)",
                )
            return None

        graph.get_aspect.side_effect = get_aspect

        result = list(
            fetch_platform_urns(
                graph, platform="snowflake", env="PROD", entity_type="dataset"
            )
        )
        # DS_A already has an instance → skipped; DS_B kept.
        assert result == [DS_B]

    def test_platform_fetch_without_skip_returns_all(self) -> None:
        """With skip_if_has_instance=False, every matched URN is returned and no
        per-entity instance lookup is performed."""
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [DS_A, DS_B]

        result = list(
            fetch_platform_urns(
                graph,
                platform="snowflake",
                env="PROD",
                entity_type="dataset",
                skip_if_has_instance=False,
            )
        )
        assert result == [DS_A, DS_B]
        graph.get_aspect.assert_not_called()

    def test_instance_fetch_filters_by_platform_instance(self) -> None:
        """instance2instance scopes the search to the old platform instance."""
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [DS_A]

        result = list(
            fetch_instance_urns(
                graph,
                platform="snowflake",
                old_instance="old",
                env="PROD",
                entity_type="dataset",
            )
        )
        assert result == [DS_A]
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["platform_instance"] == "old"

    def test_chart_fetch_does_not_pass_env(self) -> None:
        """Charts have no env/origin field, so env must not be forwarded to the
        filter for non-dataset entity types."""
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = []
        list(
            fetch_instance_urns(
                graph,
                platform="looker",
                old_instance="old",
                env="PROD",
                entity_type="chart",
            )
        )
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["env"] is None


# --- Stage 2: transform ---


class TestMakeUrnBuilder:
    @pytest.mark.parametrize(
        "entity_type,src,expected",
        [
            (
                "dataset",
                DS.format(name="db.t"),
                DS.format(name="inst.db.t"),
            ),
            ("chart", "urn:li:chart:(looker,c1)", "urn:li:chart:(looker,inst.c1)"),
            (
                "dashboard",
                "urn:li:dashboard:(looker,d1)",
                "urn:li:dashboard:(looker,inst.d1)",
            ),
            (
                "dataFlow",
                "urn:li:dataFlow:(airflow,f1,PROD)",
                "urn:li:dataFlow:(airflow,inst.f1,PROD)",
            ),
            (
                "dataJob",
                "urn:li:dataJob:(urn:li:dataFlow:(airflow,f1,PROD),j1)",
                "urn:li:dataJob:(urn:li:dataFlow:(airflow,inst.f1,PROD),j1)",
            ),
        ],
    )
    def test_platform2instance_prepends_instance(
        self, entity_type: str, src: str, expected: str
    ) -> None:
        """Platform-to-instance transforms prepend the new instance to the entity
        name for every supported entity type (dataJob prefixes its parent flow)."""
        assert make_urn_builder(entity_type, new_instance="inst")(src) == expected

    @pytest.mark.parametrize(
        "entity_type,src,expected",
        [
            (
                "dataset",
                DS.format(name="old.db.t"),
                DS.format(name="new.db.t"),
            ),
            (
                "chart",
                "urn:li:chart:(looker,old.c1)",
                "urn:li:chart:(looker,new.c1)",
            ),
            (
                "dataJob",
                "urn:li:dataJob:(urn:li:dataFlow:(airflow,old.f1,PROD),j1)",
                "urn:li:dataJob:(urn:li:dataFlow:(airflow,new.f1,PROD),j1)",
            ),
        ],
    )
    def test_instance2instance_replaces_prefix(
        self, entity_type: str, src: str, expected: str
    ) -> None:
        """Instance-to-instance transforms replace the old instance prefix with the
        new one."""
        builder = make_urn_builder(entity_type, new_instance="new", old_instance="old")
        assert builder(src) == expected

    def test_unsupported_entity_type_raises(self) -> None:
        """An entity type with no URN-construction spec is rejected."""
        with pytest.raises(ValueError, match="Unsupported entity type"):
            make_urn_builder("mlModel", new_instance="inst")


class TestPairsFromTransform:
    def test_builds_pairs_with_target_and_instance(self) -> None:
        """pairs_from_transform applies the transform to each URN and attaches the
        supplied target instance aspect to every pair."""
        transform = make_urn_builder("dataset", new_instance="inst")
        instance = DataPlatformInstanceClass(
            platform="urn:li:dataPlatform:snowflake",
            instance="urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,inst)",
        )
        pairs = list(
            pairs_from_transform([DS_A], transform, data_platform_instance=instance)
        )
        assert len(pairs) == 1
        assert pairs[0].source_urn == DS_A
        assert pairs[0].target_urn == DS.format(name="inst.db.a")
        assert pairs[0].data_platform_instance is instance

    def test_no_instance_by_default(self) -> None:
        """Without an explicit instance aspect, pairs carry none."""
        transform = make_urn_builder("dataset", new_instance="inst")
        pairs = list(pairs_from_transform([DS_A], transform))
        assert pairs[0].data_platform_instance is None


# --- Stage 3 discovery: type-less incoming-reference scroll ---


class TestIncomingRelationshipDiscovery:
    def test_queries_all_types_and_reads_source_urn_with_pagination(self) -> None:
        """Incoming references are discovered across ALL relationship types (so
        assertions are included), read from each edge's source, paged via
        scroll_id, and de-duplicated across pages."""
        graph = MagicMock()
        page1 = RelationshipScrollResult(
            scroll_id="cursor",
            relationships=[
                Relationship(
                    relationship_type="DownstreamOf",
                    source_urn=DS_B,
                    source_entity_type="dataset",
                    destination_urn=DS_A,
                    destination_entity_type="dataset",
                )
            ],
        )
        page2 = RelationshipScrollResult(
            scroll_id=None,
            relationships=[
                Relationship(
                    relationship_type="Asserts",
                    source_urn="urn:li:assertion:x",
                    source_entity_type="assertion",
                    destination_urn=DS_A,
                    destination_entity_type="dataset",
                ),
                # Duplicate source across pages must be de-duplicated.
                Relationship(
                    relationship_type="Other",
                    source_urn=DS_B,
                    source_entity_type="dataset",
                    destination_urn=DS_A,
                    destination_entity_type="dataset",
                ),
            ],
        )
        graph.scroll_relationships.side_effect = [page1, page2]

        referrers = [r.urn for r in get_incoming_relationships(DS_A, graph=graph)]

        # The assertion referrer is included — the whole point of querying all
        # relationship types rather than a hardcoded subset.
        assert referrers == [DS_B, "urn:li:assertion:x"]

        first_call = graph.scroll_relationships.call_args_list[0]
        assert first_call.kwargs["relationship_types"] is None
        assert first_call.kwargs["destination_urns"] == [DS_A]


# --- urns-mapping file loader ---


class TestUrnsMappingLoader:
    def test_loads_list_form(self, tmp_path: Path) -> None:
        """A list of {source, target} objects loads into pairs with no instance
        aspect (urns-mapping never stamps one)."""
        path = tmp_path / "m.json"
        path.write_text(
            json.dumps([{"source": DS_A, "target": DS_B}]),
        )
        pairs = _load_mapping_pairs(str(path))
        assert len(pairs) == 1
        assert pairs[0].source_urn == DS_A
        assert pairs[0].target_urn == DS_B
        assert pairs[0].data_platform_instance is None

    def test_loads_single_source_target_object(self, tmp_path: Path) -> None:
        """A single {"source": ..., "target": ...} object (not wrapped in a list)
        is treated as one pair, not as a flat source→target mapping."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps({"source": DS_A, "target": DS_B}))
        pairs = _load_mapping_pairs(str(path))
        assert len(pairs) == 1
        assert (pairs[0].source_urn, pairs[0].target_urn) == (DS_A, DS_B)

    def test_loads_single_source_urn_target_urn_object(self, tmp_path: Path) -> None:
        """Accepts source_urn/target_urn as aliases for source/target."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps({"source_urn": DS_A, "target_urn": DS_B}))
        pairs = _load_mapping_pairs(str(path))
        assert len(pairs) == 1
        assert (pairs[0].source_urn, pairs[0].target_urn) == (DS_A, DS_B)

    def test_loads_jsonl_form(self, tmp_path: Path) -> None:
        """JSONL: one {"source": ..., "target": ...} per line, no wrapping array."""
        path = tmp_path / "m.jsonl"
        path.write_text(
            json.dumps({"source": DS_A, "target": DS_B})
            + "\n"
            + json.dumps({"source": DS_B, "target": DS_A})
            + "\n"
        )
        pairs = _load_mapping_pairs(str(path))
        assert len(pairs) == 2
        assert (pairs[0].source_urn, pairs[0].target_urn) == (DS_A, DS_B)
        assert (pairs[1].source_urn, pairs[1].target_urn) == (DS_B, DS_A)

    def test_loads_jsonl_with_source_urn_keys(self, tmp_path: Path) -> None:
        """JSONL accepts source_urn/target_urn as aliases."""
        path = tmp_path / "m.jsonl"
        path.write_text(json.dumps({"source_urn": DS_A, "target_urn": DS_B}) + "\n")
        pairs = _load_mapping_pairs(str(path))
        assert len(pairs) == 1
        assert (pairs[0].source_urn, pairs[0].target_urn) == (DS_A, DS_B)

    def test_loads_object_form(self, tmp_path: Path) -> None:
        """A flat {source: target} object loads into the same pairs."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: DS_B}))
        pairs = _load_mapping_pairs(str(path))
        assert (pairs[0].source_urn, pairs[0].target_urn) == (DS_A, DS_B)

    def test_rejects_cross_entity_type_pair(self, tmp_path: Path) -> None:
        """A pair whose source and target are different entity types is rejected."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: "urn:li:chart:(looker,c)"}))
        with pytest.raises(Exception, match="same entity type"):
            _load_mapping_pairs(str(path))

    def test_rejects_malformed_urn(self, tmp_path: Path) -> None:
        """A value that isn't a URN is rejected."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: "not-a-urn"}))
        with pytest.raises(Exception, match="Invalid"):
            _load_mapping_pairs(str(path))

    def test_rejects_identity_pair(self, tmp_path: Path) -> None:
        """An identity mapping (source == target) is rejected — it would delete the
        source via a no-op self-migration."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: DS_A}))
        with pytest.raises(Exception, match="must differ"):
            _load_mapping_pairs(str(path))

    def test_rejects_duplicate_source(self, tmp_path: Path) -> None:
        """A source URN listed twice is rejected — migrating (and deleting) the same
        source more than once would leave later targets empty."""
        path = tmp_path / "m.json"
        path.write_text(
            json.dumps(
                [
                    {"source": DS_A, "target": DS_B},
                    {"source": DS_A, "target": DS.format(name="db.c")},
                ]
            )
        )
        with pytest.raises(Exception, match="Duplicate source"):
            _load_mapping_pairs(str(path))

    def test_rejects_duplicate_target(self, tmp_path: Path) -> None:
        """Two sources mapping to the same target is rejected — collapsing entities
        would drop all but one source's metadata."""
        path = tmp_path / "m.json"
        path.write_text(
            json.dumps(
                [
                    {"source": DS_A, "target": DS.format(name="db.c")},
                    {"source": DS_B, "target": DS.format(name="db.c")},
                ]
            )
        )
        with pytest.raises(Exception, match="Duplicate target"):
            _load_mapping_pairs(str(path))

    def test_rejects_empty_mapping(self, tmp_path: Path) -> None:
        """An empty mapping file is rejected rather than silently doing nothing."""
        path = tmp_path / "m.json"
        path.write_text(json.dumps([]))
        with pytest.raises(Exception, match="empty"):
            _load_mapping_pairs(str(path))
