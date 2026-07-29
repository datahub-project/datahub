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

    def test_instance_fetch_filters_by_platform_instance(self) -> None:
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
        # Charts have no env field; env must not be forwarded to the filter.
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


class TestPairsFromTransform:
    def test_builds_pairs_with_target_and_instance(self) -> None:
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
        transform = make_urn_builder("dataset", new_instance="inst")
        pairs = list(pairs_from_transform([DS_A], transform))
        assert pairs[0].data_platform_instance is None


# --- Stage 3 discovery: type-less incoming-reference scroll ---


class TestIncomingRelationshipDiscovery:
    def test_queries_all_types_and_reads_source_urn_with_pagination(self) -> None:
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
        path = tmp_path / "m.json"
        path.write_text(
            json.dumps([{"source": DS_A, "target": DS_B}]),
        )
        pairs = _load_mapping_pairs(str(path))
        assert len(pairs) == 1
        assert pairs[0].source_urn == DS_A
        assert pairs[0].target_urn == DS_B
        # urns-mapping does not stamp an instance aspect.
        assert pairs[0].data_platform_instance is None

    def test_loads_object_form(self, tmp_path: Path) -> None:
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: DS_B}))
        pairs = _load_mapping_pairs(str(path))
        assert (pairs[0].source_urn, pairs[0].target_urn) == (DS_A, DS_B)

    def test_rejects_cross_entity_type_pair(self, tmp_path: Path) -> None:
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: "urn:li:chart:(looker,c)"}))
        with pytest.raises(Exception, match="same entity type"):
            _load_mapping_pairs(str(path))

    def test_rejects_malformed_urn(self, tmp_path: Path) -> None:
        path = tmp_path / "m.json"
        path.write_text(json.dumps({DS_A: "not-a-urn"}))
        with pytest.raises(Exception, match="Invalid"):
            _load_mapping_pairs(str(path))

    def test_rejects_empty_mapping(self, tmp_path: Path) -> None:
        path = tmp_path / "m.json"
        path.write_text(json.dumps([]))
        with pytest.raises(Exception, match="empty"):
            _load_mapping_pairs(str(path))
