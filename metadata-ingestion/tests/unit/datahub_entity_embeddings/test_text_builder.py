import copy
from typing import Any, Dict, List, Optional

import pytest

from datahub.ingestion.source.datahub_entity_embeddings.config import EntityTextConfig
from datahub.ingestion.source.datahub_entity_embeddings.text_builder import (
    EntityTextBuilder,
    clean_field_path,
    extract_values,
    humanize,
    parse_registry,
)
from tests.unit.datahub_entity_embeddings.registry_fixtures import REGISTRY

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.sales.orders,PROD)"
NAMES = {
    "urn:li:domain:3f2a": "Commerce",
    "urn:li:dataPlatform:bigquery": "bigquery",
}


def resolve(urn: str) -> Optional[str]:
    return NAMES.get(urn)


@pytest.fixture
def specs():
    return parse_registry(REGISTRY)


def reversed_registry() -> List[Dict[str, Any]]:
    """REGISTRY with aspects and fields reversed; GMS serves them in no set order."""
    registry = copy.deepcopy(REGISTRY)
    for element in registry:
        element["aspectSpecs"].reverse()
        for aspect_spec in [element["keyAspectSpec"], *element["aspectSpecs"]]:
            fields = aspect_spec["searchableFieldSpec"]
            aspect_spec["searchableFieldSpec"] = dict(reversed(list(fields.items())))
    return registry


def dataset_aspects() -> Dict[str, Any]:
    return {
        "datasetKey": {
            "name": "proj.sales.orders",
            "platform": "urn:li:dataPlatform:bigquery",
            "origin": "PROD",
        },
        "datasetProperties": {
            "name": "orders",
            "qualifiedName": "proj.sales.Orders",
            "description": "<p>All customer <b>orders</b> &amp; refunds.</p>",
            "customProperties": {"is_partitioned": "true"},
            "externalUrl": "https://console.cloud.google.com/x",
        },
        "schemaMetadata": {
            "fields": [
                {
                    "fieldPath": "[version=2.0].[type=struct].customer.[type=string].id",
                    "description": "Customer identifier",
                    "globalTags": {"tags": [{"tag": "urn:li:tag:pii"}]},
                },
                {"fieldPath": "amount"},
                {"fieldPath": "created_at"},
            ]
        },
        "editableSchemaMetadata": {
            "editableSchemaFieldInfo": [
                {"fieldPath": "amount", "description": "Order total in USD"}
            ]
        },
        "globalTags": {
            "tags": [
                {"tag": "urn:li:tag:business_unit:sales"},
                {"tag": "urn:li:tag:last_updated:2026-09-28__00_02_26"},
            ]
        },
        "domains": {"domains": ["urn:li:domain:3f2a"]},
        "container": {"container": "urn:li:container:unresolvable"},
        "ownership": {"owners": [{"owner": "urn:li:corpuser:jdoe"}]},
    }


class TestParseRegistry:
    def test_selects_searchable_text_and_reference_fields(self, specs):
        dataset = specs["dataset"]
        paths = {(f.aspect, "/".join(f.path)) for f in dataset.fields}
        assert ("datasetProperties", "description") in paths
        assert ("schemaMetadata", "fields/*/fieldPath") in paths
        assert ("globalTags", "tags/*/tag") in paths
        assert ("datasetProperties", "externalUrl") not in paths
        assert ("siblings", "siblings/*") not in paths

    def test_skips_timeseries_and_semantic_content_aspects(self, specs):
        dataset = specs["dataset"]
        assert "datasetProfile" not in dataset.aspects
        assert "semanticContent" not in dataset.aspects
        assert dataset.has_semantic_content
        assert not specs["chart"].has_semantic_content
        assert "siblings" in dataset.all_aspects

    def test_search_group_and_name_fields(self, specs):
        assert specs["dataProcessInstance"].search_group == "timeseries"
        assert [f.field_name for f in specs["tag"].name_fields] == ["name"]


class TestBuild:
    def build(self, specs, config=None, **kwargs):
        builder = EntityTextBuilder(config or EntityTextConfig())
        return builder.build(
            specs["dataset"], DATASET_URN, dataset_aspects(), resolve, **kwargs
        )

    def test_renders_dataset_markdown(self, specs):
        text = self.build(specs)
        assert text.startswith("# Dataset: orders\n")
        assert "Qualified name: proj.sales.Orders" in text
        assert "Platform: bigquery" in text
        assert "Domain: Commerce" in text
        assert "Tags: business_unit:sales" in text
        assert "Description: All customer orders & refunds." in text
        assert "## Columns" in text
        assert "- customer.id: Customer identifier; Tags: pii" in text
        assert "- amount: Order total in USD" in text
        assert "Other columns: created_at" in text

    def test_leaves_out_noise(self, specs):
        text = self.build(specs)
        # Filter-only fields, owners, unresolved references, custom properties,
        # volatile tags and values repeating a higher-tier one.
        assert "console.cloud.google.com" not in text
        assert "jdoe" not in text
        assert "unresolvable" not in text
        assert "is_partitioned" not in text
        assert "last_updated" not in text
        assert "Identifier" not in text

    def test_custom_properties_opt_in(self, specs):
        text = self.build(specs, EntityTextConfig(include_custom_properties=True))
        assert "is_partitioned: true" in text

    def test_custom_properties_order_does_not_change_the_text(self, specs):
        config = EntityTextConfig(include_custom_properties=True)
        texts = []
        for properties in ({"b": "2", "a": "1"}, {"a": "1", "b": "2"}):
            aspects = dataset_aspects()
            aspects["datasetProperties"]["customProperties"] = properties
            texts.append(
                EntityTextBuilder(config).build(
                    specs["dataset"], DATASET_URN, aspects, resolve
                )
            )
        assert texts[0] == texts[1]
        assert "a: 1, b: 2" in texts[0]

    def test_field_descriptions_are_sanitized_rich_text(self, specs):
        aspects = dataset_aspects()
        aspects["schemaMetadata"]["fields"][0]["description"] = "<p>Customer id</p>"
        text = EntityTextBuilder(EntityTextConfig()).build(
            specs["dataset"], DATASET_URN, aspects, resolve
        )
        assert "- customer.id: Customer id; Tags: pii" in text

    def test_malformed_reference_urns_are_skipped(self, specs):
        aspects = dataset_aspects()
        aspects["globalTags"]["tags"].append({"tag": "not-an-urn"})
        text = EntityTextBuilder(EntityTextConfig()).build(
            specs["dataset"], DATASET_URN, aspects, resolve
        )
        assert "not-an-urn" not in text

    def test_output_is_deterministic(self, specs):
        assert self.build(specs) == self.build(specs)

    def test_output_does_not_depend_on_registry_order(self, specs):
        assert self.build(specs) == self.build(parse_registry(reversed_registry()))

    def test_name_wins_over_an_equal_qualified_name(self, specs):
        aspects = {"datasetProperties": {"name": "orders", "qualifiedName": "orders"}}
        text = EntityTextBuilder(EntityTextConfig()).build(
            specs["dataset"], DATASET_URN, aspects, resolve
        )
        assert text.startswith("# Dataset: orders\n")

    def test_merges_siblings(self, specs):
        sibling = {
            "datasetProperties": {
                "name": "stg_orders",
                "description": "dbt model with the orders of every channel.",
            }
        }
        text = self.build(specs, siblings=[(specs["dataset"], sibling)])
        assert text.startswith("# Dataset: orders\n")
        assert "Also known as: stg_orders" in text
        assert "dbt model with the orders of every channel." in text

    def test_long_description_gets_its_own_section(self, specs):
        aspects = dataset_aspects()
        aspects["datasetProperties"]["description"] = "word " * 100
        text = EntityTextBuilder(EntityTextConfig()).build(
            specs["dataset"], DATASET_URN, aspects, resolve
        )
        assert "## Description\n\nword word" in text

    def test_caps_items_and_text(self, specs):
        aspects = dataset_aspects()
        aspects["schemaMetadata"]["fields"] = [
            {"fieldPath": f"col_{i}", "description": f"Column {i}"} for i in range(50)
        ]
        config = EntityTextConfig(max_items_per_group=10, max_text_chars=400)
        text = EntityTextBuilder(config).build(
            specs["dataset"], DATASET_URN, aspects, resolve
        )
        assert len(text) <= 400
        assert text.endswith("\n")
        assert "col_10" not in text

    def test_text_without_line_breaks_is_cut_at_the_cap(self, specs):
        config = EntityTextConfig(max_text_chars=10)
        text = EntityTextBuilder(config).build(
            specs["tag"], "urn:li:tag:pii", {"tagKey": {"name": "pii"}}, resolve
        )
        assert text == "# Tag: pii"

    def test_name_only_entity_uses_urn_id(self, specs):
        text = EntityTextBuilder(EntityTextConfig()).build(
            specs["tag"], "urn:li:tag:pii", {"tagKey": {"name": "pii"}}, resolve
        )
        assert text == "# Tag: pii\n"

    def test_entity_without_content_is_empty(self, specs):
        text = EntityTextBuilder(EntityTextConfig()).build(
            specs["dataset"], DATASET_URN, {}, resolve
        )
        assert text == ""


def test_extract_values_fans_out_arrays_maps_and_unions():
    value = {
        "fields": [
            {"type": {"com.linkedin.schema.StringType": {"x": 1}}},
            {"type": {"com.linkedin.schema.NumberType": {"x": 2}}},
        ],
        "props": {"a": "1", "b": "2"},
    }
    assert extract_values(value, ["fields", "*", "type", "x"]) == [1, 2]
    assert extract_values(value, ["props", "*"]) == ["1", "2"]
    assert extract_values(value, ["missing"]) == []


@pytest.mark.parametrize(
    "path, expected",
    [
        ("[version=2.0].[type=struct].a.[type=string].b", "a.b"),
        ("plain_column", "plain_column"),
    ],
)
def test_clean_field_path(path, expected):
    assert clean_field_path(path) == expected


def test_humanize():
    assert humanize("qualifiedName") == "Qualified name"
    assert humanize("fieldPaths") == "Field paths"
