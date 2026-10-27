import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from generate_search_quality_pack import (
    bootstrap_platform_aspects,
    cranfield_relevance,
    pack_dir,
    parse_cranfield_queries,
    parse_qrels,
    validate_pack,
)


def test_pack_judgments_reference_datapack_entities() -> None:
    validate_pack(pack_dir())
    definitions = json.loads((pack_dir() / "01-definitions.json").read_text())
    property_definition = next(
        item
        for item in definitions
        if item["entityUrn"] == "urn:li:structuredProperty:catalogProbeTier"
    )
    assert (
        property_definition["aspect"]["json"]["valueType"]
        == "urn:li:dataType:datahub.string"
    )


def test_bootstrap_platforms_keep_registry_info() -> None:
    definitions = json.loads((pack_dir() / "01-definitions.json").read_text())
    bootstrap = bootstrap_platform_aspects()
    matched = set()
    for item in definitions:
        if item["aspectName"] != "dataPlatformInfo":
            continue
        name = item["entityUrn"].removeprefix("urn:li:dataPlatform:")
        assert item["changeType"] == "CREATE"
        assert item["headers"] == {"If-None-Match": "*"}
        expected = bootstrap.get(name)
        if expected is None:
            assert "logoUrl" not in item["aspect"]["json"]
            continue
        assert item["aspect"]["json"] == expected
        assert expected["logoUrl"]
        matched.add(name)
    assert matched == {
        "bigquery",
        "dbt",
        "looker",
        "mongodb",
        "postgres",
        "powerbi",
        "s3",
        "snowflake",
        "tableau",
    }
    assert all(
        item["changeType"] == "UPSERT"
        for item in definitions
        if item["aspectName"] != "dataPlatformInfo"
    )


def test_every_benchmark_query_joins_to_its_judgments() -> None:
    """Guard the cran.qry to cranqrel join.

    cran.qry labels queries with the original Cranfield numbers, which skip values
    and reach 365, while cranqrel numbers the same queries 1..225 by position.
    Joining on the label silently grades each query against a different query.
    """
    source = pack_dir() / "source/cranfield"
    queries = parse_cranfield_queries(source / "cran.qry")
    qrels = parse_qrels(source / "cranqrel")

    assert [query["id"] for query in queries] == [str(index) for index in range(1, 226)]
    assert all(query["id"] in qrels for query in queries)
    assert sum(len(qrels[query["id"]]) for query in queries) == sum(
        len(entries) for entries in qrels.values()
    )

    judgments = json.loads((pack_dir() / "judgments.json").read_text())
    benchmark = [
        item for item in judgments["judgments"] if item["group"] == "benchmark"
    ]
    assert all(
        any(grade["relevance"] > 0 for grade in item["grades"]) for item in benchmark
    )


def test_longtail_catalog_judgments_cover_the_catalog_scenarios() -> None:
    judgments = json.loads((pack_dir() / "judgments.json").read_text())
    longtail = [
        item for item in judgments["judgments"] if item["group"] == "longtail-ecommerce"
    ]
    assert [item["id"] for item in longtail] == [
        "longtail-ecommerce-pet-profiles",
        "longtail-ecommerce-qualified-orders",
        "longtail-ecommerce-lifetime-value",
        "longtail-ecommerce-customer-description",
        "longtail-ecommerce-monthly-adoption",
        "longtail-ecommerce-shipping",
        "longtail-ecommerce-pet-adoptions-domain",
        "longtail-ecommerce-repeat-purchase",
        "longtail-ecommerce-dogs-in-movies",
        "longtail-ecommerce-return-window",
        "longtail-ecommerce-identifier",
        "longtail-ecommerce-column-identifier",
        "longtail-ecommerce-pii-tag",
        "longtail-ecommerce-campaign-dashboard",
        "longtail-ecommerce-duplicate-charts",
        "longtail-ecommerce-schema-name",
        "longtail-ecommerce-quoted-title",
        "longtail-ecommerce-case",
        "longtail-ecommerce-quoted-identifier",
        "longtail-ecommerce-quoted-description",
        "longtail-ecommerce-typo",
        "longtail-ecommerce-column-prefix",
        "longtail-ecommerce-extra-token",
        "longtail-ecommerce-business-label",
        "longtail-ecommerce-structured-property-string",
        "longtail-ecommerce-structured-property-urn",
        "longtail-ecommerce-structured-property-boost",
    ]
    keyword_probes = [
        item for item in judgments["judgments"] if item["group"] == "keyword-probe"
    ]
    assert [item["id"] for item in keyword_probes] == [
        "ranking-pet-profiles",
        "ranking-pet-profile",
        "ranking-return-rate",
        "ranking-partial-pet-details",
        "ranking-collaborative-actionitems",
        "ranking-customer-orders",
        "ranking-row-count",
        "ranking-lineage-pet-profiles",
        "ranking-pet-profiles-identifier",
        "ranking-pet-profiles-case",
        "ranking-pet-profiles-quoted",
        "ranking-pii",
        "ranking-campaign-adoptions",
        "ranking-shipping-quoted",
        "interactive-list-personal-owner",
        "interactive-list-personal-owner-other-viewer",
        "interactive-list-group-owner",
        "interactive-list-group-owner-other-viewer",
        "interactive-list-catalog-completeness",
        "interactive-list-usage-percentile",
        "catalog-exact-name",
        "catalog-typo",
        "catalog-synonym",
        "catalog-quoted",
        "catalog-property",
        "identity-name-qualified-parity",
        "identity-bounded-qualified-path",
        "identity-earlier-qualified-segment",
    ]
    for item in longtail:
        grades = [grade["relevance"] for grade in item["grades"]]
        assert 4 in grades
        assert all(1 <= grade <= 4 for grade in grades)
        assert item["surfaces"] == ["keyword"]
    customers = {
        platform: (
            "urn:li:dataset:(urn:li:dataPlatform:"
            f"{platform},calm-pagoda-323403.jaffle_shop.customers,PROD)"
        )
        for platform in ("bigquery", "dbt", "postgres")
    }

    def grade(query_id: str, urn: str) -> int:
        item = next(row for row in longtail if row["id"] == query_id)
        return next(
            entry["relevance"] for entry in item["grades"] if entry["urn"] == urn
        )

    for query_id in (
        "longtail-ecommerce-lifetime-value",
        "longtail-ecommerce-column-identifier",
        "longtail-ecommerce-column-prefix",
    ):
        assert grade(query_id, customers["bigquery"]) == 4
        assert grade(query_id, customers["dbt"]) == 4
        assert grade(query_id, customers["postgres"]) == 4
    assert grade("longtail-ecommerce-customer-description", customers["bigquery"]) == 2
    pet_profile = next(
        item for item in keyword_probes if item["id"] == "ranking-pet-profile"
    )
    assert "dashboards.14" not in json.dumps(pet_profile)
    assert pet_profile["containsInTop"] == {
        "n": 5,
        "values": ["pet_profiles", "pet_profiles"],
    }
    exact_name_tables = [
        "dbt,long_tail_companions.adoption.pet_profiles",
        "mongodb,adoption.pet_profiles",
        "snowflake,long_tail_companions.adoption.pet_profiles",
    ]
    pet_profiles = next(
        item for item in keyword_probes if item["id"] == "ranking-pet-profiles"
    )
    assert pet_profiles["orderedContains"] == ["pet_profiles"]
    assert pet_profiles["containsInTop"] == {"n": 5, "values": exact_name_tables}
    quoted = next(
        item for item in keyword_probes if item["id"] == "ranking-pet-profiles-quoted"
    )
    assert "orderedContains" not in quoted
    assert quoted["containsInTop"] == {"n": 4, "values": exact_name_tables}
    pet_details = next(
        item for item in keyword_probes if item["id"] == "ranking-partial-pet-details"
    )
    assert "orderedContains" not in pet_details
    assert pet_details["containsInTop"] == {
        "n": 5,
        "values": ["dbt,long_tail_companions.analytics.pet_details"],
    }
    identity_parity = next(
        item
        for item in keyword_probes
        if item["id"] == "identity-name-qualified-parity"
    )
    assert identity_parity["containsInTop"] == {
        "n": 2,
        "values": [
            "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,identity_name_only,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,identity_name_and_qualified,PROD)",
        ],
    }
    identity_path = next(
        item
        for item in keyword_probes
        if item["id"] == "identity-bounded-qualified-path"
    )
    assert identity_path["rankAbove"] == {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,identity_path,PROD)",
        "above": [
            "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,identity_path_partial,PROD)"
        ],
    }
    boosts = [
        item for item in judgments["judgments"] if item["group"] == "entity-boost"
    ]
    assert [item["id"] for item in boosts] == [
        "entity-boost-glossary-term",
        "entity-boost-glossary-node",
        "entity-boost-domain",
    ]
    term = boosts[0]
    assert term["orderedContains"] == ["dataProduct:entityboost"]
    assert boosts[1]["orderedContains"] == [
        "dataProduct:entityboost",
        "glossaryNode:entityboost",
    ]
    assert term["containsInTop"] == {
        "n": 3,
        "values": ["dataProduct:entityboost", "catalogprobe,entityboost"],
    }
    assert "orderedContains" not in boosts[2]
    assert set(boosts[2]["present"]) == {
        "domain:entityboost",
        "dataProduct:entityboost",
    }
    searchable = [
        item for item in judgments["judgments"] if item["group"] == "entity-search"
    ]
    assert [item["id"] for item in searchable] == [
        "entity-search-user",
        "entity-search-group",
    ]
    assert all(item["exactTopHit"].startswith("urn:li:") for item in searchable)


def test_cranfield_grades_invert_onto_a_gain_scale() -> None:
    assert cranfield_relevance(1) > cranfield_relevance(4) > 0
    assert cranfield_relevance(-1) == 0


def test_structured_property_judgments_cover_fulltext_probes() -> None:
    judgments = json.loads((pack_dir() / "judgments.json").read_text())
    structured = [
        item
        for item in judgments["judgments"]
        if item["group"] == "structured-property"
    ]
    assert [item["id"] for item in structured] == [
        "structured-property-string-opt-in",
        "structured-property-urn-opt-in",
        "structured-property-urn-opt-in-identity",
        "structured-property-string-opt-out-fulltext",
        "structured-property-urn-opt-out-fulltext",
        "structured-property-string-opt-out-filter",
        "structured-property-urn-opt-in-filter",
        "structured-property-urn-opt-out-filter",
        "structured-property-boosted-ranking",
        "structured-property-fulltext-token",
        "structured-property-fulltext-stem",
        "structured-property-fulltext-phrase",
        "structured-property-fulltext-case",
    ]
    by_id = {item["id"]: item for item in structured}
    for query_id in (
        "structured-property-string-opt-out-fulltext",
        "structured-property-urn-opt-out-fulltext",
    ):
        assert by_id[query_id]["expectNoHits"] is True
    filtered = by_id["structured-property-string-opt-out-filter"]
    assert filtered["query"] == "*"
    assert filtered["filter"] == {
        "field": "structuredProperties.packStringFilter",
        "condition": "EQUAL",
        "value": "packstringoptout",
    }
    assert filtered["exactTopHit"] == filtered["grades"][0]["urn"]
    for query_id in (
        "structured-property-fulltext-token",
        "structured-property-fulltext-stem",
        "structured-property-fulltext-phrase",
        "structured-property-fulltext-case",
    ):
        item = by_id[query_id]
        assert item["surfaces"] == ["keyword"]
        assert item["exactTopHit"]
        assert item["matchedFieldContains"] == {
            "value": "structuredProperties.catalogProbeTier",
            "hits": 1,
        }
