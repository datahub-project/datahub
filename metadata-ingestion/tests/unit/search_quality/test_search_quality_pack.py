import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from generate_search_quality_pack import (
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
    longtail = [item for item in judgments["judgments"] if item["group"] == "longtail"]
    assert [item["id"] for item in longtail] == [
        "longtail-pet-profiles",
        "longtail-qualified-orders",
        "longtail-lifetime-value",
        "longtail-customer-description",
        "longtail-monthly-adoption",
        "longtail-shipping",
        "longtail-pet-adoptions-domain",
        "longtail-repeat-purchase",
        "longtail-dogs-in-movies",
        "longtail-return-window",
        "longtail-identifier",
        "longtail-column-identifier",
        "longtail-pii-tag",
        "longtail-campaign-dashboard",
        "longtail-duplicate-charts",
        "longtail-schema-name",
        "longtail-quoted-title",
        "longtail-case",
        "longtail-quoted-identifier",
        "longtail-quoted-description",
        "longtail-typo",
        "longtail-column-prefix",
        "longtail-extra-token",
        "longtail-business-label",
    ]
    ranking = [item for item in judgments["judgments"] if item["group"] == "ranking"]
    assert [item["id"] for item in ranking] == [
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
    ]
    for item in longtail:
        grades = [grade["relevance"] for grade in item["grades"]]
        assert 4 in grades
        assert all(1 <= grade <= 4 for grade in grades)
        assert item["surfaces"] == ["keyword"]
    pet_profile = next(item for item in ranking if item["id"] == "ranking-pet-profile")
    assert "dashboards.14" not in json.dumps(pet_profile)
    assert pet_profile["containsInTop"] == {
        "n": 3,
        "values": ["pet_profiles", "pet_profiles"],
    }
    pet_details = next(
        item for item in ranking if item["id"] == "ranking-partial-pet-details"
    )
    assert pet_details["containsInTop"] == {
        "n": 3,
        "values": ["dbt,long_tail_companions.analytics.pet_details"],
    }


def test_cranfield_grades_invert_onto_a_gain_scale() -> None:
    assert cranfield_relevance(1) > cranfield_relevance(4) > 0
    assert cranfield_relevance(-1) == 0
