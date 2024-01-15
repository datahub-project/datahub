import json
import pytest
from time import sleep

from datahub.cli import timeline_cli
from datahub.cli.cli_utils import guess_entity_type, post_entity

from tests.utils import (get_datahub_graph, ingest_file_via_rest,
                         wait_for_writes_to_sync)

pytestmark = pytest.mark.no_cypress_suite1

def test_all():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-timeline-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    ingest_file_via_rest("tests/timeline/timeline_test_data.json")
    ingest_file_via_rest("tests/timeline/timeline_test_datav2.json")
    ingest_file_via_rest("tests/timeline/timeline_test_datav3.json")

    res_data = timeline_cli.get_timeline(
        dataset_urn,
        ["TAG", "DOCUMENTATION", "TECHNICAL_SCHEMA", "GLOSSARY_TERM", "OWNER"],
        None,
        None,
        False,
    )
    get_datahub_graph().hard_delete_entity(urn=dataset_urn)

    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 10
    assert res_data[1]["semVerChange"] == "MAJOR"
    assert len(res_data[1]["changeEvents"]) == 9
    assert res_data[2]["semVerChange"] == "MAJOR"
    assert len(res_data[2]["changeEvents"]) == 6
    assert res_data[2]["semVer"] == "2.0.0-computed"


def test_schema():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-timeline-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    put(dataset_urn, "schemaMetadata", "test_resources/timeline/newschema.json")
    put(dataset_urn, "schemaMetadata", "test_resources/timeline/newschemav2.json")
    put(dataset_urn, "schemaMetadata", "test_resources/timeline/newschemav3.json")

    res_data = timeline_cli.get_timeline(
        dataset_urn, ["TECHNICAL_SCHEMA"], None, None, False
    )

    get_datahub_graph().hard_delete_entity(urn=dataset_urn)
    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 6
    assert res_data[1]["semVerChange"] == "MAJOR"
    assert len(res_data[1]["changeEvents"]) == 3
    assert res_data[2]["semVerChange"] == "MAJOR"
    assert len(res_data[2]["changeEvents"]) == 3
    assert res_data[2]["semVer"] == "2.0.0-computed"


def test_glossary():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-timeline-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    put(dataset_urn, "glossaryTerms", "test_resources/timeline/newglossary.json")
    put(dataset_urn, "glossaryTerms", "test_resources/timeline/newglossaryv2.json")
    put(dataset_urn, "glossaryTerms", "test_resources/timeline/newglossaryv3.json")

    res_data = timeline_cli.get_timeline(
        dataset_urn, ["GLOSSARY_TERM"], None, None, False
    )

    get_datahub_graph().hard_delete_entity(urn=dataset_urn)
    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 1
    assert res_data[1]["semVerChange"] == "MINOR"
    assert len(res_data[1]["changeEvents"]) == 1
    assert res_data[2]["semVerChange"] == "MINOR"
    assert len(res_data[2]["changeEvents"]) == 2
    assert res_data[2]["semVer"] == "0.2.0-computed"


def test_documentation():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-timeline-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    put(
        dataset_urn,
        "institutionalMemory",
        "test_resources/timeline/newdocumentation.json",
    )
    put(
        dataset_urn,
        "institutionalMemory",
        "test_resources/timeline/newdocumentationv2.json",
    )
    put(
        dataset_urn,
        "institutionalMemory",
        "test_resources/timeline/newdocumentationv3.json",
    )

    res_data = timeline_cli.get_timeline(
        dataset_urn, ["DOCUMENTATION"], None, None, False
    )

    get_datahub_graph().hard_delete_entity(urn=dataset_urn)
    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 1
    assert res_data[1]["semVerChange"] == "MINOR"
    assert len(res_data[1]["changeEvents"]) == 1
    assert res_data[2]["semVerChange"] == "MINOR"
    assert len(res_data[2]["changeEvents"]) == 3
    assert res_data[2]["semVer"] == "0.2.0-computed"


def test_tags():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-timeline-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    put(dataset_urn, "globalTags", "test_resources/timeline/newtags.json")
    put(dataset_urn, "globalTags", "test_resources/timeline/newtagsv2.json")
    put(dataset_urn, "globalTags", "test_resources/timeline/newtagsv3.json")

    res_data = timeline_cli.get_timeline(dataset_urn, ["TAG"], None, None, False)

    get_datahub_graph().hard_delete_entity(urn=dataset_urn)
    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 1
    assert res_data[1]["semVerChange"] == "MINOR"
    assert len(res_data[1]["changeEvents"]) == 1
    assert res_data[2]["semVerChange"] == "MINOR"
    assert len(res_data[2]["changeEvents"]) == 2
    assert res_data[2]["semVer"] == "0.2.0-computed"


def test_ownership():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-timeline-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    put(dataset_urn, "ownership", "test_resources/timeline/newownership.json")
    put(dataset_urn, "ownership", "test_resources/timeline/newownershipv2.json")
    put(dataset_urn, "ownership", "test_resources/timeline/newownershipv3.json")

    res_data = timeline_cli.get_timeline(dataset_urn, ["OWNER"], None, None, False)

    get_datahub_graph().hard_delete_entity(urn=dataset_urn)
    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 2
    assert res_data[1]["semVerChange"] == "MINOR"
    assert len(res_data[1]["changeEvents"]) == 1
    assert res_data[2]["semVerChange"] == "MINOR"
    assert len(res_data[2]["changeEvents"]) == 2
    assert res_data[2]["semVer"] == "0.2.0-computed"


def put(urn: str, aspect: str, aspect_data: str) -> None:
    """Update a single aspect of an entity"""

    entity_type = guess_entity_type(urn)
    with open(aspect_data) as fp:
        aspect_obj = json.load(fp)
        status = post_entity(
            urn=urn,
            aspect_name=aspect,
            entity_type=entity_type,
            aspect_value=aspect_obj,
        )
        wait_for_writes_to_sync()
