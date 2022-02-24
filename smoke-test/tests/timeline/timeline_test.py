from datahub.cli import timeline_cli
from tests.utils import delete_urns_from_file
from tests.utils import ingest_file_via_rest


def test_schema():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = (
        "test-timeline-sample-kafka"
    )
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    ingest_file_via_rest("tests/timeline/timeline_test_data.json")
    ingest_file_via_rest("tests/timeline/timeline_test_datav2.json")
    ingest_file_via_rest("tests/timeline/timeline_test_datav3.json")

    res_data = timeline_cli.get_timeline(dataset_urn, ["TECHNICAL_SCHEMA", "TAG", "DOCUMENTATION", "OWNERSHIP",
                                                       "GLOSSARY_TERM"], None, None, False)

    assert res_data

    #pdb.set_trace()
    print(res_data)
    delete_urns_from_file("tests/timeline/timeline_test_data.json")