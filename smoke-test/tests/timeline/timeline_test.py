from datahub.cli import delete_cli
from datahub.cli import timeline_cli
from tests.utils import ingest_file_via_rest


def test_all():
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = (
        "test-timeline-sample-kafka"
    )
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    ingest_file_via_rest("tests/timeline/timeline_test_data.json")
    ingest_file_via_rest("tests/timeline/timeline_test_datav2.json")
    ingest_file_via_rest("tests/timeline/timeline_test_datav3.json")

    res_data = timeline_cli.get_timeline(dataset_urn, ["TAG", "DOCUMENTATION", "TECHNICAL_SCHEMA", "GLOSSARY_TERM",
                                                       "OWNERSHIP"], None, None, False)

    delete_cli.delete_one_urn_cmd(dataset_urn, False, False, "dataset", None, None)
    assert res_data
    assert len(res_data) == 3
    assert res_data[0]["semVerChange"] == "MINOR"
    assert len(res_data[0]["changeEvents"]) == 6
    assert res_data[1]["semVerChange"] == "MAJOR"
    assert len(res_data[1]["changeEvents"]) == 6
    assert res_data[2]["semVerChange"] == "MAJOR"
    assert len(res_data[2]["changeEvents"]) == 5
    assert res_data[2]["semVer"] == "2.0.0-computed"

    print(res_data)