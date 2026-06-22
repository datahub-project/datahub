import json

import pytest
import requests
import os
from jsoncomparison import Compare, NO_DIFF

GMS_ENDPOINT = "http://localhost:8080"
GOLDEN_FILES_PATH = "./spark-smoke-test/golden_json/"
golden_files = os.listdir(GOLDEN_FILES_PATH)

print(golden_files)
[file_name.strip(".json") for file_name in golden_files]
restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}

JSONDIFF_CONFIG = {
    'output': {
        'console': False,
        'file': {
            'allow_nan': True,
            'ensure_ascii': True,
            'indent': 4,
            'name': None,
            'skipkeys': True,
        },
    },
    'types': {
        'float': {
            'allow_round': 2,
        },
        'list': {
            'check_length': False,
        },
    },
}
json_compare = Compare(JSONDIFF_CONFIG)


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    # os.system('docker run --network datahub_network spark-submit')
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def sort_aspects(input):
    print(input)
    item_id = list(input["value"].keys())[0]
    input["value"][item_id]["aspects"] = sorted(
        input["value"][item_id]["aspects"], key=lambda x: list(x.keys())[0]
    )


@pytest.mark.dependency(depends=["test_healthchecks"])
@pytest.mark.parametrize("json_file", golden_files, )
def test_ingestion_via_rest(json_file):
    print(json_file)
    # Opening JSON file
    f = open(os.path.join(GOLDEN_FILES_PATH, json_file))
    golden_data = json.load(f)
    for urn, value in golden_data.items():
        url = GMS_ENDPOINT + "/entities/" + urn
        print(url)
        response = requests.get(url)
        response.raise_for_status()

        data = sort_aspects(response.json())
        value = sort_aspects(value)
        diff = json_compare.check(value, data)
        print(urn)
        if diff != NO_DIFF:
            print("Expected: {} Actual: {}".format(value, data))
        assert diff == NO_DIFF


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_glue_iceberg_connection_instance():
    """The Iceberg-on-Glue job's lineage edge must resolve to the platform_instance mapped for its
    catalog ARN (arn:aws:glue:us-east-1:123456789012 -> domain_a). The platform_instance is encoded
    in the dataset URN, so finding it on a dataJobInputOutput edge proves the connection-instance
    mapping was applied end-to-end.

    This scenario lives in the spark-submit harness (not the in-JVM unit suite) because OpenLineage
    only emits the Glue catalog symlink under a real spark-submit execution. The job uses the file
    emitter (output bind-mounted from the spark-submit container); we assert the edge directly rather
    than golden the full output, because Spark lineage MCPs carry per-run volatility (dataProcessInstance
    UUID URNs, timestamps embedded in stringified aspects, temp paths) that a full golden can't stably
    capture — the edge is the one stable signal that proves the fix."""
    output_path = "./spark-smoke-test/glue-output/glue_mcps.json"
    assert os.path.isfile(output_path), f"Glue job emitted no MCPs to {output_path}"
    with open(output_path) as f:
        mcps = json.load(f)

    expected = (
        "urn:li:dataset:(urn:li:dataPlatform:glue,"
        "domain_a.my_glue_database.my_glue_table,PROD)"
    )
    edge_urns = set()
    for mcp in mcps:
        if mcp.get("aspectName") == "dataJobInputOutput":
            aspect = json.loads(mcp["aspect"]["value"])
            for edge_key in ("inputDatasetEdges", "outputDatasetEdges"):
                for edge in aspect.get(edge_key, []):
                    edge_urns.add(edge.get("destinationUrn"))

    assert expected in edge_urns, (
        "Glue upstream missing platform_instance 'domain_a' (connection mapping not applied). "
        f"Edges found: {sorted(edge_urns)}"
    )
