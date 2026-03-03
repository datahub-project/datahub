from typing import Any, Dict

from tests.utils import execute_graphql, ingest_file_via_rest, with_test_retry

bootstrap_small = "test_resources/bootstrap_single.json"
bootstrap_small_2 = "test_resources/bootstrap_single2.json"


@with_test_retry()
def _ensure_dataset_present_correctly(auth_session):
    urn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,testDataset,PROD)"
    query = """query getDataset($urn: String!) {
                dataset(urn: $urn) {
                    urn
                    name
                    description
                    platform {
                        urn
                    }
                    schemaMetadata {
                        name
                        version
                        createdAt
                    }
                    outgoing: relationships(
                                input: { types: ["DownstreamOf", "Consumes", "Produces"], direction: OUTGOING, start: 0, count: 2000 }
                            ) {
                            start
                            count
                            total
                            relationships {
                                type
                                direction
                                entity {
                                    urn
                                    type
                                }
                            }
                    }
                }
            }"""
    variables: Dict[str, Any] = {"urn": urn}
    res_data = execute_graphql(auth_session, query, variables)

    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    assert len(res_data["data"]["dataset"]["outgoing"]["relationships"]) == 1


def test_ingestion_via_rest_rapid(auth_session):
    ingest_file_via_rest(auth_session, bootstrap_small)
    ingest_file_via_rest(auth_session, bootstrap_small_2)
    _ensure_dataset_present_correctly(auth_session)
