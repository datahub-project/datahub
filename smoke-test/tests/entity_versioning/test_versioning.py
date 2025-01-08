import pytest


@pytest.fixture(scope="module", autouse=True)
def test_link_unlink_version(auth_session):
    """Fixture to execute setup before and tear down after all tests are run"""
    res_data = link_version(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["linkAssetVersion"]
    assert (
        res_data["data"]["linkAssetVersion"]
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
    )

    res_data = unlink_version(auth_session)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["unlinkAssetVersion"]


def link_version(auth_session):
    json = {
        "mutation": """mutation linkAssetVersion($input: LinkVersionInput!) {\n
            linkAssetVersion(input: $input)
            }\n
        }""",
        "variables": {
            "input": {
                "version": "1233456",
                "versionSet": "urn:li:versionSet:(12345678910,dataset)",
                "linkedEntity": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
            }
        },
    }
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()

    return response.json()


def unlink_version(auth_session):
    json = {
        "mutation": """mutation unlinkAssetVersion($input: UnlinkVersionInput!) {\n
            unlinkAssetVersion(input: $input)
            }\n
        }""",
        "variables": {
            "input": {
                "versionSet": "urn:li:versionSet:(12345678910,dataset)",
                "unlinkedEntity": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
            }
        },
    }
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()

    return response.json()
