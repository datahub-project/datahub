import asyncio
import base64
import logging

import pytest
import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from datahub.ingestion.source.couchbase.couchbase_aggregate import CouchbaseAggregate
from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)
retries = Retry(total=5, backoff_factor=2, status_forcelist=[404, 405, 500, 503])
adapter = HTTPAdapter(max_retries=retries)
session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)


class BasicAuth(AuthBase):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode("ascii")
        auth_encoded = base64.b64encode(auth_bytes)
        request_headers = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
        }
        r.headers.update(request_headers)
        return r


def http_test_get(url: str, auth: BasicAuth) -> int:
    response = session.get(
        url,
        verify=False,
        timeout=15,
        auth=auth,
    )
    return response.status_code


def http_test_post(url: str, auth: BasicAuth, data: dict) -> int:
    response = session.post(
        url,
        verify=False,
        timeout=15,
        auth=auth,
        data=data,
    )
    return response.status_code


@pytest.mark.slow
@pytest.mark.asyncio
async def test_couchbase_driver(
    docker_compose_runner, pytestconfig, tmp_path, mock_time
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/couchbase"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "couchbase"
    ) as docker_services:
        wait_for_port(docker_services, "testdb", 8093)

        result = http_test_get(
            "http://127.0.0.1:8091/pools/default/buckets/data",
            auth=BasicAuth("Administrator", "password"),
        )
        assert result == 200

        result = http_test_post(
            "http://127.0.0.1:8093/query/service",
            auth=BasicAuth("Administrator", "password"),
            data={"statement": "SELECT count(*) as count FROM data.data.customers"},
        )
        assert result == 200

        await asyncio.sleep(2)

        # Run the driver test.
        couchbase_connect = CouchbaseConnect(
            "couchbases://127.0.0.1", "Administrator", "password", 5, 60
        )
        couchbase_connect.cluster_init()

        bucket_list = couchbase_connect.bucket_list()
        assert bucket_list is not None and len(bucket_list) == 1

        for bucket in bucket_list:
            assert bucket == "data"
            scope_list = couchbase_connect.scope_list(bucket)
            assert "_default" in scope_list and "data" in scope_list
            for scope in scope_list:
                collection_list = couchbase_connect.collection_list(bucket, scope)
                if scope == "data":
                    assert "customers" in collection_list
                elif scope == "_default":
                    assert "_default" in collection_list

        documents = []
        aggregator = CouchbaseAggregate(couchbase_connect, "data.data.customers")
        async for chunk in aggregator.get_documents():
            documents.extend(chunk)
        assert len(documents) == 1000
