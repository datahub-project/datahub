"""Shared builders for the Fabric REST client tests.

`fabric_response` and `specced_auth_helper` are plain functions so the OneLake
tests (whose client extends `BaseFabricClient`) build their HTTP fixtures the
same way; the fixtures below are for this directory only.
"""

import json
from typing import Iterator, Optional
from unittest.mock import NonCallableMagicMock, create_autospec

import pytest
import requests

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient

FABRIC_API_URL = "https://api.fabric.microsoft.com/v1/x"


def fabric_response(status: int, body: Optional[object] = None) -> requests.Response:
    """A real `requests.Response`, so `raise_for_status()` and `.json()` behave
    as they do against Fabric. `body` is JSON-encoded unless it is already bytes."""
    response = requests.Response()
    response.status_code = status
    response._content = (
        body if isinstance(body, bytes) else json.dumps(body or {}).encode()
    )
    response.url = FABRIC_API_URL
    return response


def specced_auth_helper() -> NonCallableMagicMock:
    auth = create_autospec(FabricAuthHelper, instance=True)
    auth.get_authorization_header.return_value = "Bearer placeholder-token"
    return auth


@pytest.fixture
def auth_helper() -> NonCallableMagicMock:
    return specced_auth_helper()


@pytest.fixture
def core_client(auth_helper: NonCallableMagicMock) -> Iterator[FabricCoreClient]:
    client = FabricCoreClient(auth_helper=auth_helper)
    yield client
    client.close()
