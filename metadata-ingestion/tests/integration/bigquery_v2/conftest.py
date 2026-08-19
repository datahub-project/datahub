from typing import Iterator
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_service_account_credentials() -> Iterator[None]:
    """Stop the BigQuery connection from validating the dummy private key."""
    with patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
    ):
        yield
