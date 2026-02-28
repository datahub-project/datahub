from unittest.mock import patch

import pytest

_FAKE_PROBE_EMBEDDING = [[0.1, 0.2, 0.3]]


@pytest.fixture(autouse=True)
def mock_embedding_probe():
    """Prevent real network calls from the DocumentChunkingSource startup probe.

    The probe calls _generate_embeddings([{"text": "test"}]) in __init__ to validate
    credentials. In unit tests there are no real credentials, so we mock it to return a
    fake success. Tests that need to control probe behaviour (success or failure) patch
    _generate_embeddings themselves inside the test body — that patch takes precedence
    within its scope.
    """
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source."
        "DocumentChunkingSource._generate_embeddings",
        return_value=_FAKE_PROBE_EMBEDDING,
    ):
        yield
