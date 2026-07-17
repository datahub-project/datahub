"""Integration tests for Pinecone connector using mocked Pinecone SDK."""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pinecone")

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.pinecone.pinecone_source import PineconeSource
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration_batch_0

GOLDEN_FILE = Path(__file__).parent / "pinecone_mcps_golden.json"


def _make_index_description(
    name: str,
    dimension: int = 384,
    metric: str = "cosine",
) -> Dict[str, Any]:
    return {
        "name": name,
        "dimension": dimension,
        "metric": metric,
        "host": f"{name}-abc123.svc.pinecone.io",
        "status": {"state": "Ready"},
        "spec": {"serverless": {"cloud": "aws", "region": "us-east-1"}},
    }


def _make_index_stats(
    namespaces: Dict[str, int], dimension: int = 384
) -> Dict[str, Any]:
    return {
        "dimension": dimension,
        "namespaces": {ns: {"vector_count": count} for ns, count in namespaces.items()},
        "total_vector_count": sum(namespaces.values()),
    }


def _make_fetch_response(
    vector_ids: List[str], metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """Build a dict-based fetch response (not MagicMock) so .get() works correctly."""
    return {
        "vectors": {vid: {"values": None, "metadata": metadata} for vid in vector_ids}
    }


@pytest.fixture
def mock_pinecone_sdk():
    """Set up mocked Pinecone SDK with test data."""
    with patch(
        "datahub.ingestion.source.pinecone.pinecone_client.Pinecone"
    ) as mock_pinecone_class:
        mock_pc = MagicMock()
        mock_pinecone_class.return_value = mock_pc

        mock_pc.list_indexes.return_value = [
            {"name": "product-embeddings"},
            {"name": "document-search"},
        ]

        mock_pc.describe_index.side_effect = lambda name: _make_index_description(
            name,
            dimension=384 if name == "product-embeddings" else 768,
            metric="cosine" if name == "product-embeddings" else "dotproduct",
        )

        mock_product_index = MagicMock()
        mock_document_index = MagicMock()

        mock_pc.Index.side_effect = lambda name: (
            mock_product_index if name == "product-embeddings" else mock_document_index
        )

        mock_product_index.describe_index_stats.return_value = _make_index_stats(
            {"electronics": 5000, "clothing": 3000}
        )
        mock_document_index.describe_index_stats.return_value = _make_index_stats(
            {"": 10000}, dimension=768
        )

        # list() returns a fresh iterator each call (iterators are consumed)
        product_vec_ids = ["vec0", "vec1", "vec2", "vec3", "vec4"]
        mock_product_index.list.side_effect = lambda **kwargs: iter(product_vec_ids)

        # fetch() returns a plain dict so .get("vectors", {}) works correctly
        mock_product_index.fetch.side_effect = lambda **kwargs: _make_fetch_response(
            vector_ids=product_vec_ids,
            metadata={"category": "electronics", "price": 99.99, "in_stock": True},
        )

        doc_vec_ids = ["doc0", "doc1", "doc2"]
        mock_document_index.list.side_effect = lambda **kwargs: iter(doc_vec_ids)
        mock_document_index.fetch.side_effect = lambda **kwargs: _make_fetch_response(
            vector_ids=doc_vec_ids,
            metadata={"title": "Document", "source": "wiki"},
        )

        yield mock_pc


def test_pinecone_ingest(
    pytestconfig: pytest.Config,
    tmp_path: Path,
    mock_pinecone_sdk: MagicMock,
) -> None:
    """Test full Pinecone ingestion with mocked SDK produces expected output."""
    sink_file = str(tmp_path / "pinecone_mces.json")

    pipeline = Pipeline.create(
        {
            "run_id": "pinecone-integration-test",
            "source": {
                "type": "pinecone",
                "config": {
                    "api_key": "test-api-key",
                    "platform_instance": "test-instance",
                    "enable_schema_inference": True,
                    "schema_sampling_size": 10,
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": sink_file},
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()

    source = pipeline.source
    assert isinstance(source, PineconeSource)
    assert source.get_report().indexes_scanned == 2
    assert source.get_report().datasets_generated == 3

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=sink_file,
        golden_path=GOLDEN_FILE,
    )


def test_pinecone_ingest_no_schema_inference(
    tmp_path: Path,
    mock_pinecone_sdk: MagicMock,
) -> None:
    """Test Pinecone ingestion with schema inference disabled."""
    sink_file = str(tmp_path / "pinecone_no_schema.json")

    pipeline = Pipeline.create(
        {
            "run_id": "pinecone-no-schema-test",
            "source": {
                "type": "pinecone",
                "config": {
                    "api_key": "test-api-key",
                    "enable_schema_inference": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": sink_file},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    source = pipeline.source
    assert isinstance(source, PineconeSource)
    assert source.get_report().indexes_scanned == 2
    assert source.get_report().datasets_generated == 3


def test_pinecone_ingest_with_index_filter(
    tmp_path: Path,
    mock_pinecone_sdk: MagicMock,
) -> None:
    """Test Pinecone ingestion with index filtering."""
    sink_file = str(tmp_path / "pinecone_filtered.json")

    pipeline = Pipeline.create(
        {
            "run_id": "pinecone-filter-test",
            "source": {
                "type": "pinecone",
                "config": {
                    "api_key": "test-api-key",
                    "enable_schema_inference": False,
                    "index_pattern": {
                        "allow": ["product-.*"],
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": sink_file},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    source = pipeline.source
    assert isinstance(source, PineconeSource)
    assert source.get_report().indexes_scanned == 1
    assert source.get_report().indexes_filtered == 1
