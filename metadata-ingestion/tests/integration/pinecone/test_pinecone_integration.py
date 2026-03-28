"""Integration tests for Pinecone connector using mocked Pinecone SDK."""

from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration_batch_0

FROZEN_TIME = "2024-01-15 10:00:00"


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


def _make_index_stats(namespaces: Dict[str, int]) -> Dict[str, Any]:
    return {
        "dimension": 384,
        "namespaces": {ns: {"vector_count": count} for ns, count in namespaces.items()},
        "total_vector_count": sum(namespaces.values()),
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
            {"": 10000}
        )

        mock_product_index.list.return_value = iter(
            [["vec0", "vec1", "vec2", "vec3", "vec4"]]
        )
        mock_product_index.fetch.return_value = MagicMock(
            vectors={
                f"vec{i}": MagicMock(
                    id=f"vec{i}",
                    values=[0.1] * 384,
                    metadata={
                        "category": "electronics",
                        "price": 99.99,
                        "in_stock": True,
                    },
                )
                for i in range(5)
            }
        )

        mock_document_index.list.return_value = iter([["doc0", "doc1", "doc2"]])
        mock_document_index.fetch.return_value = MagicMock(
            vectors={
                f"doc{i}": MagicMock(
                    id=f"doc{i}",
                    values=[0.1] * 768,
                    metadata={"title": f"Document {i}", "source": "wiki"},
                )
                for i in range(3)
            }
        )

        yield mock_pc


@pytest.mark.freeze_time(FROZEN_TIME)
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

    golden_path = (
        pytestconfig.rootpath / "tests/integration/pinecone/pinecone_mcps_golden.json"
    )

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=sink_file,
        golden_path=golden_path,
        ignore_paths=mce_helpers.IGNORE_PATH_DEFAULTS,
    )


@pytest.mark.freeze_time(FROZEN_TIME)
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
    assert source.report.indexes_scanned == 2
    assert (
        source.report.datasets_generated == 3
    )  # 2 namespaces in product + 1 default in document


@pytest.mark.freeze_time(FROZEN_TIME)
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
    assert source.report.indexes_scanned == 1
    assert source.report.indexes_filtered == 1
