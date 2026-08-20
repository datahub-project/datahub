import logging
import os
import tempfile
import uuid
from typing import Any, Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mce_builder import (
    make_data_product_urn,
    make_dataset_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
)
from tests.utilities.domains import Domain
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Unique per run so the three domains are freshly seeded and their entity counts
# are deterministic regardless of what else is in the instance.
_RUN_ID = uuid.uuid4().hex[:8]

# Ground truth. Domain A carries a data product on top of its datasets so the
# _entityType-filtered (dataProducts) fast path is exercised alongside the plain
# entities count; Domain C is intentionally empty to cover the zero-count path.
_DOMAIN_A = make_domain_urn(f"test-batching-a-{_RUN_ID}")
_DOMAIN_B = make_domain_urn(f"test-batching-b-{_RUN_ID}")
_DOMAIN_C = make_domain_urn(f"test-batching-c-{_RUN_ID}")

_A_DATASETS = 2
_B_DATASETS = 3
_A_DATA_PRODUCTS = 1

# Expected total entities (datasets + data products) and data-product-only counts.
_EXPECTED = {
    _DOMAIN_A: {"entities": _A_DATASETS + _A_DATA_PRODUCTS, "dataProducts": 1},
    _DOMAIN_B: {"entities": _B_DATASETS, "dataProducts": 0},
    _DOMAIN_C: {"entities": 0, "dataProducts": 0},
}


class _FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="domain_entities_batching"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event) -> None:
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self) -> None:
        self.sink.close()


def _dataset_in_domain(
    name: str, domain_urn: str
) -> List[MetadataChangeProposalWrapper]:
    dataset_urn = make_dataset_urn("snowflake", name)
    return [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(name=name),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_urn]),
        ),
    ]


def _data_product_in_domain(
    dp_id: str, domain_urn: str
) -> List[MetadataChangeProposalWrapper]:
    dp_urn = make_data_product_urn(dp_id)
    return [
        MetadataChangeProposalWrapper(
            entityUrn=dp_urn,
            aspect=DataProductPropertiesClass(name=dp_id),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dp_urn,
            aspect=DomainsClass(domains=[domain_urn]),
        ),
    ]


def _build_test_data(filename: str) -> None:
    mcps: List[MetadataChangeProposalWrapper] = []
    for urn, label in ((_DOMAIN_A, "A"), (_DOMAIN_B, "B"), (_DOMAIN_C, "C")):
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DomainPropertiesClass(name=f"Batching Test {label} {_RUN_ID}"),
            )
        )

    for i in range(_A_DATASETS):
        mcps += _dataset_in_domain(f"batching_a_{_RUN_ID}_{i}", _DOMAIN_A)
    for i in range(_A_DATA_PRODUCTS):
        mcps += _data_product_in_domain(f"batching_a_dp_{_RUN_ID}_{i}", _DOMAIN_A)
    for i in range(_B_DATASETS):
        mcps += _dataset_in_domain(f"batching_b_{_RUN_ID}_{i}", _DOMAIN_B)

    emitter = _FileEmitter(filename)
    for mcp in mcps:
        emitter.emit(mcp)
    emitter.close()


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    _, filename = tempfile.mkstemp(suffix=".json")
    try:
        _build_test_data(filename)
        yield from _ingest_cleanup_data_impl(
            auth_session, graph_client, filename, "domain_entities_batching"
        )
    finally:
        os.remove(filename)


# Mirrors the domainEntitiesFields fragment the frontend issues for a page of
# domains: count-only (count: 0) entities, plus an _entityType-filtered alias.
# Fetching all three domains as aliased fields in a SINGLE request coalesces the
# per-domain .load() calls into one DomainEntityCountsBatchLoader.batchLoad, so
# this exercises the batched fast path (and per-domain attribution) end to end.
_COUNTS_FRAGMENT = """
    urn
    entities(input: { start: 0, count: 0 }) {
      total
    }
    dataProducts: entities(
      input: { start: 0, count: 0, filters: [{ field: "_entityType", values: ["DATA_PRODUCT"] }] }
    ) {
      total
    }
"""

_BATCHED_COUNTS_QUERY = f"""
query domainEntityCounts($a: String!, $b: String!, $c: String!) {{
  a: domain(urn: $a) {{ {_COUNTS_FRAGMENT} }}
  b: domain(urn: $b) {{ {_COUNTS_FRAGMENT} }}
  c: domain(urn: $c) {{ {_COUNTS_FRAGMENT} }}
}}
"""


def test_domain_entities_counts_are_batched_and_correct(auth_session):
    variables: Dict[str, Any] = {
        "a": _DOMAIN_A,
        "b": _DOMAIN_B,
        "c": _DOMAIN_C,
    }

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(auth_session, _BATCHED_COUNTS_QUERY, variables)
        data = res["data"]
        actual = {
            alias: {
                "urn": data[alias]["urn"],
                "entities": data[alias]["entities"]["total"],
                "dataProducts": data[alias]["dataProducts"]["total"],
            }
            for alias in ("a", "b", "c")
        }
        logger.info(f"batched domain entity counts: {actual}")

        for alias, urn in (("a", _DOMAIN_A), ("b", _DOMAIN_B), ("c", _DOMAIN_C)):
            expected = _EXPECTED[urn]
            assert data[alias]["urn"] == urn
            assert data[alias]["entities"]["total"] == expected["entities"], (
                f"{urn} entities: expected {expected['entities']}, "
                f"got {data[alias]['entities']['total']}"
            )
            assert data[alias]["dataProducts"]["total"] == expected["dataProducts"], (
                f"{urn} dataProducts: expected {expected['dataProducts']}, "
                f"got {data[alias]['dataProducts']['total']}"
            )

    check()
