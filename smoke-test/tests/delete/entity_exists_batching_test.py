import logging
import os
import tempfile
import uuid
from typing import Dict, List

import pytest

from conftest import _ingest_cleanup_data_impl
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import DatasetPropertiesClass, StatusClass
from tests.utils import execute_graphql, wait_for_writes_to_sync, with_test_retry

logger = logging.getLogger(__name__)

# Unique per run so absent urns are genuinely absent regardless of instance history.
_RUN_ID = uuid.uuid4().hex[:8]
_PLATFORM = "snowflake"

# `exists` resolves through a request-scoped DataLoader, so several `exists` selections in one
# request share a single primary-store read. The failure modes are all about the shared read:
# results applied to the wrong key, a mixed batch collapsing to all-true or all-false, and the
# soft-delete semantics being changed by the overload the batch uses.

_PRESENT_COUNT = 4
_ABSENT_COUNT = 4
_LARGE_BATCH_COUNT = 30


def _present_urn(i: int) -> str:
    return make_dataset_urn(_PLATFORM, f"exists_present_{_RUN_ID}_{i}")


def _absent_urn(i: int) -> str:
    # Never ingested.
    return make_dataset_urn(_PLATFORM, f"exists_absent_{_RUN_ID}_{i}")


def _soft_deleted_urn() -> str:
    return make_dataset_urn(_PLATFORM, f"exists_softdeleted_{_RUN_ID}")


# Interleaved so that any shift in key/result alignment changes an assertion: adjacent entries
# never share an expected value.
_INTERLEAVED: List[tuple] = []
for _i in range(max(_PRESENT_COUNT, _ABSENT_COUNT)):
    if _i < _PRESENT_COUNT:
        _INTERLEAVED.append((_present_urn(_i), True))
    if _i < _ABSENT_COUNT:
        _INTERLEAVED.append((_absent_urn(_i), False))


class _FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="entity_exists_batching"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event) -> None:
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self) -> None:
        self.sink.close()


def _build_test_data(filename: str) -> None:
    mcps: List[MetadataChangeProposalWrapper] = []

    for i in range(_PRESENT_COUNT):
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=_present_urn(i),
                aspect=DatasetPropertiesClass(name=f"exists_present_{_RUN_ID}_{i}"),
            )
        )

    for i in range(_LARGE_BATCH_COUNT):
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=make_dataset_urn(_PLATFORM, f"exists_bulk_{_RUN_ID}_{i}"),
                aspect=DatasetPropertiesClass(name=f"exists_bulk_{_RUN_ID}_{i}"),
            )
        )

    # Soft-deleted, not hard-deleted: the aspect row still exists, so `exists` must stay true.
    # The removal itself is applied after ingest, not here — see `ingest_cleanup_data`.
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=_soft_deleted_urn(),
            aspect=DatasetPropertiesClass(name=f"exists_softdeleted_{_RUN_ID}"),
        )
    )

    emitter = _FileEmitter(filename)
    for mcp in mcps:
        emitter.emit(mcp)
    emitter.close()


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    _, filename = tempfile.mkstemp(suffix=".json")
    try:
        _build_test_data(filename)
        ingest = _ingest_cleanup_data_impl(
            auth_session, graph_client, filename, "entity_exists_batching"
        )
        next(ingest)

        # Applied after ingest rather than in the ingest file: ingest gates on every dataset
        # becoming searchable, and search excludes soft-deleted entities, so a removed=true
        # entity in the file would never satisfy that wait.
        logger.info(f"soft-deleting {_soft_deleted_urn()}")
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=_soft_deleted_urn(),
                aspect=StatusClass(removed=True),
            )
        )
        wait_for_writes_to_sync()

        yield

        # Drives the helper through its cleanup half.
        for _ in ingest:
            pass
    finally:
        os.remove(filename)


def _exists_query(urns: List[str], operation: str) -> str:
    fields = "\n  ".join(
        f'e{i}: dataset(urn: "{urn}") {{ urn exists }}' for i, urn in enumerate(urns)
    )
    return f"query {operation} {{\n  {fields}\n}}"


def _fetch_exists(auth_session, urns: List[str], operation: str) -> Dict[str, bool]:
    res = execute_graphql(auth_session, _exists_query(urns, operation), {})
    data = res["data"]
    actual: Dict[str, bool] = {}
    for i in range(len(urns)):
        entry = data[f"e{i}"]
        assert entry is not None, f"dataset({urns[i]}) resolved to null"
        actual[entry["urn"]] = entry["exists"]
    return actual


def test_exists_batched_mixed_present_and_absent(auth_session):
    """A batch holding both present and absent urns must not collapse to one answer."""
    urns = [urn for urn, _ in _INTERLEAVED]
    expected = {urn: value for urn, value in _INTERLEAVED}

    @with_test_retry()
    def check() -> None:
        actual = _fetch_exists(auth_session, urns, "existsMixedBatch")
        logger.info(f"batched exists: {actual}")
        assert actual == expected

    check()


def test_exists_batched_matches_one_per_request(auth_session):
    """The batched answer must equal resolving each urn in its own request."""
    urns = [urn for urn, _ in _INTERLEAVED]

    @with_test_retry()
    def check() -> None:
        batched = _fetch_exists(auth_session, urns, "existsBatchedForCompare")
        single = {}
        for i, urn in enumerate(urns):
            single.update(_fetch_exists(auth_session, [urn], f"existsSingle{i}"))
        assert batched == single

    check()


def test_exists_soft_deleted_entity_still_exists(auth_session):
    """
    A soft-deleted entity still has aspects, so `exists` stays true. Guards the overload the
    batch path uses: switching it to exclude soft-deleted entities would flip this to false.
    """
    soft_deleted = _soft_deleted_urn()
    urns = [_present_urn(0), soft_deleted, _absent_urn(0)]

    @with_test_retry()
    def check() -> None:
        actual = _fetch_exists(auth_session, urns, "existsWithSoftDeleted")
        assert actual[_present_urn(0)] is True
        assert actual[soft_deleted] is True, (
            "soft-deleted entity must still report exists=true"
        )
        assert actual[_absent_urn(0)] is False

    check()


def test_exists_duplicate_urns_in_one_request(auth_session):
    """A urn repeated in one request is deduplicated for the read but every alias still answers."""
    present, absent = _present_urn(1), _absent_urn(1)
    urns = [present, absent, present, absent, present]

    @with_test_retry()
    def check() -> None:
        res = execute_graphql(
            auth_session, _exists_query(urns, "existsDuplicateUrns"), {}
        )
        data = res["data"]
        actual = [data[f"e{i}"]["exists"] for i in range(len(urns))]
        assert actual == [True, False, True, False, True]

    check()


def test_exists_large_batch_all_present(auth_session):
    """A batch well beyond a page of search results must answer every key."""
    urns = [
        make_dataset_urn(_PLATFORM, f"exists_bulk_{_RUN_ID}_{i}")
        for i in range(_LARGE_BATCH_COUNT)
    ]

    @with_test_retry()
    def check() -> None:
        actual = _fetch_exists(auth_session, urns, "existsLargeBatch")
        assert len(actual) == _LARGE_BATCH_COUNT
        missing = [urn for urn, value in actual.items() if value is not True]
        assert not missing, f"expected all present, got false for: {missing}"

    check()
