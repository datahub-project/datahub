import threading
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

import datahub.ingestion.source.dbt.dbt_artifacts as dbt_artifacts_module
from datahub.ingestion.source.dbt.dbt_artifacts import (
    ArtifactReader,
    is_missing_file_error,
)


def _reader(concurrency: int = 2) -> ArtifactReader:
    return ArtifactReader(
        aws_connection=None, gcs_connection=None, concurrency=concurrency
    )


def _object_store_error(
    code: Optional[str] = None,
    status: Optional[int] = None,
    with_response: bool = True,
) -> ValueError:
    """Mimic read_file_as_bytes: a generic ValueError whose __cause__ is the client error."""
    cause = Exception("client error")
    if with_response:
        response: Dict[str, Any] = {}
        if code is not None:
            response["Error"] = {"Code": code}
        if status is not None:
            response["ResponseMetadata"] = {"HTTPStatusCode": status}
        cause.response = response  # type: ignore[attr-defined]
    err = ValueError("Failed to read s3://bucket/key from object store")
    err.__cause__ = cause
    return err


def test_prefetch_reraises_worker_memory_error() -> None:
    """A MemoryError inside a prefetch worker must fail fast, not be captured.

    The worker catches Exception to hand ordinary read failures back for the main
    thread to re-raise at the original call site, but MemoryError is an Exception
    subclass: capturing it let the other workers keep reading objects into an
    already-exhausted process. It must propagate through .result() instead.
    """
    reader = _reader()
    groups = [["uri-0"], ["uri-1"]]

    def fake_read(uri: str, *args: object, **kwargs: object) -> bytes:
        if uri == "uri-0":
            raise MemoryError("artifact too large to read")
        return uri.encode()

    with mock.patch.object(dbt_artifacts_module, "read_file_as_bytes", fake_read):
        with pytest.raises(MemoryError):
            list(reader.prefetch_in_order(groups, concurrency=2))


def test_prefetch_windows_submission_when_an_early_group_is_slow() -> None:
    """A slow next-in-order group must not let the whole run's bytes accumulate.

    Submission is windowed to `concurrency` groups ahead of the consumer, so while
    the first group blocks, only that many groups are ever started - the later
    groups' bytes are not pre-fetched into an unbounded reorder buffer.
    """
    reader = _reader()
    concurrency = 2
    n = 6
    groups = [[f"uri-{i}"] for i in range(n)]

    release_first = threading.Event()
    started: List[str] = []
    started_lock = threading.Lock()
    a_group_started = threading.Semaphore(0)

    def fake_read(uri: str, *args: object, **kwargs: object) -> bytes:
        with started_lock:
            started.append(uri)
        a_group_started.release()
        if uri == "uri-0":
            assert release_first.wait(timeout=5)
        return uri.encode()

    yielded: List[str] = []

    def consume() -> None:
        for fetched in reader.prefetch_in_order(groups, concurrency):
            for value in fetched.values():
                assert isinstance(value, bytes)
                yielded.append(value.decode())

    with mock.patch.object(dbt_artifacts_module, "read_file_as_bytes", fake_read):
        consumer = threading.Thread(target=consume)
        consumer.start()
        try:
            # The window starts exactly `concurrency` groups; a further start must
            # not happen while the first group is still blocked.
            for _ in range(concurrency):
                assert a_group_started.acquire(timeout=5)
            assert not a_group_started.acquire(timeout=0.3)
            with started_lock:
                assert len(started) == concurrency
        finally:
            release_first.set()
        consumer.join(timeout=5)

    assert not consumer.is_alive()
    assert yielded == [f"uri-{i}" for i in range(n)]


@pytest.mark.parametrize(
    "err, missing",
    [
        (FileNotFoundError(2, "No such file or directory"), True),
        (_object_store_error(code="NoSuchKey"), True),
        (_object_store_error(code="NotFound"), True),
        # S3-compatible stores may report only the status, with no error code.
        (_object_store_error(status=404), True),
        (_object_store_error(code="AccessDenied", status=403), False),
        (_object_store_error(with_response=False), False),
        (ValueError("no cause at all"), False),
        (None, False),
    ],
)
def test_is_missing_file_error_classification(
    err: Optional[BaseException], missing: bool
) -> None:
    assert is_missing_file_error(err) is missing
