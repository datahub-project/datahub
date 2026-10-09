import threading
from typing import List
from unittest import mock

import pytest

import datahub.ingestion.source.dbt.dbt_artifacts as dbt_artifacts_module
from datahub.ingestion.source.common.object_store_files import ObjectNotFoundError
from datahub.ingestion.source.dbt.dbt_artifacts import ArtifactReader, is_glob_pattern


def _reader(concurrency: int = 2) -> ArtifactReader:
    return ArtifactReader(
        aws_connection=None, gcs_connection=None, concurrency=concurrency
    )


def test_prefetch_reraises_worker_memory_error() -> None:
    """A MemoryError in a prefetch worker propagates, unlike ordinary read failures,
    which are captured and re-raised at the main-thread call site."""
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


def test_prefetch_close_waits_for_in_flight_reads_and_starts_no_more() -> None:
    reader = _reader()
    started: List[str] = []
    release = threading.Event()

    def fake_read(uri: str, *args: object, **kwargs: object) -> bytes:
        started.append(uri)
        if uri == "uri-1":
            assert release.wait(timeout=5)
        return uri.encode()

    groups = [[f"uri-{i}"] for i in range(10)]
    with mock.patch.object(dbt_artifacts_module, "read_file_as_bytes", fake_read):
        prefetch = reader.prefetch_in_order(groups, concurrency=2)
        next(prefetch)
        closer = threading.Thread(target=prefetch.close)
        closer.start()
        closer.join(timeout=0.3)
        # Still waiting on the in-flight read of uri-1.
        assert closer.is_alive()
        release.set()
        closer.join(timeout=5)

    assert not closer.is_alive()
    assert started == ["uri-0", "uri-1"]


@pytest.mark.parametrize(
    "err", [FileNotFoundError(2, "missing"), ObjectNotFoundError("missing")]
)
def test_load_optional_json_absent_file_is_none(err: Exception) -> None:
    assert _reader().load_optional_json("p", {"p": err}, optional=True) is None
    with pytest.raises(FileNotFoundError):
        _reader().load_optional_json("p", {"p": err}, optional=False)


def test_load_optional_json_ambiguous_failure_raises() -> None:
    # Not proof of absence (permissions, throttling), so never read as "no file".
    with pytest.raises(ValueError):
        _reader().load_optional_json(
            "p", {"p": ValueError("AccessDenied")}, optional=True
        )


@pytest.mark.parametrize(
    "path, is_glob",
    [
        ("/dbt/*/manifest.json", True),
        ("s3://bucket/*/manifest.json", True),
        ("/dbt/manifest.json", False),
        # A literal glob character is escaped, never inferred from the filesystem.
        ("/dbt/[[]prod]/manifest.json", True),
        ("https://host/manifest.json?X-Amz-Signature=abc", False),
        ("https://host/*/manifest.json", True),
    ],
)
def test_is_glob_pattern(path: str, is_glob: bool) -> None:
    assert is_glob_pattern(path) == is_glob
