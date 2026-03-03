"""Tests for Mode connector concurrent report processing (max_threads config).

These tests verify that:
1. Threaded execution produces the same work units as sequential (correctness)
2. Connection pool sizing scales with max_threads
3. Threaded execution provides wall-clock speedup on I/O-bound workloads (perf)
"""

import json
import pathlib
import threading
import time
from typing import Any, Dict, List, Optional, Union
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = pathlib.Path(__file__).parent

JSON_RESPONSE_MAP = {
    "https://app.mode.com/api/verify": "verify.json",
    "https://app.mode.com/api/account": "user.json",
    "https://app.mode.com/api/acryl/spaces": "spaces.json",
    "https://app.mode.com/api/acryl/spaces/157933cc1168/reports": "reports_157933cc1168.json",
    "https://app.mode.com/api/acryl/spaces/75737b70402e/reports": "reports_75737b70402e.json",
    "https://app.mode.com/api/modeuser": "user.json",
    "https://app.mode.com/api/acryl/reports/9d2da37fa91e/queries": "queries.json",
    "https://app.mode.com/api/acryl/reports/9d2da37fa91e/queries/6e26a9f3d4e2/charts": "charts.json",
    "https://app.mode.com/api/acryl/data_sources": "data_sources.json",
    "https://app.mode.com/api/acryl/definitions": "definitions.json",
    "https://app.mode.com/api/acryl/spaces/157933cc1168/datasets": "datasets_157933cc1168.json",
    "https://app.mode.com/api/acryl/spaces/75737b70402e/datasets": "datasets_75737b70402e.json",
    "https://app.mode.com/api/acryl/reports/24f66e1701b6": "dataset_24f66e1701b6.json",
    "https://app.mode.com/api/acryl/reports/24f66e1701b6/queries": "dataset_queries_24f66e1701b6.json",
}

EMBEDDED_KEY_LOOKUP = {
    "datasets": "reports",
}


class ThreadSafeResponse:
    """A standalone response object returned per get() call, avoiding shared state."""

    def __init__(self, json_data: dict, status_code: int = 200, url: str = ""):
        self.json_data = json_data
        self.status_code = status_code
        self.url = url
        self.headers: Dict[str, str] = {}

    def json(self) -> dict:
        return self.json_data

    @property
    def text(self) -> str:
        return json.dumps(self.json_data)

    def raise_for_status(self) -> None:
        pass


class ThreadSafeMockSession:
    """A mock requests.Session that returns independent response objects per call.

    Supports two modes:
    - File-based: response_map maps URL -> filename, loaded from resources_dir/setup/
    - Inline: response_map maps URL -> dict (used when resources_dir is None)
    """

    def __init__(
        self,
        response_map: Union[Dict[str, str], Dict[str, dict]],
        resources_dir: Optional[pathlib.Path] = None,
        latency: float = 0.0,
    ):
        self.response_map = response_map
        self.resources_dir = resources_dir
        self.latency = latency
        self.auth = None
        self.headers: Dict[str, str] = {}
        self._call_count = 0
        self._lock = threading.Lock()

    def mount(self, prefix: str, adapter: object) -> None:
        pass

    def _resolve_response(self, base_url: str) -> dict:
        """Look up the response for a URL, loading from file if needed."""
        entry = self.response_map.get(base_url)
        if entry is None:
            return {}
        if isinstance(entry, dict):
            return entry
        # File-based lookup
        with open(f"{self.resources_dir}/setup/{entry}") as f:
            return json.loads(f.read())

    def _empty_page_response(self, base_url: str) -> ThreadSafeResponse:
        """Build an empty second-page response to stop pagination."""
        endpoint_paths = base_url.rstrip("/").split("/")
        last_path = endpoint_paths[-1]
        if last_path in EMBEDDED_KEY_LOOKUP:
            last_path = EMBEDDED_KEY_LOOKUP[last_path]
        return ThreadSafeResponse(
            {
                "_links": {"self": {"href": base_url}},
                "_embedded": {last_path: []},
            },
            url=base_url,
        )

    def get(self, url: str, timeout: int = 40) -> ThreadSafeResponse:
        if self.latency > 0:
            time.sleep(self.latency)

        with self._lock:
            self._call_count += 1

        base_url = url.split("?")[0]

        if "page=2" in url:
            return self._empty_page_response(base_url)

        data = self._resolve_response(base_url)
        return ThreadSafeResponse(data, url=base_url)

    @property
    def call_count(self) -> int:
        with self._lock:
            return self._call_count


def make_thread_safe_session(*args: Any, **kwargs: Any) -> ThreadSafeMockSession:
    return ThreadSafeMockSession(JSON_RESPONSE_MAP, test_resources_dir)


# ──────────────────────────────────────────────────────────────────────
# Correctness tests
# ──────────────────────────────────────────────────────────────────────


@freeze_time(FROZEN_TIME)
def test_mode_threaded_produces_same_output(pytestconfig, tmp_path):
    """Threaded execution (max_threads=2) produces the same MCEs as sequential."""
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=make_thread_safe_session,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "mode-test-threaded",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                        "max_threads": 2,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mode_mces_threaded.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/mode_mces_threaded.json",
            golden_path=test_resources_dir / "mode_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


@freeze_time(FROZEN_TIME)
def test_mode_threaded_higher_thread_count(pytestconfig, tmp_path):
    """Verify correctness even with more threads than reports."""
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=make_thread_safe_session,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "mode-test-threaded-high",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                        "max_threads": 10,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mode_mces_threaded_high.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/mode_mces_threaded_high.json",
            golden_path=test_resources_dir / "mode_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


# ──────────────────────────────────────────────────────────────────────
# Config and pool sizing tests
# ──────────────────────────────────────────────────────────────────────


def test_max_threads_defaults_to_one():
    """max_threads defaults to 1, preserving sequential behavior."""
    from datahub.ingestion.source.mode import ModeConfig

    config = ModeConfig(
        token="test",
        password="test",
        workspace="test_workspace",
    )
    assert config.max_threads == 1


def test_max_threads_custom_value():
    """max_threads accepts custom values."""
    from datahub.ingestion.source.mode import ModeConfig

    config = ModeConfig(
        token="test",
        password="test",
        workspace="test_workspace",
        max_threads=8,
    )
    assert config.max_threads == 8


def test_max_threads_rejects_zero_and_negative():
    """max_threads must be >= 1."""
    import pytest
    from pydantic import ValidationError

    from datahub.ingestion.source.mode import ModeConfig

    with pytest.raises(ValidationError):
        ModeConfig(
            token="test",
            password="test",
            workspace="test_workspace",
            max_threads=0,
        )
    with pytest.raises(ValidationError):
        ModeConfig(
            token="test",
            password="test",
            workspace="test_workspace",
            max_threads=-1,
        )


@freeze_time(FROZEN_TIME)
def test_pool_size_scales_with_max_threads():
    """HTTPAdapter pool_connections/pool_maxsize = max_threads + 10."""
    from unittest.mock import MagicMock

    from datahub.ingestion.source.mode import ModeConfig, ModeSource

    config = ModeConfig(
        token="test",
        password="test",
        workspace="test_workspace",
        max_threads=5,
    )

    with (
        patch(
            "datahub.ingestion.source.mode.requests.Session",
            side_effect=make_thread_safe_session,
        ),
        patch("datahub.ingestion.source.mode.HTTPAdapter") as mock_adapter_cls,
    ):
        mock_adapter_cls.return_value = MagicMock()
        ctx = MagicMock()
        ctx.graph = None
        ctx.pipeline_name = "test"
        ctx.run_id = "test-run"
        ctx.pipeline_config = None

        try:
            ModeSource(ctx, config)
        except Exception:
            pass  # May fail on verify call; we only care about the adapter

        mock_adapter_cls.assert_called_once()
        _, kwargs = mock_adapter_cls.call_args
        assert kwargs["pool_connections"] == 15
        assert kwargs["pool_maxsize"] == 15


# ──────────────────────────────────────────────────────────────────────
# Performance tests
# ──────────────────────────────────────────────────────────────────────

PERF_TIMESTAMP = "2024-01-01T00:00:00.000Z"


def _build_perf_response_map(
    num_reports: int, num_queries_per_report: int
) -> Dict[str, dict]:
    """Build a URL -> inline JSON response map for N reports with M queries each."""
    responses: Dict[str, dict] = {}

    responses["https://app.mode.com/api/verify"] = {"authenticated": True}
    responses["https://app.mode.com/api/acryl/data_sources"] = {
        "_embedded": {
            "data_sources": [
                {
                    "id": 34499,
                    "adapter": "jdbc:postgresql",
                    "name": "test_db",
                    "database": "test_database",
                }
            ]
        }
    }
    responses["https://app.mode.com/api/acryl/definitions"] = {
        "_embedded": {"definitions": []}
    }
    responses["https://app.mode.com/api/modeuser"] = {
        "username": "testuser",
        "email": "test@example.com",
    }

    space_token = "perfspace001"
    responses["https://app.mode.com/api/acryl/spaces"] = {
        "_embedded": {
            "spaces": [
                {
                    "token": space_token,
                    "id": 1,
                    "name": "PerfTest",
                    "restricted": False,
                    "default_access_level": "view",
                    "_links": {"creator": {"href": "/api/modeuser"}},
                }
            ]
        },
        "_links": {"self": {"href": "/api/acryl/spaces"}},
    }

    reports: List[dict] = []
    for i in range(num_reports):
        report_token = f"report_{i:04d}"
        reports.append(
            {
                "token": report_token,
                "id": 1000 + i,
                "name": f"Report {i}",
                "description": f"Perf test report {i}",
                "created_at": PERF_TIMESTAMP,
                "updated_at": PERF_TIMESTAMP,
                "edited_at": PERF_TIMESTAMP,
                "last_saved_at": PERF_TIMESTAMP,
                "last_run_at": PERF_TIMESTAMP,
                "archived": False,
                "view_count": i,
                "imported_datasets": [],
                "_links": {"creator": {"href": "/api/modeuser"}},
            }
        )

        queries: List[dict] = []
        for q in range(num_queries_per_report):
            query_token = f"query_{i:04d}_{q:02d}"
            queries.append(
                {
                    "id": 2000 + i * 100 + q,
                    "token": query_token,
                    "raw_query": f"SELECT * FROM table_{q}",
                    "name": f"Query {q}",
                    "created_at": PERF_TIMESTAMP,
                    "updated_at": PERF_TIMESTAMP,
                    "data_source_id": 34499,
                    "last_run_id": 99999,
                    "_links": {"creator": {"href": "/api/modeuser"}},
                }
            )

            responses[
                f"https://app.mode.com/api/acryl/reports/{report_token}/queries/{query_token}/charts"
            ] = {"_embedded": {"charts": []}}

        responses[f"https://app.mode.com/api/acryl/reports/{report_token}/queries"] = {
            "_embedded": {"queries": queries}
        }

    responses[f"https://app.mode.com/api/acryl/spaces/{space_token}/reports"] = {
        "_embedded": {"reports": reports},
        "_links": {"self": {"href": f"/api/acryl/spaces/{space_token}/reports"}},
    }

    responses[f"https://app.mode.com/api/acryl/spaces/{space_token}/datasets"] = {
        "_embedded": {"reports": []},
        "_links": {"self": {"href": f"/api/acryl/spaces/{space_token}/datasets"}},
    }

    return responses


def test_threading_speedup(tmp_path):
    """Verify that max_threads > 1 provides wall-clock speedup with simulated latency.

    Uses 10 reports with 2 queries each. Each HTTP call sleeps 50ms.
    With 4 threads, expect ~2-4x wall-clock speedup.

    Note: No @freeze_time here -- freezegun patches time.monotonic() which
    would make our wall-clock measurements return 0.
    """
    num_reports = 10
    num_queries_per_report = 2
    latency = 0.05  # 50ms per request

    responses = _build_perf_response_map(num_reports, num_queries_per_report)

    # Sequential run (max_threads=1)
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=lambda *a, **kw: ThreadSafeMockSession(responses, latency=latency),
    ):
        pipeline_seq = Pipeline.create(
            {
                "run_id": "mode-perf-sequential",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                        "max_threads": 1,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/perf_seq.json",
                    },
                },
            }
        )
        t0 = time.perf_counter()
        pipeline_seq.run()
        sequential_time = time.perf_counter() - t0

    # Threaded run (max_threads=4)
    with patch(
        "datahub.ingestion.source.mode.requests.Session",
        side_effect=lambda *a, **kw: ThreadSafeMockSession(responses, latency=latency),
    ):
        pipeline_par = Pipeline.create(
            {
                "run_id": "mode-perf-parallel",
                "source": {
                    "type": "mode",
                    "config": {
                        "token": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "https://app.mode.com/",
                        "workspace": "acryl",
                        "max_threads": 4,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/perf_par.json",
                    },
                },
            }
        )
        t0 = time.perf_counter()
        pipeline_par.run()
        parallel_time = time.perf_counter() - t0

    speedup = sequential_time / parallel_time if parallel_time > 0 else float("inf")

    print(
        f"\nPerf results: sequential={sequential_time:.2f}s, "
        f"parallel={parallel_time:.2f}s, speedup={speedup:.1f}x"
    )

    # With 4 threads and 50ms latency, we should see at least 1.5x speedup.
    # Using a conservative threshold to avoid flaky CI.
    assert speedup > 1.5, (
        f"Expected >1.5x speedup but got {speedup:.2f}x "
        f"(seq={sequential_time:.2f}s, par={parallel_time:.2f}s)"
    )


def test_threading_produces_same_entities_as_sequential(tmp_path):
    """Threaded and sequential runs produce the same set of entity URNs."""
    num_reports = 6
    num_queries_per_report = 2

    responses = _build_perf_response_map(num_reports, num_queries_per_report)

    def run_pipeline(max_threads: int, output_name: str) -> str:
        output_path = f"{tmp_path}/{output_name}.json"
        with patch(
            "datahub.ingestion.source.mode.requests.Session",
            side_effect=lambda *a, **kw: ThreadSafeMockSession(responses),
        ):
            pipeline = Pipeline.create(
                {
                    "run_id": f"mode-entity-check-{max_threads}",
                    "source": {
                        "type": "mode",
                        "config": {
                            "token": "xxxx",
                            "password": "xxxx",
                            "connect_uri": "https://app.mode.com/",
                            "workspace": "acryl",
                            "max_threads": max_threads,
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_path},
                    },
                }
            )
            pipeline.run()
            pipeline.raise_from_status()
        return output_path

    seq_path = run_pipeline(1, "entities_seq")
    par_path = run_pipeline(4, "entities_par")

    def extract_urns(path: str) -> set:
        with open(path) as f:
            data = json.load(f)
        urns = set()
        for item in data:
            if "entityUrn" in item:
                urns.add(item["entityUrn"])
            elif "proposedSnapshot" in item:
                snapshot = item["proposedSnapshot"]
                if isinstance(snapshot, dict):
                    urn = snapshot.get("urn") or snapshot.get(
                        list(snapshot.keys())[0], {}
                    ).get("urn")
                    if urn:
                        urns.add(urn)
        return urns

    seq_urns = extract_urns(seq_path)
    par_urns = extract_urns(par_path)

    assert seq_urns, "Sequential run should produce entities"
    assert seq_urns == par_urns, (
        f"Entity URN mismatch: "
        f"only_in_seq={seq_urns - par_urns}, "
        f"only_in_par={par_urns - seq_urns}"
    )
