"""The executor's masking, driven end to end with no mocks.

DefaultDispatcher -> dispatch_async (one secret scope per task) ->
DefaultExecutor -> the subprocess ingestion / test-connection tasks -> a real
`datahub` child in this venv, with secrets resolved from a file secret store
and passed in the stdin envelope. Four tasks run concurrently. The assertions
read what the executor itself produced -- results, structured reports and
every artifact written to disk -- so they hold whatever the host process
does or does not install on stdout.
"""

import json
import pathlib
import time
from typing import Dict

import pytest

from datahub.executor.dispatcher.default_dispatcher import DefaultDispatcher
from datahub.executor.execution.default_executor import (
    DefaultExecutor,
    DefaultExecutorConfig,
)
from datahub.executor.execution.task import TaskConfig
from datahub.executor.request.execution_request import ExecutionRequest
from datahub.secret.secret_store import SecretStoreConfig

# Concatenated so the secret scanner does not read the fixtures as credentials.
SECRETS = {
    "PIPE_SECRET_A": "alpha-" + "pipe-value-81",
    "PATH_SECRET": "path-" + "value-02x",
    "PIPE_SECRET_B": "bravo-" + "pipe-value-55",
    "HOST_SECRET": "host-" + "value-77.invalid",
}
NEVER_IN_PLAINTEXT = ("PATH_SECRET", "HOST_SECRET")


class _CapturingExecutor(DefaultExecutor):
    results: Dict[str, Dict[str, object]] = {}

    def execute(self, request):  # type: ignore[no-untyped-def]
        result = super().execute(request)
        self.results[request.exec_id] = {
            "type": result.type.name,
            "text": result.get_summary() + (result.get_structured_report() or ""),
        }
        return result


def _recipe(source: dict, pipeline_name: str, sink_path: pathlib.Path) -> str:
    return json.dumps(
        {
            "pipeline_name": pipeline_name,
            "source": source,
            "sink": {"type": "file", "config": {"filename": str(sink_path)}},
        }
    )


@pytest.mark.integration
def test_concurrent_tasks_mask_their_own_secrets_and_only_theirs(
    tmp_path: pathlib.Path,
) -> None:
    pytest.importorskip("psycopg2")
    secrets_dir, work, data = tmp_path / "secrets", tmp_path / "work", tmp_path / "data"
    for d in (secrets_dir, work, data):
        d.mkdir()
    for name, value in SECRETS.items():
        (secrets_dir / name).write_text(value)
    source_file = data / "input.json"
    source_file.write_text(
        json.dumps(
            [
                {
                    "entityType": "dataset",
                    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)",
                    "changeType": "UPSERT",
                    "aspectName": "status",
                    "aspect": {"json": {"removed": False}},
                }
            ]
        )
    )

    _CapturingExecutor.results = {}
    executor = _CapturingExecutor(
        DefaultExecutorConfig(
            id="default",
            task_configs=[
                TaskConfig(
                    name="RUN_INGEST",
                    type="datahub.executor.execution.sub_process_ingestion_task.SubProcessIngestionTask",
                    configs={
                        "tmp_dir": str(work / "tmp"),
                        "log_dir": str(work / "logs"),
                    },
                ),
                TaskConfig(
                    name="TEST_CONNECTION",
                    type="datahub.executor.execution.sub_process_test_connection_task.SubProcessTestConnectionTask",
                    configs={"tmp_dir": str(work / "tmp")},
                ),
            ],
            secret_stores=[
                SecretStoreConfig(type="file", config={"basedir": str(secrets_dir)})
            ],
        )
    )
    file_source = {"type": "file", "config": {"path": str(source_file)}}
    requests = [
        ExecutionRequest(
            exec_id="success",
            name="RUN_INGEST",
            args={
                "recipe": _recipe(file_source, "${PIPE_SECRET_A}", data / "out_a.json"),
                "version": "native",
            },
        ),
        # The secret sits inside a path the failure message echoes.
        ExecutionRequest(
            exec_id="failure",
            name="RUN_INGEST",
            args={
                "recipe": _recipe(
                    {
                        "type": "file",
                        "config": {"path": str(data / "${PATH_SECRET}.json")},
                    },
                    "failing-pipeline",
                    data / "out_f.json",
                ),
                "version": "native",
            },
        ),
        # Tenant B's recipe carries tenant A's secret VALUE as a plain name. On
        # a single shared registry B's output named A's variable.
        ExecutionRequest(
            exec_id="tenant-b",
            name="RUN_INGEST",
            args={
                "recipe": _recipe(
                    file_source,
                    "${PIPE_SECRET_B}",
                    data / f"out_{SECRETS['PIPE_SECRET_A']}.json",
                ),
                "version": "native",
            },
        ),
        # The DNS failure echoes the secret host name into the report.
        ExecutionRequest(
            exec_id="test-connection",
            name="TEST_CONNECTION",
            args={
                "recipe": json.dumps(
                    {
                        "source": {
                            "type": "postgres",
                            "config": {
                                "host_port": "${HOST_SECRET}:5432",
                                "username": "u",
                                "password": "p",
                                "database": "d",
                            },
                        }
                    }
                ),
                "version": "native",
            },
        ),
    ]
    dispatcher = DefaultDispatcher([executor])
    for request in requests:
        dispatcher.dispatch(request)
    deadline = time.monotonic() + 300
    while dispatcher.threads and time.monotonic() < deadline:
        time.sleep(0.5)
    results = _CapturingExecutor.results

    assert {k: v["type"] for k, v in results.items()} == {
        "success": "SUCCESS",
        "failure": "FAILURE",
        "tenant-b": "SUCCESS",
        "test-connection": "SUCCESS",
    }
    assert "***REDACTED:PATH_SECRET***" in str(results["failure"]["text"])
    assert "***REDACTED:HOST_SECRET***" in str(results["test-connection"]["text"])
    # B's own identifier survives, and nothing names A's variable.
    tenant_b = str(results["tenant-b"]["text"])
    assert SECRETS["PIPE_SECRET_A"] in tenant_b
    assert "REDACTED:PIPE_SECRET_A" not in tenant_b

    written = [
        p.read_text(errors="replace") for p in work.rglob("*") if p.is_file()
    ] + [str(r["text"]) for r in results.values()]
    for name in NEVER_IN_PLAINTEXT:
        assert not [t for t in written if SECRETS[name] in t], name
