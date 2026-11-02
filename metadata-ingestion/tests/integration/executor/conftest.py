"""Shared fixtures and helpers for executor integration tests."""

from __future__ import annotations

import io
import json
import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest

from datahub.executor.execution import wrapper_common
from datahub.executor.wrappers import run_ingest
from tests.integration.executor.gms_stub import RecordingGmsServer

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "file"
RUN_ID = "8c026510-1111-4111-8111-111111111111"


def _venv_root() -> Path:
    """Return the absolute path of the active virtual environment root."""
    return Path(sys.prefix).resolve()


def _recipe_text(gms_url: str, metadata_path: Path) -> str:
    """Build a minimal DataHub ingestion recipe YAML string pointing at *metadata_path*."""
    # chr(92) is a backslash — normalise Windows paths to forward slashes inside YAML.
    meta = str(metadata_path.resolve()).replace(chr(92), "/")
    return f"""run_id: {RUN_ID}
pipeline_name: executor_report_to_regression
source:
  type: file
  config:
    path: "{meta}"
    stateful_ingestion:
      enabled: false
sink:
  type: datahub-rest
  config:
    server: "{gms_url}"
    mode: SYNC
"""


def _run_main(
    mod: ModuleType,
    recipe: Path,
    report_out: Path,
    *,
    subprocess_run_patch: Callable[..., Any] | None = None,
) -> None:
    """Invoke ``mod.main()`` with patched argv/env and assert it exits with code 0.

    The wrapper reads a JSON envelope from stdin containing the recipe and secrets.
    We build the envelope from the recipe file and provide it via a mocked stdin.

    Restores ``sys.argv``, ``sys.stdin``, and masking env vars in a ``finally``
    block so that test isolation is guaranteed even when the call raises.
    """
    # Build the stdin envelope in the same format the executor sends
    recipe_yaml = recipe.read_text(encoding="utf-8")
    envelope = json.dumps(
        {
            "__recipe_yaml__": recipe_yaml,
            "__secrets__": {},
            "__report_out_file__": str(report_out),
            "__debug_mode__": "false",
        }
    )

    # Production launches the wrapper out-of-process as
    # [sys.executable, "-m", "datahub.executor.wrappers.run_ingest", <venv>]
    # (see sub_process_ingestion_task._create_subprocess), so this in-process
    # main() call drifts from the real invocation: it does not exercise the -m
    # entry point, and it shares this interpreter's state. That is deliberate —
    # three of the four tests here work by swapping subprocess.run inside
    # wrapper_common to script the `ingest run --help` probe, which is only
    # possible in-process. Under -m we could not steer the probe at all, which
    # is the behaviour these tests exist to pin down. sys.argv[0] mirrors what
    # -m would set (the module's file path); main() only reads it for the usage
    # message.
    argv = [
        mod.__file__ or "run_ingest.py",
        str(_venv_root()),
    ]
    old_argv = sys.argv[:]
    old_stdin = sys.stdin
    old_mask = os.environ.get("DATAHUB_ENABLE_SECRET_MASKING")
    excinfo: Any = None
    try:
        os.environ["DATAHUB_ENABLE_SECRET_MASKING"] = "false"
        sys.argv = argv
        sys.stdin = io.StringIO(envelope)
        if subprocess_run_patch is not None:
            with patch(
                f"{wrapper_common.__name__}.subprocess.run",
                side_effect=subprocess_run_patch,
            ):
                with pytest.raises(SystemExit) as exc:
                    mod.main()
                excinfo = exc
        else:
            with pytest.raises(SystemExit) as exc:
                mod.main()
            excinfo = exc
    finally:
        sys.argv = old_argv
        sys.stdin = old_stdin
        if old_mask is None:
            os.environ.pop("DATAHUB_ENABLE_SECRET_MASKING", None)
        else:
            os.environ["DATAHUB_ENABLE_SECRET_MASKING"] = old_mask

    assert excinfo is not None
    assert excinfo.value.code == 0


def _execution_source_types(server: RecordingGmsServer) -> list[str]:
    """Return all source.type values from dataHubExecutionRequestInput aspects in captured POSTs.

    Handles both GMS endpoints:
    - /aspects?action=ingestProposalBatch  → body has "proposals" (list)
    - /aspects?action=ingestProposal       → body has "proposal" (single object)

    Handles both aspect serialization formats:
    - aspect.json  → nested object (used by seed helper)
    - aspect.value → JSON-encoded string + contentType (used by the DataHub CLI)
    """
    types: list[str] = []
    for raw in server.captured_posts:
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if "proposals" in body:
            proposals = body["proposals"]
        elif "proposal" in body:
            proposals = [body["proposal"]]
        else:
            proposals = []
        for proposal in proposals:
            if proposal.get("aspectName") != "dataHubExecutionRequestInput":
                continue
            aspect = proposal.get("aspect", {})
            # Prefer the nested-object form; fall back to parsing the serialized string form.
            aspect_data: dict[str, Any] = aspect.get("json", {})
            if not aspect_data and aspect.get("contentType") == "application/json":
                try:
                    aspect_data = json.loads(aspect.get("value", "{}"))
                except json.JSONDecodeError:
                    aspect_data = {}
            source_type = aspect_data.get("source", {}).get("type")
            if source_type:
                types.append(source_type)
    return types


@pytest.fixture()
def gms_server() -> Iterator[RecordingGmsServer]:
    """Yield a started RecordingGmsServer; stop it after the test regardless of outcome."""
    with RecordingGmsServer() as server:
        yield server


@pytest.fixture(scope="session")
def run_ingest_mod() -> ModuleType:
    """Return the ingestion wrapper module; skip if the datahub CLI is absent.

    The wrapper used to be a loose script loaded by file path with a fresh
    ``exec_module`` per session, which gave it its own module object. It is now
    a normally imported module, so the whole process shares one instance and
    that isolation is gone. Safe today: ``run_ingest`` holds no module-level
    mutable state. Revisit if it ever gains any.
    """
    datahub = _venv_root() / "bin" / "datahub"
    if not datahub.is_file():
        pytest.skip(
            f"datahub CLI not found at {datahub}; run pytest from the project venv "
            "(e.g. source venv/bin/activate && pytest ...)"
        )
    return run_ingest


@pytest.fixture()
def run_ingestion(
    run_ingest_mod: ModuleType,
) -> Callable[..., tuple[str, Path]]:
    """Return a callable that runs ingestion and returns (blob, report_out).

    Signature: (server, tmp_path, *, subprocess_patch=None, seed_fn=None)
    """

    def _run(
        server: RecordingGmsServer,
        tmp_path: Path,
        *,
        subprocess_patch: Callable[..., Any] | None = None,
        seed_fn: Callable[[str], None] | None = None,
    ) -> tuple[str, Path]:
        metadata = FIXTURES / "metadata_file.json"
        if not metadata.is_file():
            pytest.skip(f"Missing fixture {metadata}")

        if seed_fn is not None:
            seed_fn(server.url)

        recipe = tmp_path / "recipe.yml"
        recipe.write_text(_recipe_text(server.url, metadata), encoding="utf-8")
        report_out = tmp_path / "ingestion_report.json"
        _run_main(
            run_ingest_mod,
            recipe,
            report_out,
            subprocess_run_patch=subprocess_patch,
        )
        blob = b"\n".join(server.captured_posts).decode(errors="replace")
        return blob, report_out

    return _run
