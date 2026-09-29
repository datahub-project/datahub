"""End-to-end tests for ODCS remote sourcing (S3, GCS, HTTP, Git).

Unlike the unit tests (which mock the object-store reader), these drive the real
code paths against real infrastructure: a moto-backed S3/GCS object store, a live
threaded HTTP server, and an on-disk git repository cloned over `file://`. They
prove the sourcing actually works and guard against regressions.
"""

import functools
import json
import pathlib
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List, Set, Tuple

import boto3
import git
import pytest
from moto import mock_aws

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.odcs.odcs_source import ODCSSourceReport

_CONTRACT_TEMPLATE = """\
apiVersion: v3.1.0
kind: DataContract
id: {contract_id}
version: 1.0.0
status: active
servers:
  - server: prod-snowflake
    type: snowflake
    account: acme-prod
    database: ANALYTICS
    schema: PUBLIC
schema:
  - name: my_table
    physicalName: my_table
    properties:
      - name: id
        physicalName: id
        logicalType: number
        primaryKey: true
"""

_ID_A = "00000000-0000-0000-0000-0000000000aa"
_ID_B = "00000000-0000-0000-0000-0000000000bb"
_CONTRACT_A = _CONTRACT_TEMPLATE.format(contract_id=_ID_A)
_CONTRACT_B = _CONTRACT_TEMPLATE.format(contract_id=_ID_B)

_BUCKET = "odcs-contracts"
_AWS_CREDS = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "aws_region": "us-east-1",
}


def _run(
    config: Dict[str, Any], tmp_path: pathlib.Path, name: str
) -> Tuple[List[Dict[str, Any]], ODCSSourceReport]:
    output_path = tmp_path / f"{name}.json"
    pipeline = Pipeline.create(
        {
            "run_id": f"test-odcs-remote-{name}",
            "source": {"type": "odcs", "config": config},
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    report = pipeline.source.get_report()
    assert isinstance(report, ODCSSourceReport)
    mces = json.loads(output_path.read_text())
    return mces, report


def _emitted_contract_ids(mces: List[Dict[str, Any]]) -> Set[str]:
    ids: Set[str] = set()
    for mce in mces:
        if mce.get("aspectName") != "datasetProperties":
            continue
        props = mce["aspect"]["json"].get("customProperties", {})
        if "odcs.id" in props:
            ids.add(props["odcs.id"])
    return ids


def _put(client: Any, key: str, body: str) -> None:
    client.put_object(Bucket=_BUCKET, Key=key, Body=body.encode("utf-8"))


# ---------------------------------------------------------------------------
# S3 (moto)
# ---------------------------------------------------------------------------


def test_s3_single_file(tmp_path: pathlib.Path) -> None:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        _put(client, "contracts/a.odcs.yaml", _CONTRACT_A)

        mces, report = _run(
            {
                "path": f"s3://{_BUCKET}/contracts/a.odcs.yaml",
                "aws_connection": _AWS_CREDS,
            },
            tmp_path,
            "s3_single",
        )

    assert report.remote_files_scanned == 1
    assert report.contracts_parsed == 1
    assert _emitted_contract_ids(mces) == {_ID_A}


def test_s3_glob_expands_and_ingests_all(tmp_path: pathlib.Path) -> None:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        _put(client, "contracts/a.odcs.yaml", _CONTRACT_A)
        _put(client, "contracts/b.odcs.yaml", _CONTRACT_B)
        # A non-matching extension under the same prefix must be filtered out.
        _put(client, "contracts/readme.txt", "not a contract")

        mces, report = _run(
            {
                "path": f"s3://{_BUCKET}/contracts/*.odcs.yaml",
                "aws_connection": _AWS_CREDS,
            },
            tmp_path,
            "s3_glob",
        )

    assert report.remote_files_scanned == 2
    assert report.contracts_parsed == 2
    assert _emitted_contract_ids(mces) == {_ID_A, _ID_B}


# ---------------------------------------------------------------------------
# GCS (moto, via the S3-compatible client — endpoint pointed at moto's AWS mock)
# ---------------------------------------------------------------------------


def test_gcs_single_file(tmp_path: pathlib.Path) -> None:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        _put(client, "contracts/a.odcs.yaml", _CONTRACT_A)

        mces, report = _run(
            {
                "path": f"gs://{_BUCKET}/contracts/a.odcs.yaml",
                "gcs_connection": {
                    "credential": {
                        "hmac_access_id": "test",
                        "hmac_access_secret": "test",
                    },
                    # Point the S3-compatible client at moto's AWS mock instead
                    # of the real GCS endpoint so the read is hermetic.
                    "endpoint_url": "https://s3.us-east-1.amazonaws.com",
                },
            },
            tmp_path,
            "gcs_single",
        )

    assert report.remote_files_scanned == 1
    assert report.contracts_parsed == 1
    assert _emitted_contract_ids(mces) == {_ID_A}


# ---------------------------------------------------------------------------
# HTTP (real threaded server)
# ---------------------------------------------------------------------------


@pytest.fixture
def http_server(tmp_path: pathlib.Path) -> Iterator[Tuple[str, pathlib.Path]]:
    serve_dir = tmp_path / "http_root"
    serve_dir.mkdir()
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(serve_dir))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        yield f"http://127.0.0.1:{port}", serve_dir
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_http_single_file(
    tmp_path: pathlib.Path, http_server: Tuple[str, pathlib.Path]
) -> None:
    base_url, serve_dir = http_server
    (serve_dir / "a.odcs.yaml").write_text(_CONTRACT_A, encoding="utf-8")

    mces, report = _run(
        {"path": f"{base_url}/a.odcs.yaml"},
        tmp_path,
        "http_single",
    )

    assert report.remote_files_scanned == 1
    assert report.contracts_parsed == 1
    assert _emitted_contract_ids(mces) == {_ID_A}


# ---------------------------------------------------------------------------
# Git (real local repo cloned over file://)
# ---------------------------------------------------------------------------


def test_git_clone_and_walk(tmp_path: pathlib.Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    repo = git.Repo.init(repo_dir)
    with repo.config_writer() as cw:
        cw.set_value("user", "email", "test@example.com")
        cw.set_value("user", "name", "Test")
    contracts_dir = repo_dir / "contracts"
    contracts_dir.mkdir()
    (contracts_dir / "a.odcs.yaml").write_text(_CONTRACT_A, encoding="utf-8")
    (contracts_dir / "b.odcs.yaml").write_text(_CONTRACT_B, encoding="utf-8")
    repo.index.add(["contracts/a.odcs.yaml", "contracts/b.odcs.yaml"])
    repo.index.commit("add contracts")

    mces, report = _run(
        {
            "path": "contracts",
            "git_info": {
                # repo is only used to infer view-source URLs; the actual clone
                # source is the local bare-less repo via repo_ssh_locator.
                "repo": "https://github.com/acme/contracts",
                "repo_ssh_locator": f"file://{repo_dir}",
            },
        },
        tmp_path,
        "git_walk",
    )

    assert report.git_checkout is not None
    assert report.contracts_parsed == 2
    assert _emitted_contract_ids(mces) == {_ID_A, _ID_B}
