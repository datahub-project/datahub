from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote


@dataclass(frozen=True)
class GitMetadata:
    commit: str
    commit_full: str
    branch: str
    tag: Optional[str]
    dirty: bool


@dataclass(frozen=True)
class DeploymentMetadata:
    mode: str
    gms_url: str
    gms_host: str
    frontend_url: str
    gms_version: Optional[str]
    gms_commit: Optional[str]
    server_type: Optional[str]
    gms_config_error: Optional[str]
    target_name: Optional[str]
    env_file: Optional[str]
    docker_tag: Optional[str]
    datahub_version: Optional[str]
    view_authorization_enabled: Optional[bool] = None
    system_info_error: Optional[str] = None
    system_info_properties: Optional[dict[str, str]] = None


def _run(cmd: list[str], cwd: Optional[Path] = None) -> str:
    return subprocess.check_output(
        cmd, cwd=cwd, stderr=subprocess.DEVNULL, text=True
    ).strip()


def capture_git_metadata(repo_root: Path) -> GitMetadata:
    try:
        commit_full = _run(["git", "rev-parse", "HEAD"], cwd=repo_root)
        commit = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo_root)
        branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_root)
        dirty = _run(["git", "status", "--porcelain"], cwd=repo_root) != ""
        tag = None
        try:
            tag = _run(["git", "describe", "--tags", "--exact-match"], cwd=repo_root)
        except subprocess.CalledProcessError:
            pass
        return GitMetadata(
            commit=commit,
            commit_full=commit_full,
            branch=branch,
            tag=tag,
            dirty=dirty,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return GitMetadata(
            commit="unknown",
            commit_full="unknown",
            branch="unknown",
            tag=None,
            dirty=False,
        )


def capture_deployment_metadata(
    gms_url: str,
    frontend_url: str,
    *,
    gms_host: str,
    gms_version: Optional[str] = None,
    gms_commit: Optional[str] = None,
    server_type: Optional[str] = None,
    gms_config_error: Optional[str] = None,
    target_name: Optional[str] = None,
    env_file: Optional[str] = None,
    docker_tag: Optional[str] = None,
    view_authorization_enabled: Optional[bool] = None,
    system_info_error: Optional[str] = None,
    system_info_properties: Optional[dict[str, str]] = None,
) -> DeploymentMetadata:
    mode = (
        "docker_tag"
        if docker_tag or os.environ.get("DATAHUB_VERSION")
        else "local_rebuild"
    )
    return DeploymentMetadata(
        mode=mode,
        gms_url=gms_url,
        gms_host=gms_host,
        frontend_url=frontend_url,
        gms_version=gms_version,
        gms_commit=gms_commit,
        server_type=server_type,
        gms_config_error=gms_config_error,
        target_name=target_name,
        env_file=env_file,
        docker_tag=docker_tag or os.environ.get("DATAHUB_VERSION"),
        datahub_version=os.environ.get("DATAHUB_VERSION"),
        view_authorization_enabled=view_authorization_enabled,
        system_info_error=system_info_error,
        system_info_properties=system_info_properties,
    )


def capture_fixture_metadata(
    fixture_dir: Path,
    *,
    datapack_dir: Optional[Path],
    pack_index_version: str,
) -> dict[str, Any]:
    personas_path = fixture_dir / "personas.json"
    benchmarks_path = fixture_dir / "benchmarks.json"
    return {
        "fixture": {
            "pack_name": "authz-perf-medium",
            "fixture_dir": str(fixture_dir),
            "datapack_dir": str(datapack_dir) if datapack_dir else None,
            "pack_index_version": pack_index_version,
            "personas_count": len(json.loads(personas_path.read_text())),
            "benchmarks_path": str(benchmarks_path),
        }
    }


def metadata_dict(
    git: GitMetadata,
    deployment: DeploymentMetadata,
    fixture: dict,
    *,
    run_label: Optional[str] = None,
    orchestration_id: Optional[str] = None,
) -> dict:
    harness: dict[str, Any] = {}
    if run_label:
        harness["run_label"] = run_label
    if orchestration_id:
        harness["orchestration_id"] = orchestration_id

    result: dict[str, Any] = {
        "git": {
            "commit": git.commit,
            "commit_full": git.commit_full,
            "branch": git.branch,
            "tag": git.tag,
            "dirty": git.dirty,
        },
        "deployment": {
            "mode": deployment.mode,
            "target_name": deployment.target_name,
            "gms_url": deployment.gms_url,
            "gms_host": deployment.gms_host,
            "frontend_url": deployment.frontend_url,
            "gms_version": deployment.gms_version,
            "gms_commit": deployment.gms_commit,
            "server_type": deployment.server_type,
            "env_file": deployment.env_file,
            "docker_tag": deployment.docker_tag,
            "datahub_version": deployment.datahub_version,
            "authorization": {
                "view_enabled": deployment.view_authorization_enabled,
                **(
                    {"system_info_error": deployment.system_info_error}
                    if deployment.system_info_error
                    else {}
                ),
                **(
                    {"properties": deployment.system_info_properties}
                    if deployment.system_info_properties
                    else {}
                ),
            },
        },
        **fixture,
    }
    if deployment.gms_config_error:
        result["deployment"]["gms_config_error"] = deployment.gms_config_error
    if harness:
        result["harness_meta"] = harness
    return result


def encode_urn_for_openapi(urn: str) -> str:
    return quote(urn, safe="")


@dataclass(frozen=True)
class OpenApiProbeResult:
    ok: bool
    status_code: int
    urn: str
    error_message: Optional[str] = None


def entity_exists_openapi(
    gms_url: str, token: Optional[str], urn: str, *, timeout_sec: float = 30.0
) -> OpenApiProbeResult:
    import requests

    entity_type = urn.split(":")[2]
    encoded = encode_urn_for_openapi(urn)
    url = f"{gms_url.rstrip('/')}/openapi/v3/entity/{entity_type}/{encoded}"
    headers: Dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
    except requests.Timeout as exc:
        return OpenApiProbeResult(
            ok=False, status_code=0, urn=urn, error_message=f"timeout: {exc}"
        )
    except requests.RequestException as exc:
        return OpenApiProbeResult(
            ok=False, status_code=0, urn=urn, error_message=str(exc)
        )

    if resp.status_code == 404:
        return OpenApiProbeResult(ok=True, status_code=404, urn=urn)
    if resp.status_code == 200:
        return OpenApiProbeResult(ok=True, status_code=200, urn=urn)
    return OpenApiProbeResult(
        ok=False,
        status_code=resp.status_code,
        urn=urn,
        error_message=resp.text[:500] if resp.text else f"HTTP {resp.status_code}",
    )
