from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

from lib.graphql import log
from lib.metadata import entity_exists_openapi
from lib.results import utc_now_iso

INSTANCE_STATE_PATH = (
    Path.home() / ".datahub" / "authz-perf-results" / "instance-state.json"
)
PACK_NAME = "authz-perf-medium"

SENTINEL_CORPUSER = "urn:li:corpuser:persona-admin"
SENTINEL_DOMAIN = "urn:li:domain:internal-domain-0001"


class LoadAction(str, Enum):
    LOAD = "load"
    SKIP = "skip"
    WARN_SKIP = "warn_skip"


@dataclass(frozen=True)
class LoadDecision:
    action: LoadAction
    message: str = ""


def _read_instance_state() -> dict:
    if not INSTANCE_STATE_PATH.exists():
        return {}
    return json.loads(INSTANCE_STATE_PATH.read_text(encoding="utf-8"))


def _write_instance_state(state: dict) -> None:
    INSTANCE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    INSTANCE_STATE_PATH.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def get_loaded_version(gms_url: str) -> Optional[str]:
    record = _read_instance_state().get(gms_url)
    if record is None:
        return None
    return str(record.get("index_version", "")) or None


def resolve_load_action(
    gms_url: str,
    target_version: str,
    *,
    force_reload: bool = False,
    skip_load: bool = False,
    allow_downgrade: bool = False,
    sentinel_present: bool = False,
) -> LoadDecision:
    if skip_load:
        return LoadDecision(LoadAction.SKIP, "skip_load requested")
    if force_reload:
        return LoadDecision(LoadAction.LOAD, "force_reload requested")

    loaded = get_loaded_version(gms_url)
    if loaded is None:
        if sentinel_present:
            return LoadDecision(LoadAction.SKIP, "sentinel present without load record")
        return LoadDecision(LoadAction.LOAD, "no sentinel on instance")

    if loaded == target_version:
        return LoadDecision(LoadAction.SKIP, f"same version {target_version}")

    try:
        loaded_i = int(loaded)
        target_i = int(target_version)
    except ValueError:
        if loaded == target_version:
            return LoadDecision(LoadAction.SKIP, "same version (string compare)")
        return LoadDecision(LoadAction.LOAD, "non-numeric version mismatch")

    if target_i > loaded_i:
        return LoadDecision(LoadAction.LOAD, f"forward upgrade {loaded} -> {target_i}")

    msg = (
        f"non-chronological pack version (loaded={loaded}, target={target_version}). "
        "Reload skipped. Use --force-reload after datapack unload."
    )
    if allow_downgrade:
        return LoadDecision(LoadAction.WARN_SKIP, msg)
    return LoadDecision(LoadAction.WARN_SKIP, msg)


def record_load(
    gms_url: str,
    index_version: str,
    *,
    gms_version: Optional[str] = None,
    gms_commit: Optional[str] = None,
) -> None:
    state = _read_instance_state()
    state[gms_url] = {
        "pack_name": PACK_NAME,
        "index_version": index_version,
        "loaded_at": utc_now_iso(),
        "gms_version": gms_version,
        "gms_commit": gms_commit,
    }
    _write_instance_state(state)


def probe_pack_loaded(gms_url: str, token: Optional[str]) -> bool:
    for urn in (SENTINEL_CORPUSER, SENTINEL_DOMAIN):
        probe = entity_exists_openapi(gms_url, token, urn)
        if not probe.ok:
            raise RuntimeError(
                f"Datapack sentinel probe failed for {probe.urn}: "
                f"HTTP {probe.status_code} — {probe.error_message}"
            )
        if probe.status_code == 200:
            return True
    return False


def load_pack(
    *,
    gms_url: str,
    token: Optional[str],
    datapack_dir: Optional[Path] = None,
) -> None:
    cmd = ["datahub", "datapack", "load", PACK_NAME]
    if datapack_dir is not None:
        index_file = datapack_dir / "index.json"
        if not index_file.exists():
            raise FileNotFoundError(f"Missing pack index at {index_file}")
        cmd.extend(
            [
                "--url",
                f"file://{index_file.resolve()}",
                "--trust-custom",
            ]
        )
    env = os.environ.copy()
    env["DATAHUB_GMS_URL"] = gms_url
    if token:
        env["DATAHUB_GMS_TOKEN"] = token
    log(f"loading datapack: {' '.join(cmd)}")
    subprocess.check_call(cmd, env=env)


def ensure_datapack(
    *,
    gms_url: str,
    datapack_dir: Optional[Path],
    target_version: str,
    token: Optional[str],
    force_reload: bool = False,
    skip_load: bool = False,
    allow_downgrade: bool = False,
    gms_version: Optional[str] = None,
    gms_commit: Optional[str] = None,
) -> LoadAction:
    sentinel = probe_pack_loaded(gms_url, token)
    decision = resolve_load_action(
        gms_url,
        target_version,
        force_reload=force_reload,
        skip_load=skip_load,
        allow_downgrade=allow_downgrade,
        sentinel_present=sentinel,
    )
    if decision.message:
        if decision.action == LoadAction.WARN_SKIP:
            log(f"WARN: {decision.message}")
        else:
            log(decision.message)

    if decision.action == LoadAction.LOAD:
        load_pack(gms_url=gms_url, token=token, datapack_dir=datapack_dir)
        record_load(
            gms_url,
            target_version,
            gms_version=gms_version,
            gms_commit=gms_commit,
        )
    elif not sentinel and not skip_load:
        raise RuntimeError(
            "Datapack sentinel probe failed and load was skipped. "
            "Use --force-reload or load authz-perf-medium manually."
        )
    return decision.action
