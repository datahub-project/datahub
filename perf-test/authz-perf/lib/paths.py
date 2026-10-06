from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

PACK_NAME = "authz-perf-medium"
REGISTRY_INDEX_URL = (
    "https://raw.githubusercontent.com/datahub-project/static-assets/main/"
    "datapacks/authz-perf-medium/index.json"
)


def repo_root_from_harness(harness_root: Path) -> Path:
    return harness_root.resolve().parent.parent


def default_fixture_dir(harness_root: Path) -> Path:
    return harness_root / "fixture"


def default_datapack_dir(harness_root: Path) -> Path:
    sibling = (
        repo_root_from_harness(harness_root).parent
        / "static-assets"
        / "datapacks"
        / PACK_NAME
    )
    if (sibling / "index.json").is_file():
        return sibling.resolve()
    return sibling.resolve()


def resolve_datapack_dir(
    harness_root: Path, override: Optional[Path] = None
) -> Optional[Path]:
    if override is not None:
        path = override.resolve()
        if not (path / "index.json").is_file():
            raise FileNotFoundError(f"Missing datapack index at {path / 'index.json'}")
        return path

    env = os.environ.get("AUTHZ_PERF_DATAPACK_DIR")
    if env:
        path = Path(env).resolve()
        if not (path / "index.json").is_file():
            raise FileNotFoundError(
                f"AUTHZ_PERF_DATAPACK_DIR missing index.json: {path / 'index.json'}"
            )
        return path

    sibling = default_datapack_dir(harness_root)
    if (sibling / "index.json").is_file():
        return sibling
    return None


def read_pack_index_version(datapack_dir: Optional[Path] = None) -> str:
    if datapack_dir is not None and (datapack_dir / "index.json").is_file():
        index = json.loads((datapack_dir / "index.json").read_text(encoding="utf-8"))
        return str(index.get("version", ""))

    try:
        with urllib.request.urlopen(REGISTRY_INDEX_URL, timeout=30) as response:
            index = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise FileNotFoundError(
            "Could not resolve authz-perf-medium pack version. "
            "Check out static-assets alongside datahub, set AUTHZ_PERF_DATAPACK_DIR, "
            f"or ensure {REGISTRY_INDEX_URL} is reachable."
        ) from exc
    return str(index.get("version", ""))
