from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml
from datahub.cli.cli_utils import fixup_gms_url, guess_frontend_url_from_gms_url
from pydantic import ValidationError

try:
    from datahub.cli.config_utils import DatahubConfig
except ImportError:
    DatahubConfig = None  # type: ignore[misc, assignment]


@dataclass(frozen=True)
class TargetSpec:
    name: str
    gms_url: str
    frontend_url: Optional[str] = None
    token: Optional[str] = None
    env_file: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


def _parse_target_kv(raw: str) -> TargetSpec:
    parts: dict[str, str] = {}
    for segment in raw.split(","):
        segment = segment.strip()
        if not segment:
            continue
        if "=" not in segment:
            raise ValueError(f"invalid --target segment (expected key=value): {segment}")
        key, value = segment.split("=", 1)
        parts[key.strip()] = value.strip()

    name = parts.get("name")
    gms_url = parts.get("gms_url")
    if not name:
        raise ValueError("--target requires name=...")
    if not gms_url:
        raise ValueError("--target requires gms_url=...")

    return TargetSpec(
        name=name,
        gms_url=fixup_gms_url(gms_url),
        frontend_url=parts.get("frontend_url"),
        token=parts.get("token"),
        username=parts.get("username"),
        password=parts.get("password"),
    )


def _default_name_from_env_path(path: Path) -> str:
    stem = path.stem
    if stem.startswith(".datahubenv_"):
        return stem[len(".datahubenv_") :]
    if stem.startswith(".datahubenv"):
        suffix = stem[len(".datahubenv") :].lstrip("_")
        if suffix:
            return suffix
    slug = re.sub(r"[^\w.-]+", "-", stem).strip("-")
    return slug or "target"


def load_target_from_env_file(
    path: Path,
    *,
    name_override: Optional[str] = None,
) -> TargetSpec:
    if DatahubConfig is None:
        raise RuntimeError("datahub SDK required to load --env-file targets")

    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"env file not found: {resolved}")

    raw = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"env file must be a YAML mapping: {resolved}")

    try:
        config = DatahubConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid env file {resolved}: {exc}") from exc

    server = config.gms.server
    if not server:
        raise ValueError(f"env file missing gms.server: {resolved}")

    token = config.gms.token
    if token is not None and not str(token).strip():
        token = None

    default_name = _default_name_from_env_path(resolved)
    return TargetSpec(
        name=name_override or default_name,
        gms_url=fixup_gms_url(str(server)),
        frontend_url=None,
        token=str(token) if token else None,
        env_file=str(resolved),
    )


def resolve_frontend_url(target: TargetSpec) -> str:
    if target.frontend_url:
        return target.frontend_url
    return guess_frontend_url_from_gms_url(target.gms_url)


def parse_targets_from_args(
    *,
    env_files: list[str],
    env_file_names: list[str],
    target_strings: list[str],
    gms_url: Optional[str],
    frontend_url: Optional[str],
) -> list[TargetSpec]:
    targets: list[TargetSpec] = []

    for index, env_path in enumerate(env_files):
        name_override = env_file_names[index] if index < len(env_file_names) else None
        targets.append(
            load_target_from_env_file(Path(env_path), name_override=name_override)
        )

    for raw in target_strings:
        spec = _parse_target_kv(raw)
        if frontend_url and not spec.frontend_url:
            spec = TargetSpec(
                name=spec.name,
                gms_url=spec.gms_url,
                frontend_url=frontend_url,
                token=spec.token,
                env_file=spec.env_file,
                username=spec.username,
                password=spec.password,
            )
        targets.append(spec)

    if not targets and gms_url:
        targets.append(
            TargetSpec(
                name="local",
                gms_url=fixup_gms_url(gms_url),
                frontend_url=frontend_url,
            )
        )

    return targets


def validate_target_list(targets: list[TargetSpec], *, multi_mode: bool) -> None:
    if not targets:
        raise SystemExit(
            "No targets specified. Use --env-file, --target, or --gms-url "
            "(with --output or --output-dir)."
        )

    names = [t.name for t in targets]
    if len(names) != len(set(names)):
        raise SystemExit("Duplicate target names; use --env-file-name to disambiguate.")

    if multi_mode and len(targets) == 1 and targets[0].name == "local":
        pass  # bisect / single local remote with --output-dir
