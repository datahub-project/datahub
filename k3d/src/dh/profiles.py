"""Deployment profile resolution — maps profile names to values files and --set args."""

from __future__ import annotations

from pathlib import Path

PROFILES = ("minimal", "consumers", "backend")


def resolve_profiles(
    profiles: tuple[str, ...],
    profiles_dir: Path,
) -> tuple[list[Path], list[str]]:
    """Validate profile names and return (values_files, set_args) to apply.

    Values files are returned in profile order; set_args likewise.
    """
    values_files: list[Path] = []
    set_args: list[str] = []

    for name in profiles:
        name = name.lower()
        vf = profiles_dir / f"{name}.yaml"
        if not vf.is_file():
            raise FileNotFoundError(f"Profile values file not found: {vf}")
        values_files.append(vf)
        set_args.extend(_profile_set_args(name))

    return values_files, set_args


def _profile_set_args(profile: str) -> list[str]:
    """Return Helm --set flags for array element patches.

    GMS extraEnvs index allocation (from datahub-worktree.yaml.tpl):
        0: MAE_CONSUMER_ENABLED
        1: MCE_CONSUMER_ENABLED
        2: PE_CONSUMER_ENABLED
        3: UI_INGESTION_ENABLED
        4: ENTITY_SERVICE_ENABLE_RETENTION
        5: ES_BULK_REFRESH_POLICY
        6: JAVA_OPTS
        7: DATAHUB_SYSTEM_CLIENT_ID
        8: DATAHUB_SYSTEM_CLIENT_SECRET

    Profiles only modify existing indices 0-8; no two profiles touch the same index.
        minimal  → [3]   (UI_INGESTION_ENABLED=false)
        consumers → [0-2] (MAE/MCE/PE_CONSUMER_ENABLED=false)
        backend  → (none — YAML-only)
    """
    sets: list[str] = []

    if profile == "minimal":
        sets.append("datahub-gms.extraEnvs[3].value=false")
    elif profile == "consumers":
        # Disable embedded consumers in GMS (standalone consumers take over)
        sets.append("datahub-gms.extraEnvs[0].value=false")
        sets.append("datahub-gms.extraEnvs[1].value=false")
        sets.append("datahub-gms.extraEnvs[2].value=false")

    result: list[str] = []
    for s in sets:
        result.extend(["--set", s])
    return result
