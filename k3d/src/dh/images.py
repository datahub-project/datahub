"""Build and import local Docker images into k3d.

Delegates all image building to Gradle's per-module :docker task, which handles
artifact assembly, Docker buildx build, layer caching, and up-to-date checks.

When --local is used, auto-detects which services have modifications in the
current branch (vs main) and only builds those. Unmodified services continue
to use published images from the registry.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import click

from dh.config import K3dConfig
from dh.naming import derive_worktree_name, git_root, namespace_for
from dh.utils import die, info, ok, require_cmd, run, warn


# ── Service registry ──────────────────────────────────────────────────────
# Each service maps to its Gradle module, Docker image, Helm chart keys,
# and the source paths that trigger a rebuild when changed.

# Service → Gradle module path (the :docker task handles the full build chain)
SERVICE_DOCKER_MODULES: dict[str, str] = {
    "gms": ":metadata-service:war",
    "frontend": ":datahub-frontend",
    "upgrade": ":datahub-upgrade",
    "mysql-setup": ":docker:mysql-setup",
    "elasticsearch-setup": ":docker:elasticsearch-setup",
}

# Service → Docker image name (registry/repo, without tag)
SERVICE_IMAGES: dict[str, str] = {
    "gms": "acryldata/datahub-gms",
    "frontend": "acryldata/datahub-frontend-react",
    "upgrade": "acryldata/datahub-upgrade",
    "mysql-setup": "acryldata/datahub-mysql-setup",
    "elasticsearch-setup": "acryldata/datahub-elasticsearch-setup",
}

# Service → Helm chart keys to override when using local images.
# Each entry maps helm value paths for image.tag and image.pullPolicy.
SERVICE_HELM_OVERRIDES: dict[str, list[str]] = {
    "gms": [
        "datahub-gms.image",
    ],
    "frontend": [
        "datahub-frontend.image",
    ],
    "upgrade": [
        # datahubSystemUpdate and datahubUpgrade share the same image
        "datahubSystemUpdate.image",
        "datahubUpgrade.image",
    ],
    "mysql-setup": [
        "mysqlSetupJob.image",
    ],
    "elasticsearch-setup": [
        "elasticsearchSetupJob.image",
    ],
}

# Source paths that trigger a rebuild for each service.
# Includes direct source directories and shared libraries the service depends on.
# Changes to gradle/ or buildSrc/ trigger rebuilds for ALL services.
_GLOBAL_TRIGGER_PATHS: list[str] = [
    "gradle/",
    "buildSrc/",
]

SERVICE_SOURCE_PATHS: dict[str, list[str]] = {
    "gms": [
        "metadata-service/",
        "metadata-io/",
        "metadata-models/",
        "entity-registry/",
        "metadata-utils/",
        "metadata-events/",
        "metadata-auth/",
        "metadata-jobs/",
        "datahub-graphql-core/",
        "li-utils/",
        "docker/datahub-gms/",
    ],
    "frontend": [
        "datahub-frontend/",
        "docker/datahub-frontend/",
    ],
    "upgrade": [
        "datahub-upgrade/",
        "metadata-models/",
        "metadata-io/",
        "entity-registry/",
        "metadata-utils/",
        "metadata-events/",
        "li-utils/",
        "docker/datahub-upgrade/",
    ],
    "mysql-setup": [
        "docker/mysql-setup/",
    ],
    "elasticsearch-setup": [
        "docker/elasticsearch-setup/",
    ],
}

# Service → Helm deployment suffix (only long-running deployments, not jobs).
# Full deployment name: {release_name}-{suffix}
SERVICE_DEPLOYMENTS: dict[str, str] = {
    "gms": "datahub-gms",
    "frontend": "datahub-frontend",
}


# ── Public API ────────────────────────────────────────────────────────────


def detect_modified_services(root: str) -> list[str]:
    """Detect which services have modifications in the current branch.

    Compares the current HEAD against the merge-base with main to find changed
    files, then maps them to affected services using SERVICE_SOURCE_PATHS.
    Also includes uncommitted working-tree changes.
    """
    # Find merge base with the default branch (try main, then master)
    merge_base = None
    for branch in ("main", "master"):
        result = run(
            ["git", "merge-base", "HEAD", branch],
            capture_output=True,
            check=False,
            cwd=root,
        )
        if result.returncode == 0:
            merge_base = result
            break
    if merge_base is None:
        warn("Cannot determine merge-base — building all services.")
        return list(SERVICE_DOCKER_MODULES.keys())

    base_ref = merge_base.stdout.strip()

    # Committed changes since merge-base
    committed = run(
        ["git", "diff", "--name-only", base_ref, "HEAD"],
        capture_output=True,
        cwd=root,
    ).stdout.strip().splitlines()

    # Uncommitted changes (staged + unstaged)
    uncommitted = run(
        ["git", "diff", "--name-only", "HEAD"],
        capture_output=True,
        cwd=root,
    ).stdout.strip().splitlines()

    all_changed = set(f for f in committed + uncommitted if f)
    if not all_changed:
        info("No file changes detected in branch.")
        return []

    # Check for global triggers (gradle/, buildSrc/)
    global_triggered = any(
        f.startswith(tuple(_GLOBAL_TRIGGER_PATHS)) for f in all_changed
    )

    modified: list[str] = []
    for svc, paths in SERVICE_SOURCE_PATHS.items():
        if global_triggered or any(
            f.startswith(tuple(paths)) for f in all_changed
        ):
            modified.append(svc)

    return modified


def build_and_import_modified(
    cfg: K3dConfig, name: str,
) -> list[str]:
    """Auto-detect modified services, build, retag, and import into k3d.

    Returns the list of services that were built.
    """
    require_cmd("docker")
    require_cmd("k3d")
    root = git_root()

    # Allow explicit override via env var (default: gms,frontend)
    env_services = os.environ.get("DH_LOCAL_SERVICES", "gms,frontend")
    if env_services:
        services = [s.strip() for s in env_services.split(",") if s.strip()]
        info(f"Using DH_LOCAL_SERVICES: {', '.join(services)}")
    else:
        services = detect_modified_services(root)

    if not services:
        warn("No modified services detected. Deploy will use published images.")
        return []

    info(f"Modified services: {', '.join(services)}")

    # Build all modified services in a single Gradle invocation for efficiency.
    # Gradle's up-to-date checks and BuildKit layer cache make this fast
    # for services with no actual source changes (transitive dep detection
    # was conservative).
    tasks = []
    for svc in services:
        module = SERVICE_DOCKER_MODULES.get(svc)
        if not module:
            known = ", ".join(SERVICE_DOCKER_MODULES)
            die(f"Unknown service '{svc}'. Known services: {known}")
        tasks.append(f"{module}:docker")

    info(f"Building images via Gradle ({len(tasks)} tasks)...")
    run(
        ["./gradlew", *tasks],
        cwd=root,
    )

    # Retag and import each built image
    local_tag = f"local-{name}"
    version_tag = _get_version_tag(root)

    for svc in services:
        _retag_and_import(cfg, svc, version_tag, local_tag)

    return services


def helm_set_args_for_local(services: list[str], local_tag: str) -> list[str]:
    """Generate Helm --set args to override tag and pullPolicy for built services."""
    args: list[str] = []
    for svc in services:
        overrides = SERVICE_HELM_OVERRIDES.get(svc, [])
        for image_path in overrides:
            args.extend(["--set", f"{image_path}.tag={local_tag}"])
            args.extend(["--set", f"{image_path}.pullPolicy=Never"])
    return args


def import_specific(cfg: K3dConfig, services: str) -> None:
    """Build and import specific service images (for import-images command)."""
    require_cmd("docker")
    require_cmd("k3d")
    root = git_root()
    name = derive_worktree_name()
    local_tag = f"local-{name}"
    version_tag = _get_version_tag(root)

    svc_list = [s.strip() for s in services.split(",") if s.strip()]

    # Build via Gradle
    tasks = []
    for svc in svc_list:
        module = SERVICE_DOCKER_MODULES.get(svc)
        if not module:
            known = ", ".join(SERVICE_DOCKER_MODULES)
            die(f"Unknown service '{svc}'. Known services: {known}")
        tasks.append(f"{module}:docker")

    info(f"Building images via Gradle ({len(tasks)} tasks)...")
    run(
        ["./gradlew", *tasks],
        cwd=root,
    )

    for svc in svc_list:
        _retag_and_import(cfg, svc, version_tag, local_tag)

    ok("Images imported. Restart pods to pick them up:")
    ns = namespace_for(name)
    click.echo(f"  kubectl rollout restart deployment -n {ns}")


# ── Internal helpers ──────────────────────────────────────────────────────


def _get_version_tag(root: str) -> str:
    """Read versionTag from build/version.json, or return default.

    The Gradle versioning plugin writes this file during generateGitPropertiesGlobal.
    If it exists from a previous build, we use its versionTag to know how Gradle
    tagged the Docker images.
    """
    version_file = Path(root) / "build" / "version.json"
    if version_file.is_file():
        data = json.loads(version_file.read_text())
        return data.get("versionTag", "v0.0.0-unknown-SNAPSHOT")
    return "v0.0.0-unknown-SNAPSHOT"


def _retag_and_import(
    cfg: K3dConfig, svc: str, version_tag: str, local_tag: str,
) -> None:
    """Retag a Gradle-built image with our local tag and import into k3d."""
    image = SERVICE_IMAGES.get(svc)
    if not image:
        known = ", ".join(SERVICE_IMAGES)
        die(f"Unknown service '{svc}'. Known services: {known}")

    source = f"{image}:{version_tag}"
    target = f"{image}:{local_tag}"

    info(f"Retagging {source} -> {target}")
    run(["docker", "tag", source, target])

    info(f"Importing '{target}' into k3d cluster '{cfg.cluster_name}'...")
    run(["k3d", "image", "import", target, "--cluster", cfg.cluster_name])

    ok(f"Image '{target}' ready in cluster.")
