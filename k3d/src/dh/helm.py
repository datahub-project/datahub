"""Helm chart source abstraction and repo management."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dh.utils import info, run


@dataclass(frozen=True)
class HelmChartSource:
    """Where to find a Helm chart — either a repo version or a local path.

    Mutually exclusive: at most one of ``version`` or ``path`` may be set.
    """

    version: Optional[str] = None
    path: Optional[Path] = None

    def __post_init__(self) -> None:
        if self.version and self.path:
            from dh.utils import die

            die("--chart-version and --chart-path are mutually exclusive.")

    def chart_ref(self, repo_name: str, chart_name: str) -> str:
        """Return the chart reference for ``helm upgrade --install``."""
        if self.path:
            return str(self.path)
        return f"{repo_name}/{chart_name}"

    def version_args(self) -> list[str]:
        """Return ``['--version', ver]`` or ``[]``."""
        if self.version:
            return ["--version", self.version]
        return []


def ensure_helm_repo(repo_name: str, repo_url: str) -> None:
    """Add the Helm repo if missing, then update it."""
    result = run(["helm", "repo", "list", "-o", "json"], capture_output=True, check=False)
    if result.returncode != 0 or repo_name not in result.stdout:
        info(f"Adding Helm repo '{repo_name}'...")
        run(["helm", "repo", "add", repo_name, repo_url])
    run(["helm", "repo", "update", repo_name], quiet=True)
