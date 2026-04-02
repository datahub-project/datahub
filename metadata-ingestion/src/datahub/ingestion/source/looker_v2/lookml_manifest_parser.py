"""
Manifest Parser for Looker V2 Source.

Parses manifest.lkml files to extract project dependencies and constants.
"""

from __future__ import annotations

import logging
import pathlib
import tempfile
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Set

import lkml
import lkml.simple
from pydantic import SecretStr

from datahub.ingestion.source.git.git_import import GitClone

logger = logging.getLogger(__name__)


class _DeployKeyProvider(Protocol):
    """Structural protocol for any config that provides a deploy key."""

    deploy_key: Optional[SecretStr]


# Patch lkml to support manifest.lkml plural keys
if not hasattr(lkml.simple, "_LOOKER_V2_PATCHED"):
    lkml.simple.PLURAL_KEYS = (
        *lkml.simple.PLURAL_KEYS,
        "local_dependency",
        "remote_dependency",
        "constant",
        "override_constant",
    )
    lkml.simple._LOOKER_V2_PATCHED = True  # type: ignore


@dataclass
class LookerConstant:
    """A LookML constant from manifest.lkml."""

    name: str
    value: str


@dataclass
class LookerRemoteDependency:
    """A remote dependency from manifest.lkml."""

    name: str
    url: str
    ref: Optional[str] = None


@dataclass
class LookerManifest:
    """Parsed content from a manifest.lkml file."""

    project_name: Optional[str] = None
    constants: Dict[str, LookerConstant] = field(default_factory=dict)
    local_dependencies: List[str] = field(default_factory=list)
    remote_dependencies: List[LookerRemoteDependency] = field(default_factory=list)


class ManifestParser:
    """
    Parses manifest.lkml files for LookML project configuration.

    Handles:
    - Project name detection
    - LookML constants extraction
    - Local dependency resolution
    - Remote dependency resolution (with git cloning)
    """

    def __init__(
        self,
        base_folder: str,
        project_name: Optional[str] = None,
        project_dependencies: Optional[Dict[str, str]] = None,
        git_info: Optional[_DeployKeyProvider] = None,
    ):
        """
        Initialize the manifest parser.

        Args:
            base_folder: Path to the main LookML project
            project_name: Configured project name (overrides manifest)
            project_dependencies: Pre-configured project dependencies
            git_info: Git info for cloning remote dependencies
        """
        self.base_folder = pathlib.Path(base_folder)
        self.project_name = project_name
        self.project_dependencies = project_dependencies or {}
        self.git_info = git_info

        # Results
        self.resolved_projects: Dict[str, pathlib.Path] = {}
        self.constants: Dict[str, LookerConstant] = {}

        # Temp directories for cloned repos
        self._temp_dirs: List[str] = []

    def parse_and_resolve(self) -> Dict[str, pathlib.Path]:
        """
        Parse manifest and resolve all dependencies.

        Returns:
            Dictionary mapping project name to local path
        """
        # Start with pre-configured dependencies
        for name, path in self.project_dependencies.items():
            self.resolved_projects[name] = pathlib.Path(path)

        # Add main project
        main_name = self.project_name or "_base_"
        self.resolved_projects[main_name] = self.base_folder

        # Recursively process manifests
        visited: Set[str] = set()
        self._process_manifest_recursive(main_name, visited)

        return self.resolved_projects

    def _process_manifest_recursive(self, project_name: str, visited: Set[str]) -> None:
        """Recursively process manifests for a project and its dependencies."""
        if project_name in visited:
            return
        visited.add(project_name)

        project_path = self.resolved_projects.get(project_name)
        if not project_path or not project_path.exists():
            logger.warning(f"Project path not found for '{project_name}'")
            return

        manifest = self._parse_manifest(project_path)
        if not manifest:
            return

        # Extract constants
        for name, constant in manifest.constants.items():
            if name not in self.constants:
                self.constants[name] = constant

        # Update project name from manifest if not set
        if project_name == "_base_" and manifest.project_name:
            # Re-register with actual name
            del self.resolved_projects["_base_"]
            self.resolved_projects[manifest.project_name] = project_path
            self.project_name = manifest.project_name

        # Process remote dependencies
        for remote in manifest.remote_dependencies:
            if remote.name in visited or remote.name in self.resolved_projects:
                continue

            cloned_path = self._clone_remote_dependency(remote)
            if cloned_path:
                self.resolved_projects[remote.name] = cloned_path
                self._process_manifest_recursive(remote.name, visited)

        # Process local dependencies
        for local_name in manifest.local_dependencies:
            if local_name not in self.resolved_projects:
                logger.warning(
                    f"Local dependency '{local_name}' not found in project_dependencies config"
                )
                continue
            self._process_manifest_recursive(local_name, visited)

    def _parse_manifest(self, project_path: pathlib.Path) -> Optional[LookerManifest]:
        """Parse a manifest.lkml file."""
        manifest_file = project_path / "manifest.lkml"

        if not manifest_file.exists():
            logger.debug(f"No manifest.lkml found in {project_path}")
            return None

        try:
            with open(manifest_file, "r", encoding="utf-8") as f:
                content = f.read()

            parsed = lkml.load(content)

            manifest = LookerManifest(
                project_name=parsed.get("project_name"),
            )

            # Parse constants
            for const in parsed.get("constants", []):
                if const.get("name") and const.get("value"):
                    manifest.constants[const["name"]] = LookerConstant(
                        name=const["name"],
                        value=const["value"],
                    )

            # Parse local dependencies (note: lkml pluralizes as 'local_dependencys')
            for dep in parsed.get("local_dependencys", []):
                if dep.get("project"):
                    manifest.local_dependencies.append(dep["project"])

            # Parse remote dependencies (note: lkml pluralizes as 'remote_dependencys')
            for dep in parsed.get("remote_dependencys", []):
                if dep.get("name") and dep.get("url"):
                    manifest.remote_dependencies.append(
                        LookerRemoteDependency(
                            name=dep["name"],
                            url=dep["url"],
                            ref=dep.get("ref"),
                        )
                    )

            return manifest

        except (OSError, ValueError, KeyError) as e:
            logger.warning(f"Failed to parse manifest {manifest_file}: {e}")
            return None

    def _clone_remote_dependency(
        self, remote: LookerRemoteDependency
    ) -> Optional[pathlib.Path]:
        """Clone a remote dependency."""
        try:
            temp_dir = tempfile.mkdtemp(prefix=f"looker_v2_remote_{remote.name}_")
            self._temp_dirs.append(temp_dir)

            git_clone = GitClone(temp_dir)

            # Get SSH key if available
            ssh_key: Optional[SecretStr] = None
            if self.git_info and self.git_info.deploy_key:
                ssh_key = self.git_info.deploy_key

            checkout_dir = git_clone.clone(
                ssh_key=ssh_key,
                repo_url=remote.url,
                branch=remote.ref or "main",
            )

            logger.info(f"Cloned remote dependency '{remote.name}' to {checkout_dir}")
            return checkout_dir

        except Exception as e:
            logger.warning(
                f"Failed to clone remote dependency '{remote.name}': {e}. "
                "Views from this dependency will be missing from lineage."
            )
            return None

    def consume_temp_dirs(self) -> List[str]:
        """Return and clear the list of temp dirs created during remote dependency cloning.

        The caller is responsible for cleaning up the returned directories.
        """
        dirs = list(self._temp_dirs)
        self._temp_dirs = []
        return dirs

    def cleanup(self) -> None:
        """Cleanup temporary directories."""
        import shutil

        for temp_dir in self._temp_dirs:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp dir {temp_dir}: {e}")


def parse_manifest_constants(
    base_folder: str,
    project_dependencies: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Extract LookML constants from manifest.lkml without full dependency resolution.

    Useful when only @{constant} substitution is needed and remote dependency cloning
    is not required. Cleans up any temporary directories before returning.

    Args:
        base_folder: Path to the root LookML project directory.
        project_dependencies: Optional map of project name to local path for local deps.

    Returns:
        Dictionary mapping constant name to its string value.

    Example:
        >>> constants = parse_manifest_constants("/path/to/my_lookml_project")
        >>> constants.get("my_schema")
        'prod_schema'
    """
    parser = ManifestParser(
        base_folder=base_folder,
        project_dependencies=project_dependencies,
    )

    try:
        parser.parse_and_resolve()
        return {name: const.value for name, const in parser.constants.items()}
    finally:
        parser.cleanup()
