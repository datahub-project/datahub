"""
View Discovery for Looker V2 Source.

Handles discovery of views from model includes, categorizing them as:
- Reachable: Referenced by explores (use API for lineage)
- Unreachable: Included but not referenced (parse LookML)
- Orphaned: Not included by any model (warn and skip)
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from looker_sdk.sdk.api40.models import LookmlModelExplore

logger = logging.getLogger(__name__)


@dataclass
class ViewFile:
    """A LookML view file and the view names it defines."""

    file_path: str
    project_name: str
    view_names: List[str] = field(default_factory=list)


@dataclass
class ModelIncludes:
    """Include directives parsed from a single LookML model file.

    Attributes:
        include_patterns: Raw include strings (may contain wildcards or cross-project paths).
        resolved_files: Absolute paths of files that matched the include patterns.
    """

    model_name: str
    model_file: str
    include_patterns: List[str] = field(default_factory=list)
    resolved_files: Set[str] = field(default_factory=set)


@dataclass
class ViewDiscoveryResult:
    """Output of a ViewDiscovery.discover() run.

    Attributes:
        reachable_views: Views referenced by at least one explore. Metadata should
            be fetched via Looker API for these views.
        unreachable_views: Views included by a model but not referenced by any explore.
            These require LookML file parsing to extract metadata.
        orphaned_files: View files not included by any model. These are skipped with
            a warning because they cannot be linked to a project connection.
        view_to_file: Maps view name to its source file path.
        view_to_project: Maps view name to the project that owns it.
        model_includes: Parsed include data keyed by model name.
    """

    reachable_views: Set[str] = field(default_factory=set)
    unreachable_views: Set[str] = field(default_factory=set)
    orphaned_files: Set[str] = field(default_factory=set)
    view_to_file: Dict[str, str] = field(default_factory=dict)
    view_to_project: Dict[str, str] = field(default_factory=dict)
    model_includes: Dict[str, ModelIncludes] = field(default_factory=dict)


class ViewDiscovery:
    """
    Discovers and categorizes views from LookML projects.

    Parses model files to find includes, resolves wildcards,
    and categorizes views based on explore references.
    """

    # Regex to extract include statements from model files
    INCLUDE_PATTERN = re.compile(r'include:\s*"([^"]+)"', re.MULTILINE)

    # Regex to extract base view names (not refinements which use view: +name)
    VIEW_NAME_PATTERN = re.compile(r"view:\s*(\w+)\s*\{", re.MULTILINE)

    # Regex to extract refinement view names (view: +name)
    REFINEMENT_VIEW_NAME_PATTERN = re.compile(r"view:\s*\+(\w+)\s*\{", re.MULTILINE)

    # Regex to extract explore from: references
    EXPLORE_FROM_PATTERN = re.compile(r"from:\s*(\w+)", re.MULTILINE)

    # Regex to extract explore view_name references
    EXPLORE_VIEW_NAME_PATTERN = re.compile(r"view_name:\s*(\w+)", re.MULTILINE)

    def __init__(
        self,
        base_folder: str,
        project_name: str,
        project_dependencies: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize view discovery.

        Args:
            base_folder: Path to the main LookML project
            project_name: Name of the main project
            project_dependencies: Mapping of project name to path for dependencies
        """
        self.base_folder = Path(base_folder)
        self.project_name = project_name
        self.project_dependencies = project_dependencies or {}

        # Cache for resolved paths
        self._path_cache: Dict[str, Path] = {}

    def discover(
        self,
        explore_view_names: FrozenSet[str],
    ) -> ViewDiscoveryResult:
        """
        Discover and categorize all views.

        Args:
            explore_view_names: Set of view names referenced by explores (from API)

        Returns:
            ViewDiscoveryResult with categorized views
        """
        result = ViewDiscoveryResult()

        # Step 1: Find all model files and parse includes
        model_files = self._find_model_files()
        for model_file in model_files:
            model_includes = self._parse_model_includes(model_file)
            if model_includes:
                result.model_includes[model_includes.model_name] = model_includes

        # Step 2: Resolve all includes to actual files
        included_files: Set[str] = set()
        for model_includes in result.model_includes.values():
            resolved = self._resolve_includes(model_includes.include_patterns)
            model_includes.resolved_files = resolved
            included_files.update(resolved)

        # Step 3: Parse view names from included files
        # Process main project files first for deterministic precedence
        main_files = [
            f
            for f in included_files
            if self._get_project_for_file(f) == self.project_name
        ]
        dep_files = [
            f
            for f in included_files
            if self._get_project_for_file(f) != self.project_name
        ]
        for file_path in main_files + dep_files:
            view_names = self._parse_view_names(file_path)
            project = self._get_project_for_file(file_path)
            for view_name in view_names:
                if view_name in result.view_to_file:
                    existing_file = result.view_to_file[view_name]
                    existing_project = result.view_to_project[view_name]
                    # Main project takes precedence over dependencies
                    if existing_project == self.project_name:
                        logger.warning(
                            f"View '{view_name}' defined in multiple files: "
                            f"'{existing_file}' (keeping, main project) and "
                            f"'{file_path}' (skipping, project '{project}')"
                        )
                        continue
                    logger.warning(
                        f"View '{view_name}' defined in multiple files: "
                        f"'{existing_file}' (project '{existing_project}') and "
                        f"'{file_path}' (project '{project}'). "
                        f"Using '{file_path}'."
                    )
                result.view_to_file[view_name] = file_path
                result.view_to_project[view_name] = project

        # Step 4: Categorize views
        all_included_views = set(result.view_to_file.keys())
        result.reachable_views = all_included_views & explore_view_names
        result.unreachable_views = all_included_views - explore_view_names

        # Step 5: Find orphaned files
        all_view_files = self._find_all_view_files()
        result.orphaned_files = all_view_files - included_files

        logger.info(
            f"View discovery complete: "
            f"reachable={len(result.reachable_views)}, "
            f"unreachable={len(result.unreachable_views)}, "
            f"orphaned={len(result.orphaned_files)}"
        )

        return result

    def _find_model_files(self) -> List[str]:
        """Find all .model.lkml files in the project."""
        model_files = []

        for root, _, files in os.walk(self.base_folder):
            for file in files:
                if file.endswith(".model.lkml"):
                    model_files.append(os.path.join(root, file))

        return model_files

    def _parse_model_includes(self, model_file: str) -> Optional[ModelIncludes]:
        """Parse include statements from a model file."""
        try:
            with open(model_file, "r", encoding="utf-8") as f:
                content = f.read()

            includes = self.INCLUDE_PATTERN.findall(content)
            if not includes:
                return None

            model_name = Path(model_file).stem.replace(".model", "")
            return ModelIncludes(
                model_name=model_name,
                model_file=model_file,
                include_patterns=includes,
            )
        except OSError as e:
            logger.warning(f"Failed to parse model file {model_file}: {e}")
            return None

    def _resolve_includes(self, patterns: List[str]) -> Set[str]:
        """
        Resolve include patterns to actual file paths.

        Handles:
        - Wildcards: "views/*.view.lkml"
        - Cross-project: "//other_project/views/file.lkml"
        - Relative paths: "/views/file.lkml"
        """
        resolved: Set[str] = set()

        for pattern in patterns:
            if pattern.startswith("//"):
                # Cross-project include
                resolved.update(self._resolve_cross_project_include(pattern))
            else:
                # Local include
                resolved.update(self._resolve_local_include(pattern))

        return resolved

    def _resolve_cross_project_include(self, pattern: str) -> Set[str]:
        """Resolve a cross-project include pattern."""
        # Pattern: //project_name/path/to/file.lkml
        match = re.match(r"//([^/]+)/(.+)", pattern)
        if not match:
            logger.warning(f"Invalid cross-project include pattern: {pattern}")
            return set()

        project_name, relative_path = match.groups()

        if project_name not in self.project_dependencies:
            logger.warning(
                f"Unknown project dependency '{project_name}' in include: {pattern}"
            )
            return set()

        project_path = Path(self.project_dependencies[project_name])
        return self._resolve_glob_pattern(project_path, relative_path)

    def _resolve_local_include(self, pattern: str) -> Set[str]:
        """Resolve a local include pattern."""
        # Remove leading slash if present
        pattern = pattern.lstrip("/")
        return self._resolve_glob_pattern(self.base_folder, pattern)

    def _resolve_glob_pattern(self, base_path: Path, pattern: str) -> Set[str]:
        """Resolve a glob pattern to file paths.

        Uses pathlib.glob which properly supports ** for recursive matching.
        """
        resolved: Set[str] = set()

        if "*" in pattern:
            # Use pathlib glob which supports ** recursive patterns
            for file_path in base_path.glob(pattern):
                if file_path.is_file():
                    resolved.add(str(file_path))
        else:
            # Direct file reference
            direct_path = base_path / pattern
            if direct_path.exists():
                resolved.add(str(direct_path))

        return resolved

    def _parse_view_names(self, file_path: str) -> List[str]:
        """Parse view names from a view file.

        Returns only base view definitions (view: name), not refinements
        (view: +name). Refinements share a name with their base view, so
        if both files were included, the base file should win the file mapping.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            base_views = self.VIEW_NAME_PATTERN.findall(content)
            # Also include refinement-only files so that views defined only
            # via refinement (no base file in this project) are still discovered.
            if not base_views:
                return self.REFINEMENT_VIEW_NAME_PATTERN.findall(content)
            return base_views
        except OSError as e:
            logger.warning(f"Failed to parse view file {file_path}: {e}")
            return []

    def _find_all_view_files(self) -> Set[str]:
        """Find all .view.lkml files in the project and dependencies."""
        all_files: Set[str] = set()

        # Main project
        for root, _, files in os.walk(self.base_folder):
            for file in files:
                if file.endswith(".view.lkml"):
                    all_files.add(os.path.join(root, file))

        # Dependencies
        for project_path in self.project_dependencies.values():
            for root, _, files in os.walk(project_path):
                for file in files:
                    if file.endswith(".view.lkml"):
                        all_files.add(os.path.join(root, file))

        return all_files

    def _get_project_for_file(self, file_path: str) -> str:
        """Determine which project a file belongs to."""
        file_path_obj = Path(file_path)

        # Check if it's in the main project
        try:
            file_path_obj.relative_to(self.base_folder)
            return self.project_name
        except ValueError:
            pass

        # Check dependencies
        for project_name, project_path in self.project_dependencies.items():
            try:
                file_path_obj.relative_to(project_path)
                return project_name
            except ValueError:
                continue

        return "unknown"


def extract_explore_views_from_api(
    explores: List[Tuple[str, str, LookmlModelExplore]],
) -> FrozenSet[str]:
    """Collect all view names referenced by a list of explore objects.

    Includes both the explore's base view (view_name / name) and all joined views
    (join.from_ or join.name). Used to distinguish reachable from unreachable views.

    Args:
        explores: List of (project_name, model_name, explore) tuples from the API.

    Returns:
        Frozenset of unique view names referenced across all provided explores.
    """
    view_names: Set[str] = set()

    for _project, _model, explore in explores:
        # Get the main view (from: or view_name: or explore name)
        if explore.view_name:
            view_names.add(explore.view_name)
        elif explore.name:
            view_names.add(explore.name)

        # Get joined views
        if explore.joins:
            for join in explore.joins:
                if join.from_:
                    view_names.add(join.from_)
                elif join.name:
                    view_names.add(join.name)

    return frozenset(view_names)
