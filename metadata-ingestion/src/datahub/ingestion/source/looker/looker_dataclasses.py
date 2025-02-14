import glob
import logging
import pathlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from datahub.ingestion.source.looker.looker_connection import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_template_language import (
    load_and_preprocess_file,
)
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    EXPLORE_FILE_EXTENSION,
    LookMLSourceConfig,
    LookMLSourceReport,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class ProjectInclude:
    project: str
    include: str


@dataclass
class LookerField:
    name: str
    primary_key: str  # possible values yes and no
    type: str
    sql: Optional[str]


@dataclass
class LookerConstant:
    name: str
    value: str


@dataclass
class LookerModel:
    connection: str
    includes: List[str]
    explores: List[dict]
    resolved_includes: List[ProjectInclude]

    @staticmethod
    def from_looker_dict(
        looker_model_dict: dict,
        base_project_name: str,
        root_project_name: Optional[str],
        base_projects_folders: Dict[str, pathlib.Path],
        path: str,
        source_config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
    ) -> "LookerModel":
        logger.debug(f"Loading model from {path}")
        connection = looker_model_dict["connection"]
        includes = looker_model_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(
            includes,
            base_project_name,
            root_project_name,
            base_projects_folders,
            path,
            source_config,
            reporter,
            seen_so_far=set(),
            traversal_path=pathlib.Path(path).stem,
        )
        logger.debug(f"{path} has resolved_includes: {resolved_includes}")
        explores = looker_model_dict.get("explores", [])

        explore_files = [
            x.include
            for x in resolved_includes
            if x.include.endswith(EXPLORE_FILE_EXTENSION)
        ]
        for included_file in explore_files:
            try:
                parsed = load_and_preprocess_file(
                    path=included_file,
                    reporter=reporter,
                    source_config=source_config,
                )
                included_explores = parsed.get("explores", [])
                explores.extend(included_explores)
            except Exception as e:
                reporter.report_warning(
                    title="Error Loading Include",
                    message="Failed to load include file",
                    context=f"Include Details: {included_file}",
                    exc=e,
                )
                # continue in this case, as it might be better to load and resolve whatever we can

        return LookerModel(
            connection=connection,
            includes=includes,
            resolved_includes=resolved_includes,
            explores=explores,
        )

    @staticmethod
    def resolve_includes(
        includes: List[str],
        project_name: str,
        root_project_name: Optional[str],
        base_projects_folder: Dict[str, pathlib.Path],
        path: str,
        source_config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        seen_so_far: Set[str],
        traversal_path: str = "",  # a cosmetic parameter to aid debugging
    ) -> List[ProjectInclude]:
        """Resolve ``include`` statements in LookML model files to a list of ``.lkml`` files.

        For rules on how LookML ``include`` statements are written, see
            https://docs.looker.com/data-modeling/getting-started/ide-folders#wildcard_examples
        """

        resolved = []
        for inc in includes:
            # Filter out dashboards - we get those through the looker source.
            if (
                inc.endswith(".dashboard")
                or inc.endswith(".dashboard.lookml")
                or inc.endswith(".dashboard.lkml")
            ):
                logger.debug(f"include '{inc}' is a dashboard, skipping it")
                continue

            resolved_project_name = project_name
            resolved_project_folder = str(base_projects_folder[project_name])

            # Massage the looker include into a valid glob wildcard expression
            if inc.startswith("//"):
                # remote include, let's see if we have the project checked out locally
                (remote_project, project_local_path) = inc[2:].split("/", maxsplit=1)
                if remote_project in base_projects_folder:
                    resolved_project_folder = str(base_projects_folder[remote_project])
                    glob_expr = f"{resolved_project_folder}/{project_local_path}"
                    resolved_project_name = remote_project
                else:
                    logger.warning(
                        f"Resolving {inc} failed. Could not find a locally checked out reference for {remote_project}"
                    )
                    continue
            elif inc.startswith("/"):
                glob_expr = f"{resolved_project_folder}{inc}"

                # The include path is sometimes '/{project_name}/{path_within_project}'
                # instead of '//{project_name}/{path_within_project}' or '/{path_within_project}'.
                #
                # TODO: I can't seem to find any documentation on this pattern, but we definitely
                # have seen it in the wild. Example from Mozilla's public looker-hub repo:
                # https://github.com/mozilla/looker-hub/blob/f491ca51ce1add87c338e6723fd49bc6ae4015ca/fenix/explores/activation.explore.lkml#L7
                # As such, we try to handle it but are as defensive as possible.

                non_base_project_name = project_name
                if project_name == BASE_PROJECT_NAME and root_project_name is not None:
                    non_base_project_name = root_project_name
                if non_base_project_name != BASE_PROJECT_NAME and inc.startswith(
                    f"/{non_base_project_name}/"
                ):
                    # This might be a local include. Let's make sure that '/{project_name}' doesn't
                    # exist as normal include in the project.
                    if not pathlib.Path(
                        f"{resolved_project_folder}/{non_base_project_name}"
                    ).exists():
                        path_within_project = pathlib.Path(*pathlib.Path(inc).parts[2:])
                        glob_expr = f"{resolved_project_folder}/{path_within_project}"
            else:
                # Need to handle a relative path.
                glob_expr = str(pathlib.Path(path).parent / inc)
            # "**" matches an arbitrary number of directories in LookML
            # we also resolve these paths to absolute paths so we can de-dup effectively later on
            included_files = [
                str(p.resolve())
                for p in [
                    pathlib.Path(p)
                    for p in sorted(
                        glob.glob(glob_expr, recursive=True)
                        + glob.glob(f"{glob_expr}.lkml", recursive=True)
                    )
                ]
                # We don't want to match directories. The '**' glob can be used to
                # recurse into directories.
                if p.is_file()
            ]
            logger.debug(
                f"traversal_path={traversal_path}, included_files = {included_files}, seen_so_far: {seen_so_far}"
            )
            if "*" not in inc and not included_files:
                reporter.warning(
                    title="Error Resolving Include",
                    message="Cannot resolve included file",
                    context=f"Include: {inc}, path: {path}, traversal_path: {traversal_path}",
                )
            elif not included_files:
                reporter.warning(
                    title="Error Resolving Include",
                    message="Did not find anything matching the wildcard include",
                    context=f"Include: {inc}, path: {path}, traversal_path: {traversal_path}",
                )
            # only load files that we haven't seen so far
            included_files = [x for x in included_files if x not in seen_so_far]
            for included_file in included_files:
                # Filter out dashboards - we get those through the looker source.
                if (
                    included_file.endswith(".dashboard")
                    or included_file.endswith(".dashboard.lookml")
                    or included_file.endswith(".dashboard.lkml")
                ):
                    logger.debug(
                        f"include '{included_file}' is a dashboard, skipping it"
                    )
                    continue

                logger.debug(
                    f"Will be loading {included_file}, traversed here via {traversal_path}"
                )
                try:
                    parsed = load_and_preprocess_file(
                        path=included_file,
                        reporter=reporter,
                        source_config=source_config,
                    )
                    seen_so_far.add(included_file)
                    if "includes" in parsed:  # we have more includes to resolve!
                        resolved.extend(
                            LookerModel.resolve_includes(
                                parsed["includes"],
                                resolved_project_name,
                                root_project_name,
                                base_projects_folder,
                                included_file,
                                source_config,
                                reporter,
                                seen_so_far,
                                traversal_path=f"{traversal_path} -> {pathlib.Path(included_file).stem}",
                            )
                        )
                except Exception as e:
                    reporter.report_warning(
                        title="Error Loading Include File",
                        message="Failed to load included file",
                        context=f"Include Details: {included_file}",
                        exc=e,
                    )
                    # continue in this case, as it might be better to load and resolve whatever we can

            resolved.extend(
                [
                    ProjectInclude(project=resolved_project_name, include=f)
                    for f in included_files
                ]
            )
        return resolved


@dataclass
class LookerViewFile:
    absolute_file_path: str
    connection: Optional[LookerConnectionDefinition]
    includes: List[str]
    resolved_includes: List[ProjectInclude]
    views: List[Dict]
    raw_file_content: str

    @classmethod
    def from_looker_dict(
        cls,
        absolute_file_path: str,
        looker_view_file_dict: dict,
        project_name: str,
        root_project_name: Optional[str],
        base_projects_folder: Dict[str, pathlib.Path],
        raw_file_content: str,
        source_config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
    ) -> "LookerViewFile":
        logger.debug(f"Loading view file at {absolute_file_path}")
        includes = looker_view_file_dict.get("includes", [])
        resolved_path = str(pathlib.Path(absolute_file_path).resolve())
        seen_so_far = set()
        seen_so_far.add(resolved_path)
        resolved_includes = LookerModel.resolve_includes(
            includes,
            project_name,
            root_project_name,
            base_projects_folder,
            absolute_file_path,
            source_config,
            reporter,
            seen_so_far=seen_so_far,
        )
        logger.debug(
            f"resolved_includes for {absolute_file_path} is {resolved_includes}"
        )
        views = looker_view_file_dict.get("views", [])

        return cls(
            absolute_file_path=absolute_file_path,
            connection=None,
            includes=includes,
            resolved_includes=resolved_includes,
            views=views,
            raw_file_content=raw_file_content,
        )
