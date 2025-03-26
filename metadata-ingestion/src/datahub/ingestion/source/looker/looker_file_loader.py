import logging
import pathlib
from dataclasses import replace
from typing import Dict, Optional

from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerConstant,
    LookerViewFile,
)
from datahub.ingestion.source.looker.looker_template_language import (
    load_and_preprocess_file,
)
from datahub.ingestion.source.looker.lookml_config import (
    EXPLORE_FILE_EXTENSION,
    VIEW_FILE_EXTENSION,
    LookMLSourceConfig,
    LookMLSourceReport,
)

logger = logging.getLogger(__name__)


class LookerViewFileLoader:
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(
        self,
        root_project_name: Optional[str],
        base_projects_folder: Dict[str, pathlib.Path],
        reporter: LookMLSourceReport,
        source_config: LookMLSourceConfig,
        manifest_constants: Optional[Dict[str, LookerConstant]] = None,
    ) -> None:
        self.viewfile_cache: Dict[str, Optional[LookerViewFile]] = {}
        self._root_project_name = root_project_name
        self._base_projects_folder = base_projects_folder
        self.reporter = reporter
        self.source_config = source_config
        self.manifest_constants = manifest_constants or {}

    def _load_viewfile(
        self, project_name: str, path: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        # always fully resolve paths to simplify de-dup
        path = str(pathlib.Path(path).resolve())
        allowed_extensions = [VIEW_FILE_EXTENSION, EXPLORE_FILE_EXTENSION]
        matched_any_extension = [
            match for match in [path.endswith(x) for x in allowed_extensions] if match
        ]
        if not matched_any_extension:
            # not a view file
            logger.debug(
                f"Skipping file {path} because it doesn't appear to be a view file. Matched extensions {allowed_extensions}"
            )
            return None

        if path in self.viewfile_cache:
            return self.viewfile_cache[path]

        try:
            with open(path) as file:
                raw_file_content = file.read()
        except Exception as e:
            self.reporter.report_warning(
                title="LKML File Loading Error",
                message="A lookml file is not present on local storage or GitHub",
                context=f"file path: {path}",
                exc=e,
            )
            self.viewfile_cache[path] = None
            return None
        try:
            logger.debug(f"Loading viewfile {path}")

            # load_and preprocess_file is called multiple times for loading view file from multiple flows.
            # Flag resolve_constants is a hack to avoid passing around manifest_constants from all of the flows.
            # This is fine as rest of flows do not need resolution of constants.
            parsed = load_and_preprocess_file(
                path=path,
                reporter=self.reporter,
                source_config=self.source_config,
                resolve_constants=True,
                manifest_constants=self.manifest_constants,
            )

            looker_viewfile = LookerViewFile.from_looker_dict(
                absolute_file_path=path,
                looker_view_file_dict=parsed,
                project_name=project_name,
                root_project_name=self._root_project_name,
                base_projects_folder=self._base_projects_folder,
                raw_file_content=raw_file_content,
                source_config=self.source_config,
                reporter=reporter,
            )
            logger.debug(f"adding viewfile for path {path} to the cache")
            self.viewfile_cache[path] = looker_viewfile
            return looker_viewfile
        except Exception as e:
            self.reporter.report_warning(
                title="LKML File Parsing Error",
                message="The input file is not lookml file",
                context=f"file path: {path}",
                exc=e,
            )

            logger.debug(f"Raw file content for path {path}")

            logger.debug(raw_file_content)

            self.viewfile_cache[path] = None

            return None

    def load_viewfile(
        self,
        path: str,
        project_name: str,
        connection: Optional[LookerConnectionDefinition],
        reporter: LookMLSourceReport,
    ) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(
            project_name=project_name,
            path=path,
            reporter=reporter,
        )
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)
