import logging
import pathlib
from dataclasses import replace
from typing import Any, Dict, Optional

from datahub.ingestion.source.looker.lkml_patched import load_lkml
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import LookerViewFile
from datahub.ingestion.source.looker.looker_template_language import (
    resolve_liquid_variable_in_view_dict,
)
from datahub.ingestion.source.looker.lookml_config import (
    _EXPLORE_FILE_EXTENSION,
    _VIEW_FILE_EXTENSION,
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
        liquid_variable: Dict[Any, Any],
    ) -> None:
        self.viewfile_cache: Dict[str, Optional[LookerViewFile]] = {}
        self._root_project_name = root_project_name
        self._base_projects_folder = base_projects_folder
        self.reporter = reporter
        self.liquid_variable = liquid_variable

    def _load_viewfile(
        self, project_name: str, path: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        # always fully resolve paths to simplify de-dup
        path = str(pathlib.Path(path).resolve())
        allowed_extensions = [_VIEW_FILE_EXTENSION, _EXPLORE_FILE_EXTENSION]
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
            self.reporter.failure("Failed to read lkml file", path, exc=e)
            self.viewfile_cache[path] = None
            return None
        try:
            logger.debug(f"Loading viewfile {path}")

            parsed = load_lkml(path)

            resolve_liquid_variable_in_view_dict(
                raw_view=parsed,
                liquid_variable=self.liquid_variable,
            )

            looker_viewfile = LookerViewFile.from_looker_dict(
                absolute_file_path=path,
                looker_view_file_dict=parsed,
                project_name=project_name,
                root_project_name=self._root_project_name,
                base_projects_folder=self._base_projects_folder,
                raw_file_content=raw_file_content,
                reporter=reporter,
            )
            logger.debug(f"adding viewfile for path {path} to the cache")
            self.viewfile_cache[path] = looker_viewfile
            return looker_viewfile
        except Exception as e:
            self.reporter.failure("Failed to parse lkml file", path, exc=e)
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
