"""Shared builders for the LookML unit tests.

Everything here is real DataHub code over a temporary LookML project on disk;
only the Looker API client is ever mocked, and that happens in the tests.
"""

import pathlib
import textwrap
from typing import Any, Callable, Dict, List, Optional

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerConstant,
    LookerModel,
    LookerViewFile,
    ProjectInclude,
)
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.looker_template_language import (
    load_and_preprocess_file,
)
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewIdCache
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver
from datahub.ingestion.source.looker.view_upstream import (
    AbstractViewUpstream,
    create_view_upstream,
)

PROJECT_NAME = "test_project"
DEP_PROJECT_NAME = "dep_project"
MODEL_NAME = "data"
CONNECTION_NAME = "my_connection"

CONNECTION = LookerConnectionDefinition(
    platform="postgres", default_db="db", default_schema="public"
)

LOOKER_API_CONFIG: Dict[str, str] = {
    "client_id": "id",
    "client_secret": "secret",
    "base_url": "https://looker.example.com",
}


def write_lkml(path: pathlib.Path, content: str = "") -> str:
    """Write (dedented) LookML to ``path``, creating parent folders, and return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))
    return str(path)


def dataset_urn(name: str, platform: str = "postgres") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


class LookMLProject:
    """A LookML project on disk with a real config, reporter, file loader,
    refinement resolver and view-id cache wired together.

    ``write`` registers the file as a resolved include of the model, so views
    written here are visible to the view-id cache, the refinement resolver and
    ``${view.SQL_TABLE_NAME}`` references without any further setup.
    """

    def __init__(
        self,
        base_folder: pathlib.Path,
        dep_folder: pathlib.Path,
        config: LookMLSourceConfig,
    ):
        self.base_folder = base_folder
        self.dep_folder = dep_folder
        self.projects: Dict[str, pathlib.Path] = {
            BASE_PROJECT_NAME: base_folder,
            DEP_PROJECT_NAME: dep_folder,
        }
        self.config = config
        self.reporter = LookMLSourceReport()
        self.connection = CONNECTION
        self.loader = self.make_loader()
        self.model = LookerModel(
            connection=CONNECTION_NAME, includes=[], explores=[], resolved_includes=[]
        )
        self.refinement_resolver = LookerRefinementResolver(
            looker_model=self.model,
            looker_viewfile_loader=self.loader,
            connection_definition=self.connection,
            source_config=config,
            reporter=self.reporter,
        )
        self.view_id_cache = LookerViewIdCache(
            project_name=PROJECT_NAME,
            model_name=MODEL_NAME,
            looker_model=self.model,
            looker_viewfile_loader=self.loader,
            reporter=self.reporter,
        )

    def make_loader(
        self, manifest_constants: Optional[Dict[str, LookerConstant]] = None
    ) -> LookerViewFileLoader:
        return LookerViewFileLoader(
            root_project_name=PROJECT_NAME,
            base_projects_folder=self.projects,
            reporter=self.reporter,
            source_config=self.config,
            manifest_constants=manifest_constants,
        )

    def write(self, name: str, content: str = "", dep: bool = False) -> str:
        project = DEP_PROJECT_NAME if dep else BASE_PROJECT_NAME
        path = write_lkml(self.projects[project] / name, content)
        self.model.resolved_includes.append(ProjectInclude(project, path))
        return path

    def load(
        self, path: str, loader: Optional[LookerViewFileLoader] = None
    ) -> Optional[LookerViewFile]:
        return (loader or self.loader).load_viewfile(
            path, BASE_PROJECT_NAME, self.connection, self.reporter
        )

    def resolve_includes(
        self, includes: List[str], model_path: str
    ) -> List[ProjectInclude]:
        return LookerModel.resolve_includes(
            includes,
            BASE_PROJECT_NAME,
            PROJECT_NAME,
            self.projects,
            model_path,
            self.config,
            self.reporter,
            seen_so_far=set(),
        )

    def context(
        self, lkml: str, view_name: str, file_name: Optional[str] = None
    ) -> LookerViewContext:
        lkml = textwrap.dedent(lkml)
        path = self.write(file_name or f"{view_name}.view.lkml", lkml)
        parsed = load_and_preprocess_file(
            path=path, source_config=self.config, reporter=self.reporter
        )
        view_file = LookerViewFile.from_looker_dict(
            absolute_file_path=path,
            looker_view_file_dict=parsed,
            project_name=BASE_PROJECT_NAME,
            root_project_name=PROJECT_NAME,
            base_projects_folder=self.projects,
            raw_file_content=lkml,
            source_config=self.config,
            reporter=self.reporter,
        )
        raw_view = next(v for v in view_file.views if v["name"] == view_name)
        return LookerViewContext(
            raw_view=raw_view,
            view_file=view_file,
            view_connection=self.connection,
            view_file_loader=self.loader,
            looker_refinement_resolver=self.refinement_resolver,
            base_folder_path=str(self.base_folder),
            reporter=self.reporter,
        )

    def field(self, lkml: str, view_name: str, field_name: str) -> LookerFieldContext:
        """The field context for one dimension, measure or dimension group of a view."""
        context = self.context(
            lkml, view_name, file_name=f"{view_name}_{field_name}.view.lkml"
        )
        raw = next(
            f
            for f in context.dimensions()
            + context.measures()
            + context.dimension_groups()
            if f["name"] == field_name
        )
        return LookerFieldContext(raw_field=raw)

    def upstream(
        self, lkml: str, view_name: str, **kwargs: Any
    ) -> AbstractViewUpstream:
        return create_view_upstream(
            view_context=self.context(lkml, view_name),
            looker_view_id_cache=self.view_id_cache,
            config=self.config,
            ctx=PipelineContext(run_id="t"),
            reporter=self.reporter,
            **kwargs,
        )

    def warning_titles(self) -> List[str]:
        return [w.title for w in self.reporter.warnings if w.title]


@pytest.fixture
def lookml_config_factory(
    tmp_path: pathlib.Path,
) -> Callable[..., LookMLSourceConfig]:
    """Build a ``LookMLSourceConfig`` rooted at ``tmp_path/project``; keyword
    arguments override or extend the raw config dict."""
    base_folder = tmp_path / "project"
    base_folder.mkdir(exist_ok=True)

    def build(**overrides: Any) -> LookMLSourceConfig:
        raw: Dict[str, Any] = {
            "base_folder": str(base_folder),
            "connection_to_platform_map": {CONNECTION_NAME: "postgres"},
            "project_name": PROJECT_NAME,
        }
        raw.update(overrides)
        return LookMLSourceConfig.model_validate(raw)

    return build


@pytest.fixture
def lookml_config(
    lookml_config_factory: Callable[..., LookMLSourceConfig],
) -> LookMLSourceConfig:
    return lookml_config_factory()


@pytest.fixture
def lookml_project_factory(
    tmp_path: pathlib.Path,
    lookml_config_factory: Callable[..., LookMLSourceConfig],
) -> Callable[..., LookMLProject]:
    dep_folder = tmp_path / DEP_PROJECT_NAME
    dep_folder.mkdir(exist_ok=True)

    def build(**config_overrides: Any) -> LookMLProject:
        config = lookml_config_factory(**config_overrides)
        return LookMLProject(pathlib.Path(str(config.base_folder)), dep_folder, config)

    return build


@pytest.fixture
def lookml_project(
    lookml_project_factory: Callable[..., LookMLProject],
) -> LookMLProject:
    return lookml_project_factory()
