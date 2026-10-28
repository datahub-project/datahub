import pathlib
from typing import Dict, List, Optional

import pytest

from datahub.ingestion.source.looker.looker_common import ViewFieldValue
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerConstant,
    LookerModel,
    ProjectInclude,
)
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.looker_view_id_cache import (
    LookerViewIdCache,
    determine_view_file_path,
)
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)

CONNECTION = LookerConnectionDefinition(
    platform="postgres", default_db="db", default_schema="public"
)


class Project:
    """A temp LookML project with a real config, reporter and file loader."""

    def __init__(self, tmp_path: pathlib.Path):
        self.folder = tmp_path / "project"
        self.folder.mkdir()
        self.dep_folder = tmp_path / "dep_project"
        self.dep_folder.mkdir()
        self.projects = {BASE_PROJECT_NAME: self.folder, "dep_project": self.dep_folder}
        self.config = LookMLSourceConfig.model_validate(
            {
                "base_folder": str(self.folder),
                "connection_to_platform_map": {"my_connection": "postgres"},
                "project_name": "test_project",
                "lookml_constants": {"schema_name": "config_schema"},
            }
        )
        self.reporter = LookMLSourceReport()
        self.loader = self.make_loader()

    def make_loader(
        self, manifest_constants: Optional[Dict[str, LookerConstant]] = None
    ) -> LookerViewFileLoader:
        return LookerViewFileLoader(
            root_project_name="test_project",
            base_projects_folder=self.projects,
            reporter=self.reporter,
            source_config=self.config,
            manifest_constants=manifest_constants,
        )

    def write(self, name: str, content: str = "", dep: bool = False) -> str:
        path = (self.dep_folder if dep else self.folder) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return str(path)

    def load(
        self, path: str, loader: Optional[LookerViewFileLoader] = None
    ) -> Optional[object]:
        return (loader or self.loader).load_viewfile(
            path, BASE_PROJECT_NAME, CONNECTION, self.reporter
        )

    def resolve(self, includes: List[str], model_path: str) -> List[ProjectInclude]:
        return LookerModel.resolve_includes(
            includes,
            BASE_PROJECT_NAME,
            "test_project",
            self.projects,
            model_path,
            self.config,
            self.reporter,
            seen_so_far=set(),
        )

    def view_id_cache(self, includes: List[ProjectInclude]) -> LookerViewIdCache:
        model = LookerModel(
            connection="c", includes=[], explores=[], resolved_includes=includes
        )
        return LookerViewIdCache(
            project_name="test_project",
            model_name="data",
            looker_model=model,
            looker_viewfile_loader=self.loader,
            reporter=self.reporter,
        )

    def warning_titles(self) -> List[str]:
        return [w.title for w in self.reporter.warnings if w.title]


@pytest.fixture
def project(tmp_path: pathlib.Path) -> Project:
    return Project(tmp_path)


class TestLookerViewFileLoader:
    def test_loads_views_and_resolves_constants(self, project: Project) -> None:
        path = project.write(
            "orders.view.lkml",
            "view: orders {\n  sql_table_name: @{schema_name}.@{table_name} ;;\n}\n",
        )
        loader = project.make_loader(
            {"table_name": LookerConstant(name="table_name", value="orders_tbl")}
        )

        viewfile = loader.load_viewfile(
            path, BASE_PROJECT_NAME, CONNECTION, project.reporter
        )

        assert viewfile is not None
        assert viewfile.connection is CONNECTION
        assert (
            viewfile.views[0]["datahub_transformed_sql_table_name"]
            == "config_schema.orders_tbl"
        )

    def test_non_view_file_extension_is_skipped(self, project: Project) -> None:
        path = project.write("data.model.lkml", "connection: my_connection\n")

        assert project.load(path) is None

    def test_missing_file_is_reported_and_returns_none(self, project: Project) -> None:
        assert project.load(str(project.folder / "missing.view.lkml")) is None
        assert "LKML File Loading Error" in project.warning_titles()

    def test_unparseable_file_is_reported_and_returns_none(
        self, project: Project
    ) -> None:
        path = project.write("broken.view.lkml", "view: broken {\n")

        assert project.load(path) is None
        assert "LKML File Parsing Error" in project.warning_titles()

    def test_file_is_read_from_disk_only_once(self, project: Project) -> None:
        path = project.write("orders.view.lkml", "view: orders {}\n")
        project.load(path)

        # Rewrite on disk: a cached loader must keep serving the first parse,
        # while still attaching the connection passed on this call.
        pathlib.Path(path).write_text("view: orders_changed {}\n")
        other_connection = LookerConnectionDefinition(platform="mysql", default_db="x")
        second = project.loader.load_viewfile(
            path, BASE_PROJECT_NAME, other_connection, project.reporter
        )

        assert second is not None
        assert second.views[0]["name"] == "orders"
        assert second.connection is other_connection


class TestResolveIncludes:
    def test_wildcard_include_matches_views_recursively(self, project: Project) -> None:
        a = project.write("views/a.view.lkml", "view: a {}\n")
        b = project.write("views/nested/b.view.lkml", "view: b {}\n")
        model_path = project.write("data.model.lkml")

        resolved = project.resolve(["/views/**/*.view.lkml"], model_path)

        assert {r.include for r in resolved} == {a, b}
        assert all(r.project == BASE_PROJECT_NAME for r in resolved)

    def test_relative_include_without_extension_is_resolved(
        self, project: Project
    ) -> None:
        a = project.write("models/a.view.lkml", "view: a {}\n")
        model_path = project.write("models/data.model.lkml")

        resolved = project.resolve(["a.view"], model_path)

        assert [r.include for r in resolved] == [a]

    def test_dashboard_includes_are_dropped(self, project: Project) -> None:
        project.write("weekly.dashboard.lookml")
        a = project.write("a.view.lkml", "view: a {}\n")
        model_path = project.write("models/data.model.lkml")

        resolved = project.resolve(
            ["/weekly.dashboard.lookml", "/a.view.lkml"], model_path
        )

        assert [r.include for r in resolved] == [a]

    def test_nested_includes_in_view_files_are_followed(self, project: Project) -> None:
        parent = project.write("parent.view.lkml", "view: parent {}\n")
        child = project.write(
            "child.view.lkml",
            'include: "/parent.view.lkml"\nview: child { extends: [parent] }\n',
        )
        model_path = project.write("data.model.lkml")

        resolved = project.resolve(["/child.view.lkml"], model_path)

        assert {r.include for r in resolved} == {child, parent}

    def test_remote_include_resolves_against_dependency_project(
        self, project: Project
    ) -> None:
        shared = project.write("views/shared.view.lkml", "view: shared {}\n", dep=True)
        model_path = project.write("data.model.lkml")

        resolved = project.resolve(["//dep_project/views/shared.view.lkml"], model_path)

        assert resolved == [ProjectInclude(project="dep_project", include=shared)]

    def test_remote_include_for_unknown_project_is_skipped(
        self, project: Project
    ) -> None:
        model_path = project.write("data.model.lkml")

        resolved = project.resolve(["//not_checked_out/views/x.view.lkml"], model_path)

        assert resolved == []

    def test_project_prefixed_local_include_is_resolved(self, project: Project) -> None:
        # "/{project_name}/path" is treated as "/path" when no such folder exists.
        a = project.write("views/a.view.lkml", "view: a {}\n")
        model_path = project.write("data.model.lkml")

        resolved = project.resolve(["/test_project/views/a.view.lkml"], model_path)

        assert [r.include for r in resolved] == [a]

    def test_unresolvable_explicit_include_is_reported(self, project: Project) -> None:
        model_path = project.write("data.model.lkml")

        resolved = project.resolve(["/views/missing.view.lkml"], model_path)

        assert resolved == []
        assert "Error Resolving Include" in project.warning_titles()


class TestLookerModelFromDict:
    def test_explores_from_included_explore_files_are_merged(
        self, project: Project
    ) -> None:
        project.write("sales.explore.lkml", "explore: sales_explore { from: orders }\n")
        model_path = project.write("data.model.lkml")

        model = LookerModel.from_looker_dict(
            looker_model_dict={
                "connection": "my_connection",
                "includes": ["/*.explore.lkml"],
                "explores": [{"name": "inline_explore"}],
            },
            base_project_name=BASE_PROJECT_NAME,
            root_project_name="test_project",
            base_projects_folders=project.projects,
            path=model_path,
            source_config=project.config,
            reporter=project.reporter,
        )

        assert model.connection == "my_connection"
        assert [e["name"] for e in model.explores] == [
            "inline_explore",
            "sales_explore",
        ]


class TestDetermineViewFilePath:
    def test_returns_path_relative_to_base_folder(self) -> None:
        assert (
            determine_view_file_path("/repo/project", "/repo/project/views/a.view.lkml")
            == "views/a.view.lkml"
        )

    def test_returns_not_available_when_base_folder_does_not_match(self) -> None:
        assert (
            determine_view_file_path("/other", "/repo/views/a.view.lkml")
            == ViewFieldValue.NOT_AVAILABLE.value
        )


class TestLookerViewIdCache:
    def test_finds_view_in_included_file_and_builds_id(self, project: Project) -> None:
        path = project.write("views/orders.view.lkml", "view: orders {}\n")
        cache = project.view_id_cache([ProjectInclude(BASE_PROJECT_NAME, path)])

        view_id = cache.get_looker_view_id("orders", str(project.folder))

        assert view_id is not None
        assert view_id.project_name == "test_project"
        assert view_id.model_name == "data"
        assert view_id.file_path == "views/orders.view.lkml"
        assert cache.get_looker_view_id("orders", str(project.folder)) is view_id

    def test_view_from_dependency_project_keeps_that_project_name(
        self, project: Project
    ) -> None:
        path = project.write("shared.view.lkml", "view: shared {}\n", dep=True)
        cache = project.view_id_cache([ProjectInclude("dep_project", path)])

        view_id = cache.get_looker_view_id("shared", str(project.folder))

        assert view_id is not None
        assert view_id.project_name == "dep_project"

    def test_unknown_view_returns_none(self, project: Project) -> None:
        path = project.write("orders.view.lkml", "view: orders {}\n")
        cache = project.view_id_cache([ProjectInclude(BASE_PROJECT_NAME, path)])

        assert cache.get_looker_view_id("customers", str(project.folder)) is None
