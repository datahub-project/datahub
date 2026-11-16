"""LookerViewFileLoader, include resolution and the view-id cache over a real
temporary project (base project plus one dependency project)."""

import pathlib

from datahub.ingestion.source.looker.looker_common import ViewFieldValue
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerConstant,
    LookerModel,
    ProjectInclude,
)
from datahub.ingestion.source.looker.looker_view_id_cache import (
    determine_view_file_path,
)
from datahub.ingestion.source.looker.lookml_config import BASE_PROJECT_NAME
from tests.unit.lookml.conftest import CONNECTION, DEP_PROJECT_NAME, LookMLProject


class TestLookerViewFileLoader:
    def test_loads_views_and_resolves_constants(
        self, lookml_project: LookMLProject
    ) -> None:
        """Constants come from the config (``lookml_constants``) and from the
        manifest; both are substituted into ``sql_table_name``."""
        lookml_project.config.lookml_constants = {"schema_name": "config_schema"}
        path = lookml_project.write(
            "orders.view.lkml",
            "view: orders {\n  sql_table_name: @{schema_name}.@{table_name} ;;\n}\n",
        )
        loader = lookml_project.make_loader(
            {"table_name": LookerConstant(name="table_name", value="orders_tbl")}
        )

        viewfile = lookml_project.load(path, loader)

        assert viewfile is not None
        assert viewfile.connection is CONNECTION
        assert (
            viewfile.views[0]["datahub_transformed_sql_table_name"]
            == "config_schema.orders_tbl"
        )

    def test_non_view_file_extension_is_skipped(
        self, lookml_project: LookMLProject
    ) -> None:
        path = lookml_project.write("data.model.lkml", "connection: my_connection\n")

        assert lookml_project.load(path) is None

    def test_missing_file_is_reported_and_returns_none(
        self, lookml_project: LookMLProject
    ) -> None:
        missing = str(lookml_project.base_folder / "missing.view.lkml")

        assert lookml_project.load(missing) is None
        assert "LKML File Loading Error" in lookml_project.warning_titles()

    def test_unparseable_file_is_reported_and_returns_none(
        self, lookml_project: LookMLProject
    ) -> None:
        path = lookml_project.write("broken.view.lkml", "view: broken {\n")

        assert lookml_project.load(path) is None
        assert "LKML File Parsing Error" in lookml_project.warning_titles()

    def test_file_is_read_from_disk_only_once(
        self, lookml_project: LookMLProject
    ) -> None:
        """The loader caches parsed files: a rewrite on disk is not picked up,
        but the connection passed on each call is still attached."""
        path = lookml_project.write("orders.view.lkml", "view: orders {}\n")
        lookml_project.load(path)

        pathlib.Path(path).write_text("view: orders_changed {}\n")
        other_connection = LookerConnectionDefinition(platform="mysql", default_db="x")
        second = lookml_project.loader.load_viewfile(
            path, BASE_PROJECT_NAME, other_connection, lookml_project.reporter
        )

        assert second is not None
        assert second.views[0]["name"] == "orders"
        assert second.connection is other_connection


class TestResolveIncludes:
    def test_wildcard_include_matches_views_recursively(
        self, lookml_project: LookMLProject
    ) -> None:
        a = lookml_project.write("views/a.view.lkml", "view: a {}\n")
        b = lookml_project.write("views/nested/b.view.lkml", "view: b {}\n")
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(
            ["/views/**/*.view.lkml"], model_path
        )

        assert {r.include for r in resolved} == {a, b}
        assert all(r.project == BASE_PROJECT_NAME for r in resolved)

    def test_relative_include_without_extension_is_resolved(
        self, lookml_project: LookMLProject
    ) -> None:
        a = lookml_project.write("models/a.view.lkml", "view: a {}\n")
        model_path = lookml_project.write("models/data.model.lkml")

        resolved = lookml_project.resolve_includes(["a.view"], model_path)

        assert [r.include for r in resolved] == [a]

    def test_dashboard_includes_are_dropped(
        self, lookml_project: LookMLProject
    ) -> None:
        """Dashboards are ingested through the Looker source, so model includes
        pointing at ``*.dashboard.lookml`` files are skipped here."""
        lookml_project.write("weekly.dashboard.lookml")
        a = lookml_project.write("a.view.lkml", "view: a {}\n")
        model_path = lookml_project.write("models/data.model.lkml")

        resolved = lookml_project.resolve_includes(
            ["/weekly.dashboard.lookml", "/a.view.lkml"], model_path
        )

        assert [r.include for r in resolved] == [a]

    def test_dashboards_matched_by_a_wildcard_are_dropped(
        self, lookml_project: LookMLProject
    ) -> None:
        """Regression: the per-file dashboard check only skipped loading, so a
        wildcard such as ``/views/*`` still returned the dashboard files it
        matched as resolved includes."""
        lookml_project.write("views/weekly.dashboard.lookml")
        lookml_project.write("views/daily.dashboard.lkml")
        a = lookml_project.write("views/a.view.lkml", "view: a {}\n")
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(["/views/*"], model_path)

        assert {r.include for r in resolved} == {a}

    def test_nested_includes_in_view_files_are_followed(
        self, lookml_project: LookMLProject
    ) -> None:
        parent = lookml_project.write("parent.view.lkml", "view: parent {}\n")
        child = lookml_project.write(
            "child.view.lkml",
            'include: "/parent.view.lkml"\nview: child { extends: [parent] }\n',
        )
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(["/child.view.lkml"], model_path)

        assert {r.include for r in resolved} == {child, parent}

    def test_remote_include_resolves_against_dependency_project(
        self, lookml_project: LookMLProject
    ) -> None:
        shared = lookml_project.write(
            "views/shared.view.lkml", "view: shared {}\n", dep=True
        )
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(
            [f"//{DEP_PROJECT_NAME}/views/shared.view.lkml"], model_path
        )

        assert resolved == [ProjectInclude(project=DEP_PROJECT_NAME, include=shared)]

    def test_remote_include_for_unknown_project_is_skipped(
        self, lookml_project: LookMLProject
    ) -> None:
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(
            ["//not_checked_out/views/x.view.lkml"], model_path
        )

        assert resolved == []

    def test_project_prefixed_local_include_is_resolved(
        self, lookml_project: LookMLProject
    ) -> None:
        # "/{project_name}/path" is treated as "/path" when no such folder exists.
        a = lookml_project.write("views/a.view.lkml", "view: a {}\n")
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(
            ["/test_project/views/a.view.lkml"], model_path
        )

        assert [r.include for r in resolved] == [a]

    def test_unresolvable_explicit_include_is_reported(
        self, lookml_project: LookMLProject
    ) -> None:
        model_path = lookml_project.write("data.model.lkml")

        resolved = lookml_project.resolve_includes(
            ["/views/missing.view.lkml"], model_path
        )

        assert resolved == []
        assert "Error Resolving Include" in lookml_project.warning_titles()


class TestLookerModelFromDict:
    def test_explores_from_included_explore_files_are_merged(
        self, lookml_project: LookMLProject
    ) -> None:
        lookml_project.write(
            "sales.explore.lkml", "explore: sales_explore { from: orders }\n"
        )
        model_path = lookml_project.write("data.model.lkml")

        model = LookerModel.from_looker_dict(
            looker_model_dict={
                "connection": "my_connection",
                "includes": ["/*.explore.lkml"],
                "explores": [{"name": "inline_explore"}],
            },
            base_project_name=BASE_PROJECT_NAME,
            root_project_name="test_project",
            base_projects_folders=lookml_project.projects,
            path=model_path,
            source_config=lookml_project.config,
            reporter=lookml_project.reporter,
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
    def test_finds_view_in_included_file_and_builds_id(
        self, lookml_project: LookMLProject
    ) -> None:
        lookml_project.write("views/orders.view.lkml", "view: orders {}\n")
        base = str(lookml_project.base_folder)

        view_id = lookml_project.view_id_cache.get_looker_view_id("orders", base)

        assert view_id is not None
        assert view_id.project_name == "test_project"
        assert view_id.model_name == "data"
        assert view_id.file_path == "views/orders.view.lkml"
        assert (
            lookml_project.view_id_cache.get_looker_view_id("orders", base) is view_id
        )

    def test_view_from_dependency_project_keeps_that_project_name(
        self, lookml_project: LookMLProject
    ) -> None:
        lookml_project.write("shared.view.lkml", "view: shared {}\n", dep=True)

        view_id = lookml_project.view_id_cache.get_looker_view_id(
            "shared", str(lookml_project.base_folder)
        )

        assert view_id is not None
        assert view_id.project_name == DEP_PROJECT_NAME

    def test_unknown_view_returns_none(self, lookml_project: LookMLProject) -> None:
        lookml_project.write("orders.view.lkml", "view: orders {}\n")

        assert (
            lookml_project.view_id_cache.get_looker_view_id(
                "customers", str(lookml_project.base_folder)
            )
            is None
        )
