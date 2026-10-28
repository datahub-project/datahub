"""End-to-end LookMLSource runs over small LookML projects on disk (no Looker API).

These exercise the model/view discovery loop, container + dataset emission,
lineage, custom properties and the reporting paths that only run when the
whole source is driven, rather than individual helpers.
"""

import logging
import pathlib
import textwrap
from typing import Dict, List, Union
from unittest import mock

import pytest
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import LookmlModel

from datahub.configuration.git import GitInfo
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig
from datahub.ingestion.source.looker.lookml_source import LookMLSource
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sdk.entity import Entity

ORDERS_VIEW = """
view: orders {
  sql_table_name: @{schema_name}.orders ;;
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }
  dimension_group: created {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.created_at ;;
  }
  measure: count {
    type: count
  }
}
"""

CUSTOMERS_VIEW = """
view: customers {
  sql_table_name: analytics.customers ;;
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }
}
"""

ORDER_FACTS_VIEW = """
view: order_facts {
  derived_table: {
    sql: SELECT customer_id, COUNT(*) AS order_count FROM analytics.orders GROUP BY 1 ;;
  }
  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }
  dimension: order_count {
    type: number
    sql: ${TABLE}.order_count ;;
  }
}
"""

ORPHAN_VIEW = """
view: orphan {
  sql_table_name: analytics.orphan ;;
  dimension: id {
    type: number
    sql: ${TABLE}.id ;;
  }
}
"""

SALES_MODEL = """
connection: "my_connection"
include: "/views/*.view.lkml"

explore: orders {
  join: customers {
    sql_on: ${orders.customer_id} = ${customers.id} ;;
    relationship: many_to_one
  }
  join: order_facts {
    sql_on: ${orders.customer_id} = ${order_facts.customer_id} ;;
    relationship: one_to_one
  }
}
"""

MANIFEST = """
project_name: "my_project"
constant: schema_name {
  value: "analytics"
}
"""


def write(path: pathlib.Path, content: str) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))
    return path


def make_project(root: pathlib.Path, with_manifest: bool = True) -> pathlib.Path:
    write(root / "views" / "orders.view.lkml", ORDERS_VIEW)
    write(root / "views" / "customers.view.lkml", CUSTOMERS_VIEW)
    write(root / "views" / "order_facts.view.lkml", ORDER_FACTS_VIEW)
    write(root / "unreferenced" / "orphan.view.lkml", ORPHAN_VIEW)
    write(root / "models" / "sales.model.lkml", SALES_MODEL)
    if with_manifest:
        write(root / "manifest.lkml", MANIFEST)
    return root


def base_config(root: pathlib.Path, **overrides: object) -> Dict[str, object]:
    config: Dict[str, object] = {
        "base_folder": str(root),
        "project_name": "my_project",
        "connection_to_platform_map": {
            "my_connection": {
                "platform": "postgres",
                "default_db": "warehouse",
                "default_schema": "public",
            }
        },
    }
    config.update(overrides)
    return config


def run_source(config: Dict[str, object]) -> "SourceRun":
    source = LookMLSource(
        LookMLSourceConfig.model_validate(config), PipelineContext(run_id="t")
    )
    workunits: List[Union[MetadataWorkUnit, Entity]] = []
    for workunit in source.get_workunits():
        # The pipeline counts produced events; the source checks that count
        # at the end of its run, so mirror it here.
        source.get_report().report_workunit(workunit)
        workunits.append(workunit)
    return SourceRun(source, workunits)


class SourceRun:
    def __init__(
        self, source: LookMLSource, workunits: List[Union[MetadataWorkUnit, Entity]]
    ):
        self.source = source
        self.workunits = workunits

    def _aspects(self, cls: type) -> Dict[str, object]:
        found: Dict[str, object] = {}
        for wu in self.workunits:
            if not isinstance(wu, MetadataWorkUnit):
                continue
            aspect = wu.get_aspect_of_type(cls)
            if aspect is not None:
                found[wu.get_urn()] = aspect
        return found

    def aspects(self, cls: type) -> Dict[str, object]:
        return self._aspects(cls)

    def urns_with(self, cls: type) -> List[str]:
        return sorted(self._aspects(cls).keys())


def dataset_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:looker,{name},PROD)"


@pytest.fixture
def project(tmp_path: pathlib.Path) -> pathlib.Path:
    return make_project(tmp_path / "repo")


class TestLookMLSourceEmission:
    def test_emits_project_container_and_reachable_views(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project))

        view_urns = run.urns_with(ViewPropertiesClass)
        assert view_urns == [
            dataset_urn("my_project.view.customers"),
            dataset_urn("my_project.view.order_facts"),
            dataset_urn("my_project.view.orders"),
        ]
        container_urns = run.urns_with(ContainerPropertiesClass)
        assert len(container_urns) == 1
        container = run.aspects(ContainerPropertiesClass)[container_urns[0]]
        assert isinstance(container, ContainerPropertiesClass)
        assert container.name == "my_project"
        assert run.source.processed_projects == ["my_project"]

    def test_manifest_constant_resolves_sql_table_name(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project))
        lineage = run.aspects(UpstreamLineageClass)
        orders = lineage[dataset_urn("my_project.view.orders")]
        assert isinstance(orders, UpstreamLineageClass)
        # @{schema_name} is defined in manifest.lkml.
        assert [u.dataset for u in orders.upstreams] == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.analytics.orders,PROD)"
        ]
        assert run.source.manifest_constants["schema_name"].value == "analytics"

    def test_config_constant_takes_precedence_over_manifest(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(
            base_config(project, lookml_constants={"schema_name": "config_schema"})
        )
        lineage = run.aspects(UpstreamLineageClass)
        orders = lineage[dataset_urn("my_project.view.orders")]
        assert isinstance(orders, UpstreamLineageClass)
        assert [u.dataset for u in orders.upstreams] == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.config_schema.orders,PROD)"
        ]

    def test_missing_manifest_is_reported_and_constant_left_unresolved(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = make_project(tmp_path / "repo", with_manifest=False)
        run = run_source(base_config(root))
        assert any(
            w.title == "Manifest File Missing" for w in run.source.reporter.warnings
        )
        assert any(
            w.title == "LookML constant not found" for w in run.source.reporter.warnings
        )

    def test_column_lineage_and_schema_for_regular_view(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project))
        orders_urn = dataset_urn("my_project.view.orders")

        schema = run.aspects(SchemaMetadataClass)[orders_urn]
        assert isinstance(schema, SchemaMetadataClass)
        assert [f.fieldPath for f in schema.fields] == [
            "id",
            "customer_id",
            "created",
            "count",
        ]
        assert schema.primaryKeys == ["id"]
        tags_by_field = {
            f.fieldPath: [t.tag for t in f.globalTags.tags] if f.globalTags else []
            for f in schema.fields
        }
        assert tags_by_field["created"] == [
            "urn:li:tag:Dimension",
            "urn:li:tag:Temporal",
        ]
        assert tags_by_field["count"] == ["urn:li:tag:Measure"]

        lineage = run.aspects(UpstreamLineageClass)[orders_urn]
        assert isinstance(lineage, UpstreamLineageClass)
        assert lineage.fineGrainedLineages is not None
        by_downstream = {
            (fgl.downstreams or [""])[0].split(",")[-1]: fgl.upstreams
            for fgl in lineage.fineGrainedLineages
        }
        assert by_downstream["id)"] == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.analytics.orders,PROD),id)"
        ]
        # A field without `sql` is assumed to map onto a same-named column.
        assert by_downstream["count)"] == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.analytics.orders,PROD),count)"
        ]

    def test_derived_sql_view_lineage_and_view_logic(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project))
        facts_urn = dataset_urn("my_project.view.order_facts")

        view_props = run.aspects(ViewPropertiesClass)[facts_urn]
        assert isinstance(view_props, ViewPropertiesClass)
        assert view_props.viewLanguage == "sql"
        assert "COUNT(*) AS order_count" in view_props.viewLogic

        lineage = run.aspects(UpstreamLineageClass)[facts_urn]
        assert isinstance(lineage, UpstreamLineageClass)
        assert [u.dataset for u in lineage.upstreams] == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.analytics.orders,PROD)"
        ]

    def test_custom_properties_and_external_url_from_git_info(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(
            base_config(
                project,
                git_info={"repo": "example-org/lookml-repo", "branch": "main"},
            )
        )
        props = run.aspects(DatasetPropertiesClass)[
            dataset_urn("my_project.view.orders")
        ]
        assert isinstance(props, DatasetPropertiesClass)
        assert props.customProperties == {
            "looker.file.path": "views/orders.view.lkml",
            "looker.model": "sales",
        }
        assert (
            props.externalUrl
            == "https://github.com/example-org/lookml-repo/blob/main/views/orders.view.lkml"
        )

    def test_tag_definitions_emitted_once_views_are_produced(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project))
        tag_urns = [
            wu.get_urn()
            for wu in run.workunits
            if isinstance(wu, MetadataWorkUnit)
            and wu.get_urn().startswith("urn:li:tag:")
        ]
        assert set(tag_urns) == {
            "urn:li:tag:Dimension",
            "urn:li:tag:Temporal",
            "urn:li:tag:Measure",
        }

    def test_tags_disabled_puts_field_type_in_description(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project, tag_measures_and_dimensions=False))
        schema = run.aspects(SchemaMetadataClass)[dataset_urn("my_project.view.orders")]
        assert isinstance(schema, SchemaMetadataClass)
        count_field = next(f for f in schema.fields if f.fieldPath == "count")
        assert (count_field.description or "").startswith("Measure.")
        assert count_field.globalTags is None
        assert run.aspects(GlobalTagsClass) == {}


class TestLookMLSourceFiltering:
    def test_unreferenced_view_file_is_reported_not_emitted(
        self, project: pathlib.Path
    ) -> None:
        run = run_source(base_config(project, emit_reachable_views_only=False))
        assert dataset_urn("my_project.view.orphan") not in run.urns_with(
            ViewPropertiesClass
        )
        skipped = [
            w for w in run.source.reporter.warnings if w.title == "Skipped View File"
        ]
        assert len(skipped) == 1
        assert "orphan.view.lkml" in skipped[0].context[0]

    def test_included_but_unreachable_view_is_dropped(
        self, project: pathlib.Path
    ) -> None:
        write(
            project / "views" / "lonely.view.lkml",
            "view: lonely { sql_table_name: analytics.lonely ;; dimension: id { type: number sql: ${TABLE}.id ;; } }",
        )
        run = run_source(base_config(project))
        assert dataset_urn("my_project.view.lonely") not in run.urns_with(
            ViewPropertiesClass
        )
        assert "lonely" in run.source.reporter.views_dropped_unreachable

    def test_view_pattern_drops_views(self, project: pathlib.Path) -> None:
        run = run_source(base_config(project, view_pattern={"deny": ["^customers$"]}))
        assert dataset_urn("my_project.view.customers") not in run.urns_with(
            ViewPropertiesClass
        )
        assert len(run.source.reporter.views_dropped) == 1
        assert "customers" in run.source.reporter.views_dropped[0]

    def test_model_pattern_drops_models(self, project: pathlib.Path) -> None:
        run = run_source(base_config(project, model_pattern={"deny": ["^sales$"]}))
        assert run.urns_with(ViewPropertiesClass) == []
        assert list(run.source.reporter.models_dropped) == ["sales"]
        assert run.source.reporter.models_discovered == 1

    def test_unknown_connection_without_api_drops_model(
        self, project: pathlib.Path
    ) -> None:
        write(
            project / "models" / "other.model.lkml",
            'connection: "unknown_connection"\ninclude: "/views/customers.view.lkml"\nexplore: customers {}',
        )
        run = run_source(base_config(project))
        assert list(run.source.reporter.models_dropped) == ["other"]
        assert any(
            w.title == "Failed to Load Connection" for w in run.source.reporter.warnings
        )
        # The healthy model is unaffected.
        assert dataset_urn("my_project.view.orders") in run.urns_with(
            ViewPropertiesClass
        )

    def test_unparseable_model_is_reported_and_skipped(
        self, project: pathlib.Path
    ) -> None:
        write(project / "models" / "broken.model.lkml", "connection: {{{ not lookml")
        run = run_source(base_config(project))
        assert any(
            w.title == "Error Loading Model File" for w in run.source.reporter.warnings
        )
        assert dataset_urn("my_project.view.orders") in run.urns_with(
            ViewPropertiesClass
        )

    def test_view_seen_via_second_connection_is_emitted_once(
        self, project: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        write(
            project / "models" / "other.model.lkml",
            'connection: "other_connection"\ninclude: "/views/customers.view.lkml"\nexplore: customers {}',
        )
        config = base_config(project)
        connections = config["connection_to_platform_map"]
        assert isinstance(connections, dict)
        connections["other_connection"] = {
            "platform": "snowflake",
            "default_db": "db",
            "default_schema": "schema",
        }
        with caplog.at_level(logging.WARNING):
            run = run_source(config)
        customers_urn = dataset_urn("my_project.view.customers")
        emitted = [
            wu
            for wu in run.workunits
            if isinstance(wu, MetadataWorkUnit)
            and wu.get_urn() == customers_urn
            and wu.get_aspect_of_type(ViewPropertiesClass) is not None
        ]
        assert len(emitted) == 1
        assert "was previously processed via model" in caplog.text


class TestLookMLSourceProjects:
    def test_local_dependency_views_keep_their_project(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = make_project(tmp_path / "repo")
        write(
            root / "manifest.lkml",
            MANIFEST + '\nlocal_dependency: {\n  project: "dep_project"\n}\n',
        )
        dep = tmp_path / "dep"
        write(
            dep / "views" / "regions.view.lkml",
            "view: regions { sql_table_name: analytics.regions ;; dimension: id { type: number sql: ${TABLE}.id ;; } }",
        )
        write(dep / "manifest.lkml", 'project_name: "dep_project"')
        write(
            root / "models" / "geo.model.lkml",
            'connection: "my_connection"\ninclude: "//dep_project/views/regions.view.lkml"\nexplore: regions {}',
        )

        run = run_source(
            base_config(root, project_dependencies={"dep_project": str(dep)})
        )
        assert dataset_urn("dep_project.view.regions") in run.urns_with(
            ViewPropertiesClass
        )
        assert set(run.source.processed_projects) == {"my_project", "dep_project"}
        props = run.aspects(DatasetPropertiesClass)[
            dataset_urn("dep_project.view.regions")
        ]
        assert isinstance(props, DatasetPropertiesClass)
        assert props.customProperties["looker.file.path"] == "views/regions.view.lkml"

    def test_manifest_project_name_mismatch_is_logged(
        self, project: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            run_source(base_config(project, project_name="configured_name"))
        assert "does not match the configured project name" in caplog.text

    def test_missing_dependency_folder_is_logged(
        self, project: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        write(
            project / "manifest.lkml",
            MANIFEST + '\nlocal_dependency: {\n  project: "missing_dep"\n}\n',
        )
        with caplog.at_level(logging.WARNING):
            run_source(base_config(project))
        assert "Could not find missing_dep in the project_dependencies" in caplog.text

    def test_repo_is_cloned_when_no_base_folder(self, tmp_path: pathlib.Path) -> None:
        checkout = make_project(tmp_path / "checkout")
        config = base_config(checkout)
        del config["base_folder"]
        config["git_info"] = {"repo": "example-org/lookml-repo", "branch": "main"}

        with mock.patch.object(GitInfo, "clone", return_value=checkout) as clone:
            run = run_source(config)

        clone.assert_called_once()
        assert run.source.reporter.git_clone_latency is not None
        assert dataset_urn("my_project.view.orders") in run.urns_with(
            ViewPropertiesClass
        )

    def test_dependency_clone_failure_is_tolerated(
        self, project: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = base_config(
            project,
            project_dependencies={
                "dep_project": {"repo": "example-org/dep", "branch": "main"}
            },
        )
        with (
            mock.patch.object(GitInfo, "clone", side_effect=RuntimeError("no ssh")),
            caplog.at_level(logging.WARNING),
        ):
            run = run_source(config)
        assert "Failed to clone project dependency dep_project" in caplog.text
        assert "dep_project" not in run.source.base_projects_folder
        assert dataset_urn("my_project.view.orders") in run.urns_with(
            ViewPropertiesClass
        )


class TestProjectNameFromApi:
    def _api_config(self, project: pathlib.Path) -> Dict[str, object]:
        config = base_config(project)
        del config["project_name"]
        config["api"] = {
            "client_id": "id",
            "client_secret": "secret",
            "base_url": "https://looker.example.com",
        }
        return config

    def test_project_name_resolved_through_lookml_model(
        self, project: pathlib.Path
    ) -> None:
        client = mock.MagicMock()
        client.lookml_model.return_value = LookmlModel(
            name="sales", project_name="api_project"
        )
        # manifest.lkml names the project; without it the API is consulted.
        (project / "manifest.lkml").unlink()
        with mock.patch("looker_sdk.init40", return_value=client):
            run = run_source(self._api_config(project))
        assert dataset_urn("api_project.view.orders") in run.urns_with(
            ViewPropertiesClass
        )
        client.lookml_model.assert_called_once()

    def test_project_name_lookup_failure_reports_and_aborts(
        self, project: pathlib.Path
    ) -> None:
        client = mock.MagicMock()
        client.lookml_model.side_effect = SDKError("boom")
        (project / "manifest.lkml").unlink()
        with mock.patch("looker_sdk.init40", return_value=client):
            source = LookMLSource(
                LookMLSourceConfig.model_validate(self._api_config(project)),
                PipelineContext(run_id="t"),
            )
            with pytest.raises(ValueError, match="project name"):
                list(source.get_workunits())
        assert any(
            f.title == "Failed to find a project name for model"
            for f in source.reporter.failures
        )

    def test_connections_permission_failure_rejects_source(
        self, project: pathlib.Path
    ) -> None:
        client = mock.MagicMock()
        client.all_connections.side_effect = SDKError("forbidden")
        with (
            mock.patch("looker_sdk.init40", return_value=client),
            pytest.raises(ValueError, match="manage_models"),
        ):
            LookMLSource(
                LookMLSourceConfig.model_validate(self._api_config(project)),
                PipelineContext(run_id="t"),
            )


class TestViewExploreOptimisation:
    def test_views_assigned_to_largest_shared_explore(
        self, project: pathlib.Path
    ) -> None:
        source = LookMLSource(
            LookMLSourceConfig.model_validate(base_config(project)),
            PipelineContext(run_id="t"),
        )
        mapping = source._optimize_views_by_common_explore(
            view_to_explores={
                "orders": {"orders_explore", "small_explore"},
                "customers": {"orders_explore"},
                "items": {"orders_explore"},
                "regions": {"small_explore"},
                "unused": set(),
            },
            explore_to_views={
                "orders_explore": {"orders", "customers", "items"},
                "small_explore": {"orders", "regions"},
            },
        )
        assert mapping == {
            "orders": "orders_explore",
            "customers": "orders_explore",
            "items": "orders_explore",
            "regions": "small_explore",
        }

    def test_empty_explores_yield_empty_mapping(self, project: pathlib.Path) -> None:
        source = LookMLSource(
            LookMLSourceConfig.model_validate(base_config(project)),
            PipelineContext(run_id="t"),
        )
        assert source._optimize_views_by_common_explore({}, {}) == {}
