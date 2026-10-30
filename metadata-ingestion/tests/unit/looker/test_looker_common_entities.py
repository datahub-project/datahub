"""looker_common behaviours not covered by the explore-field tests: view field
de-duplication, LookML `extends` resolution, tag definitions, the user
registry and explore fetch failures."""

import datetime
from typing import Callable, Dict, List, Optional
from unittest import mock

import pytest
from looker_sdk.error import SDKError
from looker_sdk.rtl.serialize import DeserializeError
from looker_sdk.sdk.api40.models import User

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.looker.looker_common import (
    LookerDashboardSourceReport,
    LookerExplore,
    LookerUser,
    LookerUserRegistry,
    LookerUtil,
    LookerViewId,
    StageLatency,
    ViewField,
    ViewFieldType,
    deduplicate_fields,
)
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
    LookerDashboardSourceConfig,
    LookerViewNamingPattern,
)
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerViewFile,
    ProjectInclude,
)
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceReport,
)
from datahub.metadata.schema_classes import (
    NullTypeClass,
    NumberTypeClass,
)


def _field(
    name: str,
    field_type: ViewFieldType,
    group_label: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> ViewField:
    return ViewField(
        name=name,
        label="",
        type="string",
        description="",
        field_type=field_type,
        group_label=group_label,
        tags=tags or [],
    )


class TestDeduplicateFields:
    def test_dimension_group_wins_over_same_named_dimension(self) -> None:
        fields = [
            _field("created", ViewFieldType.DIMENSION),
            _field("created", ViewFieldType.DIMENSION_GROUP),
            _field("count", ViewFieldType.MEASURE),
        ]
        kept = deduplicate_fields(fields)
        assert [(f.name, f.field_type) for f in kept] == [
            ("created", ViewFieldType.DIMENSION_GROUP),
            ("count", ViewFieldType.MEASURE),
        ]


class TestExploreFromDict:
    def _loader(self, views_by_file: Dict[str, List[str]]) -> mock.MagicMock:
        loader = mock.MagicMock(spec=LookerViewFileLoader)

        def load_viewfile(path: str, *args: object, **kwargs: object) -> LookerViewFile:
            return LookerViewFile(
                absolute_file_path=path,
                connection=None,
                includes=[],
                resolved_includes=[],
                views=[{"name": v} for v in views_by_file.get(path, [])],
                raw_file_content="",
            )

        loader.load_viewfile.side_effect = load_viewfile
        return loader

    def test_extended_explore_inherits_upstream_views(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``extends__all`` pulls in the base explore's ``from`` view and its
        joins; an extended explore missing from the model is logged, not fatal."""
        includes = [ProjectInclude(BASE_PROJECT_NAME, "views.lkml")]
        model_explores = {
            "base": {
                "name": "base",
                "from": "orders",
                "joins": [{"name": "customers"}],
            },
        }
        explore = LookerExplore.from_dict(
            "sales",
            {"name": "child", "extends__all": [["base", "missing"]]},
            includes,
            self._loader({"views.lkml": ["orders", "customers"]}),
            LookMLSourceReport(),
            model_explores,
        )
        assert explore.upstream_views is not None
        assert sorted(v.include for v in explore.upstream_views) == [
            "customers",
            "orders",
        ]
        assert "Could not find extended explore missing" in caplog.text

    def test_unresolvable_view_is_logged_and_skipped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        explore = LookerExplore.from_dict(
            "sales",
            {"name": "orders", "tags": ["t"]},
            [ProjectInclude(BASE_PROJECT_NAME, "views.lkml")],
            self._loader({"views.lkml": ["other"]}),
            LookMLSourceReport(),
            {},
        )
        assert explore.upstream_views == []
        assert explore.tags == ["t"]
        assert "Could not resolve view orders" in caplog.text


class TestExploreFromApiFailures:
    def _client(self, error: Exception) -> mock.MagicMock:
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.side_effect = error
        return client

    @pytest.fixture
    def config(
        self, looker_config_factory: Callable[..., LookerDashboardSourceConfig]
    ) -> LookerDashboardSourceConfig:
        return looker_config_factory()

    def test_non_404_sdk_error_is_logged_not_raised(
        self, caplog: pytest.LogCaptureFixture, config: LookerDashboardSourceConfig
    ) -> None:
        report = SourceReport()
        assert (
            LookerExplore.from_api(
                "sales", "orders", self._client(SDKError("500")), report, config
            )
            is None
        )
        assert "Failed to extract explore orders" in caplog.text
        assert not report.warnings

    def test_deserialize_error_is_reported(
        self, config: LookerDashboardSourceConfig
    ) -> None:
        report = SourceReport()
        assert (
            LookerExplore.from_api(
                "sales",
                "orders",
                self._client(DeserializeError("bad payload")),
                report,
                config,
            )
            is None
        )
        assert any(
            w.title == "Failed to fetch explore from the Looker API"
            for w in report.warnings
        )


class TestTagsAndTypes:
    def test_group_label_tag_definition_is_built_on_the_fly(self) -> None:
        aspect = LookerUtil._get_tag_properties("urn:li:tag:looker:group_label:Contact")
        assert aspect.name == "looker:group_label:Contact"
        assert aspect.description is not None and "Contact" in aspect.description

    def test_unknown_tag_gets_a_generic_definition(self) -> None:
        aspect = LookerUtil._get_tag_properties("urn:li:tag:custom")
        assert aspect.name == "custom"

    def test_group_label_and_explicit_tags_attach_to_schema_field(self) -> None:
        field = _field(
            "email", ViewFieldType.DIMENSION, group_label="Contact", tags=["pii"]
        )
        schema_field = LookerUtil.view_field_to_schema_field(field, SourceReport())
        assert schema_field.globalTags is not None
        assert [t.tag for t in schema_field.globalTags.tags] == [
            "urn:li:tag:pii",
            "urn:li:tag:Dimension",
            "urn:li:tag:looker:group_label:Contact",
        ]

    def test_unknown_native_types_fall_back_to_null_type(self) -> None:
        assert isinstance(LookerUtil.get_field_type("sum").type, NumberTypeClass)
        assert isinstance(
            LookerUtil.get_field_type("numeric(10,2)").type, NumberTypeClass
        )
        assert isinstance(LookerUtil.get_field_type("mystery").type, NullTypeClass)


class TestViewIdPaths:
    def _config(self, browse_pattern: str) -> LookerCommonConfig:
        return LookerCommonConfig(
            view_browse_pattern=LookerViewNamingPattern(pattern=browse_pattern)
        )

    def test_parent_container_follows_folder_path(self) -> None:
        view_id = LookerViewId(
            project_name="proj",
            model_name="m",
            view_name="orders",
            file_path="views/sales/orders.view.lkml",
        )
        parents = view_id.get_view_dataset_parent_container(
            self._config("/Develop/{project}/{folder_path}")
        )
        assert parents[0] == "Develop"
        assert parents[1].startswith("urn:li:container:")
        assert parents[2:] == ["views", "sales"]

        browse = view_id.get_browse_path_v2(self._config("/Develop/{file_path}"))
        assert [entry.id for entry in browse.path][2:] == ["views", "sales", "orders"]

    def test_malformed_imported_path_falls_back_to_view_project(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        view_id = LookerViewId(
            project_name="proj",
            model_name="m",
            view_name="orders",
            file_path="imported_projects/",
        )
        assert (
            view_id.preprocess_file_path("imported_projects/") == "imported_projects."
        )
        assert "Malformed imported_projects path" in caplog.text


class TestUserRegistry:
    def _registry(self, known: List[User], lookup: object) -> LookerUserRegistry:
        api = mock.MagicMock(spec=LookerAPI)
        api.all_users.return_value = known
        api.get_user.side_effect = lookup
        return LookerUserRegistry(api, LookerDashboardSourceReport())

    def test_users_missing_from_the_initial_listing_are_fetched(self) -> None:
        registry = self._registry(
            [User(id="1", email="one@example.com")],
            lambda id_, user_fields: (
                User(id=id_, email=f"{id_}@example.com") if id_ == "2" else None
            ),
        )
        assert registry.get_by_id("") is None
        cached = registry.get_by_id("1")
        assert cached is not None and cached.email == "one@example.com"
        fetched = registry.get_by_id("2")
        assert fetched is not None and fetched.email == "2@example.com"
        assert registry.get_by_id("3") is None

    def test_platform_resource_maps_ids_to_emails(self) -> None:
        registry = self._registry(
            [User(id="1", email="one@example.com"), User(id="2", email=None)],
            lambda id_, user_fields: None,
        )
        mcps = list(registry.to_platform_resource(platform_instance=None))
        assert mcps
        # Only users with an email make it into the id -> email mapping.
        assert registry.report.looker_user_count == 1

    def test_user_cache_is_not_shared_between_registries(self) -> None:
        """Regression: the user cache was a class-level dict, so users listed
        by one ingestion run were served to every later registry in the process."""
        self._registry(
            [User(id="1", email="one@example.com")], lambda id_, user_fields: None
        )
        later = self._registry([], lambda id_, user_fields: None)

        assert later.get_by_id("1") is None

    def test_user_urn_can_strip_email_domain(self) -> None:
        user = LookerUser("1", "one@example.com", None, None, None)
        assert user.get_urn(strip_user_ids_from_email=True) == "urn:li:corpuser:one"
        assert LookerUser("2", None, None, None, None).get_urn(False) is None


def test_stage_latency_is_computed_once_a_stage_ends() -> None:
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    stage = StageLatency(name="s", start_time=start)
    stage.end_time = start + datetime.timedelta(seconds=3)
    stage.compute_stats()
    assert stage.latency == datetime.timedelta(seconds=3)
    assert stage.start_time is None
