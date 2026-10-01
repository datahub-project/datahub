"""ADF's probe: names judged as ingestion judges them, and records that are
projections -- a linked service's definition is where ADF keeps credentials."""

import json
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional, cast

import pytest
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.mgmt.datafactory import models as adf

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import _iter_specs
from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    ADF_ACTIVITY_KIND,
    ADF_PIPELINE_KIND,
    AzureDataFactoryConfig,
)
from datahub.ingestion.source.azure_data_factory.adf_probe import (
    AzureDataFactoryMetadataProbe,
)
from datahub.ingestion.source.azure_data_factory.adf_source import (
    AzureDataFactorySource,
)
from datahub.ingestion.source.common.subtypes import FlowContainerSubTypes

SUB = "00000000-0000-0000-0000-000000000000"
PLANTED = "PLANTED-SECRET-VALUE"
_BASE_RECIPE: Dict[str, Any] = {
    "subscription_id": SUB,
    "credential": {"authentication_method": "cli"},
}


def _factory_id(rg: str, name: str) -> str:
    return (
        f"/subscriptions/{SUB}/resourceGroups/{rg}"
        f"/providers/Microsoft.DataFactory/factories/{name}"
    )


def _factory(name: Optional[str], rg: str = "my-rg") -> adf.Factory:
    factory = adf.Factory(
        location="westeurope",
        global_parameters={
            "api_key": adf.GlobalParameterSpecification(type="String", value=PLANTED)
        },
    )
    # name/id are readonly on the SDK model, so they are set after construction,
    # which is how the deserializer populates them.
    factory.name = name
    factory.id = _factory_id(rg, name) if name else None
    return factory


def _forbidden() -> Iterator[Any]:
    # An Azure pager raises on iteration, not on the call -- the fake has to as well.
    raise HttpResponseError(
        message="forbidden",
        response=SimpleNamespace(
            status_code=403,
            reason="Forbidden",
            text=lambda: "",
            headers={},
            content_type="application/json",
            request=None,
        ),
    )
    yield  # pragma: no cover


class _FakeClient:
    def __init__(self, factories: Optional[List[adf.Factory]] = None) -> None:
        self.factories = (
            factories if factories is not None else [_factory("my-factory")]
        )
        self.pipelines: Dict[str, Any] = {}
        self.linked_services: Dict[str, Any] = {}
        self.datasets: Dict[str, Any] = {}
        self.factory_listings: List[Optional[str]] = []
        self.closed = False

    def get_factories(
        self, resource_group: Optional[str] = None
    ) -> Iterator[adf.Factory]:
        self.factory_listings.append(resource_group)
        for f in self.factories:
            if resource_group is None or (f.id or "").split("/")[4] == resource_group:
                yield f

    def get_pipelines(self, resource_group: str, factory_name: str) -> Any:
        return self.pipelines.get(factory_name, iter([]))

    def get_pipeline(
        self, resource_group: str, factory_name: str, pipeline_name: str
    ) -> Any:
        found = self.pipelines.get(factory_name)
        if callable(found):
            return found()
        for p in found or []:
            if p.name == pipeline_name:
                return p
        raise ResourceNotFoundError(message=f"{pipeline_name} not found")

    def get_linked_services(self, resource_group: str, factory_name: str) -> Any:
        return self.linked_services.get(factory_name, iter([]))

    def get_datasets(self, resource_group: str, factory_name: str) -> Any:
        return self.datasets.get(factory_name, iter([]))

    def close(self) -> None:
        self.closed = True


class _FakeCredential:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _config(**overrides: Any) -> AzureDataFactoryConfig:
    return AzureDataFactoryConfig.model_validate({**_BASE_RECIPE, **overrides})


def _probe(
    client: _FakeClient, credential: Optional[_FakeCredential] = None, **overrides: Any
) -> AzureDataFactoryMetadataProbe:
    source = AzureDataFactorySource.for_probe(
        _config(**overrides), cast(AzureDataFactoryClient, client)
    )
    return AzureDataFactoryMetadataProbe(source, credential or _FakeCredential())


def test_factories_include_ones_factory_pattern_would_exclude() -> None:
    client = _FakeClient([_factory("prod-sales"), _factory("dev-scratch")])
    probe = _probe(client, factory_pattern={"allow": ["^prod-.*"]})
    assert [f["name"] for f in probe.factories()] == ["prod-sales", "dev-scratch"]


def test_a_factory_record_is_a_projection_that_never_carries_global_parameters() -> (
    None
):
    [record] = _probe(_FakeClient()).factories()
    assert record == {
        "name": "my-factory",
        "resource_group": "my-rg",
        "location": "westeurope",
    }
    assert PLANTED not in json.dumps(record)


def test_resource_group_narrows_the_listing_and_says_so() -> None:
    client = _FakeClient([_factory("a", rg="rg-one"), _factory("b", rg="rg-two")])
    probe = _probe(client, resource_group="rg-one")
    assert [f["name"] for f in probe.factories()] == ["a"]
    assert client.factory_listings == ["rg-one"]
    assert any("resource_group" in w for w in probe.warnings)


def test_a_nameless_factory_is_skipped_as_ingestion_skips_it_and_counted() -> None:
    probe = _probe(_FakeClient([_factory(None), _factory("my-factory")]))
    assert [f["name"] for f in probe.factories()] == ["my-factory"]
    assert any("no name" in w for w in probe.warnings)


def test_the_provider_closes_the_client_and_the_credential() -> None:
    client, credential = _FakeClient(), _FakeCredential()
    with _probe(client, credential):
        pass
    assert client.closed and credential.closed


def test_for_config_builds_through_the_ingestion_credential_without_connecting() -> (
    None
):
    # CLI credential construction touches nothing; a network call here would hang/raise.
    with AzureDataFactoryMetadataProbe.for_config(_config()) as probe:
        assert probe.warnings == []


def test_factory_verdicts_use_factory_pattern_on_the_bare_name() -> None:
    result = check_filters(
        source_type="azure-data-factory",
        config_dict={**_BASE_RECIPE, "factory_pattern": {"allow": ["^prod-.*"]}},
        kind=str(FlowContainerSubTypes.ADF_DATA_FACTORY),
        parent_path=[],
        names=["prod-sales", "dev-scratch"],
    )
    assert result.pattern_field == "factory_pattern"
    assert {v.name: v.included for v in result.results} == {
        "prod-sales": True,
        "dev-scratch": False,
    }
    assert [v.target for v in result.results] == ["prod-sales", "dev-scratch"]


def test_a_pipeline_under_a_denied_factory_is_excluded_by_factory_pattern() -> None:
    result = check_filters(
        source_type="azure-data-factory",
        config_dict={**_BASE_RECIPE, "factory_pattern": {"deny": ["^dev-.*"]}},
        kind=ADF_PIPELINE_KIND,
        parent_path=["dev-scratch"],
        names=["sales_pipeline"],
    )
    assert result.results[0].excluded_by == "factory_pattern"


def test_pipeline_verdicts_are_not_qualified_by_the_factory() -> None:
    # adf_source.py matches pipeline_pattern against the bare pipeline name.
    result = check_filters(
        source_type="azure-data-factory",
        config_dict={**_BASE_RECIPE, "pipeline_pattern": {"allow": ["^sales_.*"]}},
        kind=ADF_PIPELINE_KIND,
        parent_path=["my-factory"],
        names=["sales_pipeline", "hr_pipeline"],
    )
    assert result.pattern_field == "pipeline_pattern"
    assert [v.target for v in result.results] == ["sales_pipeline", "hr_pipeline"]
    assert [v.included for v in result.results] == [True, False]


def test_activities_are_unfiltered_but_inherit_their_pipelines_exclusion() -> None:
    result = check_filters(
        source_type="azure-data-factory",
        config_dict={**_BASE_RECIPE, "pipeline_pattern": {"deny": ["^hr_.*"]}},
        kind=ADF_ACTIVITY_KIND,
        parent_path=["my-factory", "hr_pipeline"],
        names=["CopyRows"],
    )
    assert result.filtering == "unfiltered"
    assert result.results[0].excluded_by == "pipeline_pattern"


def _pipeline(
    name: Optional[str],
    activities: Optional[List[Any]] = None,
    folder: Optional[str] = None,
) -> adf.PipelineResource:
    pipeline = adf.PipelineResource(
        activities=activities or [],
        folder=adf.PipelineFolder(name=folder) if folder else None,
        parameters={
            "token": adf.ParameterSpecification(type="String", default_value=PLANTED)
        },
    )
    pipeline.name = name
    return pipeline


def test_pipelines_include_ones_pipeline_pattern_would_exclude() -> None:
    client = _FakeClient()
    client.pipelines["my-factory"] = iter(
        [_pipeline("sales_pipeline"), _pipeline("hr_pipeline")]
    )
    probe = _probe(client, pipeline_pattern={"allow": ["^sales_.*"]})
    assert [p["name"] for p in probe.pipelines("my-factory")] == [
        "sales_pipeline",
        "hr_pipeline",
    ]


def test_a_pipeline_record_never_carries_parameter_defaults() -> None:
    client = _FakeClient()
    client.pipelines["my-factory"] = iter(
        [_pipeline("sales_pipeline", folder="finance")]
    )
    [record] = _probe(client).pipelines("my-factory")
    assert record == {
        "name": "sales_pipeline",
        "folder": "finance",
        "activity_count": 0,
    }
    assert PLANTED not in json.dumps(record)


def test_a_nameless_pipeline_is_skipped_and_counted() -> None:
    client = _FakeClient()
    client.pipelines["my-factory"] = iter(
        [_pipeline(None), _pipeline("sales_pipeline")]
    )
    probe = _probe(client)
    assert [p["name"] for p in probe.pipelines("my-factory")] == ["sales_pipeline"]
    assert any("no name" in w for w in probe.warnings)


def test_a_forbidden_pipeline_listing_degrades_with_a_warning() -> None:
    client = _FakeClient()
    client.pipelines["my-factory"] = _forbidden()
    probe = _probe(client)
    assert probe.pipelines("my-factory") == []
    assert any("403" in w for w in probe.warnings)


def test_an_unknown_factory_is_a_bad_argument() -> None:
    with pytest.raises(ValueError):
        _probe(_FakeClient()).pipelines("no-such-factory")


def test_a_factory_outside_resource_group_is_a_bad_argument_that_names_the_narrowing() -> (
    None
):
    client = _FakeClient([_factory("elsewhere", rg="rg-two")])
    # The recipe field name is the contract with the caller, not message wording.
    with pytest.raises(ValueError, match="resource_group"):
        _probe(client, resource_group="rg-one").pipelines("elsewhere")


def test_pipelines_report_their_factory_as_the_parent() -> None:
    specs = dict(_iter_specs(AzureDataFactoryMetadataProbe))
    assert specs["pipelines"].parent_params == ("factory",)
    assert specs["pipelines"].kind == ADF_PIPELINE_KIND
