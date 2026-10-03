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
from datahub.ingestion.agent.verdicts import ProbeArgumentError
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


def _raw(value: object) -> Any:
    # ARM returns these properties as untyped JSON -- a connection string or a
    # header arrives as a plain string -- so the SDK's declared types
    # (MutableMapping, Key Vault references) don't describe what this fixture
    # must imitate.
    return value


def _factory_id(rg: str, name: str) -> str:
    return (
        f"/subscriptions/{SUB}/resourceGroups/{rg}"
        f"/providers/Microsoft.DataFactory/factories/{name}"
    )


def _factory(name: Optional[str], rg: str = "my-rg") -> adf.Factory:
    factory = adf.Factory(
        location="westeurope",
        global_parameters={
            "api_key": adf.GlobalParameterSpecification(
                type="String", value=_raw(PLANTED)
            )
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
            "token": adf.ParameterSpecification(
                type="String", default_value=_raw(PLANTED)
            )
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


def _copy(name: str, source_ds: str, sink_ds: str) -> adf.CopyActivity:
    return adf.CopyActivity(
        name=name,
        inputs=[
            adf.DatasetReference(type="DatasetReference", reference_name=source_ds)
        ],
        outputs=[adf.DatasetReference(type="DatasetReference", reference_name=sink_ds)],
        source=adf.AzureSqlSource(
            sql_reader_query=_raw(f"SELECT * FROM t WHERE k = '{PLANTED}'")
        ),
        sink=adf.AzureSqlSink(),
    )


def _web(name: str, depends_on: str) -> adf.WebActivity:
    return adf.WebActivity(
        name=name,
        method="GET",
        url=_raw("https://example.invalid/hook"),
        headers=_raw({"Authorization": f"Bearer {PLANTED}"}),
        depends_on=[
            adf.ActivityDependency(
                activity=depends_on, dependency_conditions=["Succeeded"]
            )
        ],
    )


def _with_pipeline(activities: List[Any]) -> _FakeClient:
    client = _FakeClient()
    client.pipelines["my-factory"] = [_pipeline("sales_pipeline", activities)]
    return client


def test_activities_are_listed_with_the_subtype_ingestion_stamps() -> None:
    probe = _probe(
        _with_pipeline(
            [_copy("CopyRows", "src_ds", "dst_ds"), _web("Notify", "CopyRows")]
        )
    )
    assert probe.activities("my-factory", "sales_pipeline") == [
        {
            "name": "CopyRows",
            "type": "Copy",
            "subtype": "Copy Activity",
            "depends_on": [],
            "inputs": ["src_ds"],
            "outputs": ["dst_ds"],
        },
        {
            "name": "Notify",
            "type": "WebActivity",
            "subtype": "Web Activity",
            "depends_on": ["CopyRows"],
            "inputs": [],
            "outputs": [],
        },
    ]


def test_an_activity_record_never_carries_headers_or_sql_text() -> None:
    probe = _probe(
        _with_pipeline([_copy("CopyRows", "a", "b"), _web("Notify", "CopyRows")])
    )
    assert PLANTED not in json.dumps(probe.activities("my-factory", "sales_pipeline"))


def test_nested_activities_are_counted_and_warned_about() -> None:
    branch = adf.IfConditionActivity(
        name="IfBig",
        expression=adf.Expression(type="Expression", value="@true"),
        if_true_activities=[_copy("CopyThree", "e", "f")],
    )
    loop = adf.ForEachActivity(
        name="EachTable",
        items=adf.Expression(type="Expression", value="@pipeline().parameters.t"),
        activities=[_copy("CopyOne", "a", "b"), branch],
    )
    probe = _probe(_with_pipeline([loop]))
    [record] = probe.activities("my-factory", "sales_pipeline")
    # CopyOne, IfBig and CopyThree, two levels deep.
    assert record["nested_activities"] == 3
    assert any("nested" in w for w in probe.warnings)


def test_an_unknown_pipeline_is_a_bad_argument() -> None:
    with pytest.raises(ValueError):
        _probe(_with_pipeline([])).activities("my-factory", "no_such_pipeline")


def test_activities_report_factory_and_pipeline_as_parents() -> None:
    spec = dict(_iter_specs(AzureDataFactoryMetadataProbe))["activities"]
    assert spec.parent_params == ("factory", "pipeline")
    assert spec.kind == ADF_ACTIVITY_KIND


def _linked_service(name: str, props: Any) -> adf.LinkedServiceResource:
    resource = adf.LinkedServiceResource(properties=props)
    resource.name = name
    return resource


def _blob_ls(name: str = "blob_ls") -> adf.LinkedServiceResource:
    return _linked_service(
        name,
        adf.AzureBlobStorageLinkedService(
            connection_string=_raw(
                f"DefaultEndpointsProtocol=https;AccountName=acct;AccountKey={PLANTED}"
            ),
            connect_via=adf.IntegrationRuntimeReference(
                type="IntegrationRuntimeReference", reference_name="my-ir"
            ),
        ),
    )


def _sql_ls(name: str = "sql_ls") -> adf.LinkedServiceResource:
    return _linked_service(
        name,
        adf.AzureSqlDatabaseLinkedService(
            connection_string=_raw("Server=tcp:example.invalid;Database=my_db;"),
            password=_raw(adf.SecureString(value=PLANTED)),
        ),
    )


def test_linked_services_resolve_the_platform_and_instance_ingestion_uses() -> None:
    client = _FakeClient()
    client.linked_services["my-factory"] = iter([_blob_ls(), _sql_ls()])
    probe = _probe(client, platform_instance_map={"sql_ls": "prod_mssql"})
    assert probe.linked_services("my-factory") == [
        {
            "name": "blob_ls",
            "type": "AzureBlobStorage",
            "platform": "abs",
            "platform_instance": None,
            "integration_runtime": "my-ir",
        },
        {
            "name": "sql_ls",
            "type": "AzureSqlDatabase",
            "platform": "mssql",
            "platform_instance": "prod_mssql",
            "integration_runtime": None,
        },
    ]


def test_an_unmapped_linked_service_type_reports_no_platform() -> None:
    client = _FakeClient()
    client.linked_services["my-factory"] = iter(
        [
            _linked_service(
                "odata_ls", adf.ODataLinkedService(url=_raw("https://x.invalid"))
            )
        ]
    )
    [record] = _probe(client).linked_services("my-factory")
    assert record["platform"] is None


def test_a_linked_service_record_never_carries_its_connection_details() -> None:
    client = _FakeClient()
    client.linked_services["my-factory"] = iter([_blob_ls(), _sql_ls()])
    out = json.dumps(_probe(client).linked_services("my-factory"))
    assert PLANTED not in out
    assert "AccountKey" not in out and "Server=" not in out


def test_linked_services_say_when_ingestion_will_not_read_them() -> None:
    client = _FakeClient()
    client.linked_services["my-factory"] = iter([_blob_ls()])
    probe = _probe(client, include_lineage=False)
    probe.linked_services("my-factory")
    assert any("include_lineage" in w for w in probe.warnings)


def _dataset(name: str, props: Any) -> adf.DatasetResource:
    resource = adf.DatasetResource(properties=props)
    resource.name = name
    return resource


def _ls_ref(name: str) -> adf.LinkedServiceReference:
    return adf.LinkedServiceReference(
        type="LinkedServiceReference", reference_name=name
    )


def _factory_with_datasets(datasets: List[Any]) -> _FakeClient:
    client = _FakeClient()
    client.datasets["my-factory"] = iter(datasets)
    client.linked_services["my-factory"] = iter([_blob_ls(), _sql_ls()])
    return client


def _orders_table() -> adf.DatasetResource:
    return _dataset(
        "orders_ds",
        adf.AzureSqlTableDataset(
            linked_service_name=_ls_ref("sql_ls"),
            schema_type_properties_schema=_raw("dbo"),
            table=_raw("orders"),
        ),
    )


def test_datasets_resolve_to_the_urn_ingestion_emits() -> None:
    table = _orders_table()
    probe = _probe(
        _factory_with_datasets([table]),
        platform_instance_map={"sql_ls": "prod_mssql"},
    )
    [record] = probe.datasets("my-factory")
    assert record["platform"] == "mssql"
    assert record["unresolved_reason"] is None
    # Resolved by the ingestion function on an identically primed source, so the
    # probe cannot drift from what a run emits.
    source = AzureDataFactorySource.for_probe(
        _config(platform_instance_map={"sql_ls": "prod_mssql"}),
        cast(AzureDataFactoryClient, _FakeClient()),
    )
    source._datasets_cache["my-rg/my-factory"] = {"orders_ds": table}
    source._linked_services_cache["my-rg/my-factory"] = {"sql_ls": _sql_ls()}
    assert record["urn"] == str(
        source._resolve_dataset_urn("orders_ds", "my-rg/my-factory")
    )
    assert "prod_mssql" in str(record["urn"])


def test_a_dataset_on_an_unmapped_linked_service_says_why_it_will_not_resolve() -> None:
    client = _FakeClient()
    client.datasets["my-factory"] = iter(
        [
            _dataset(
                "feed_ds", adf.ODataResourceDataset(linked_service_name=_ls_ref("o"))
            )
        ]
    )
    client.linked_services["my-factory"] = iter(
        [_linked_service("o", adf.ODataLinkedService(url=_raw("https://x.invalid")))]
    )
    probe = _probe(client)
    [record] = probe.datasets("my-factory")
    assert record["urn"] is None
    assert "OData" in str(record["unresolved_reason"])
    # The connector's own report.warning() reaches the result through probe_report.
    assert len(probe.probe_report.warnings) == 1


def test_a_dataset_whose_linked_services_could_not_be_listed_says_so() -> None:
    client = _FakeClient()
    client.datasets["my-factory"] = iter([_orders_table()])
    client.linked_services["my-factory"] = _forbidden()
    probe = _probe(client)
    [record] = probe.datasets("my-factory")
    assert record["urn"] is None
    assert "could not be listed" in str(record["unresolved_reason"])
    assert any("403" in w for w in probe.warnings)


def test_a_dataset_record_never_carries_request_headers() -> None:
    http = _dataset(
        "api_ds",
        adf.HttpDataset(
            linked_service_name=_ls_ref("blob_ls"),
            relative_url=_raw("/v1/items"),
            additional_headers=_raw(f"Authorization: Bearer {PLANTED}"),
            request_body=_raw(PLANTED),
        ),
    )
    out = _probe(_factory_with_datasets([http])).datasets("my-factory")
    assert PLANTED not in json.dumps(out)


def test_a_forbidden_listing_reports_the_failure_not_the_lineage_note() -> None:
    client = _FakeClient()
    client.linked_services["my-factory"] = _forbidden()
    client.datasets["my-factory"] = _forbidden()
    probe = _probe(client, include_lineage=False)
    assert probe.linked_services("my-factory") == []
    assert probe.datasets("my-factory") == []
    assert any("403" in w for w in probe.warnings)
    assert not any("include_lineage" in w for w in probe.warnings)


def test_a_dataset_without_properties_is_an_unresolved_row_not_a_crash() -> None:
    # The SDK model requires properties, but the probe already guards for a
    # reply without them, and the resolver must not be reached then.
    bare = SimpleNamespace(name="bare_ds", properties=None)
    records = _probe(_factory_with_datasets([bare, _orders_table()])).datasets(
        "my-factory"
    )
    assert [r["name"] for r in records] == ["bare_ds", "orders_ds"]
    assert records[0]["urn"] is None
    assert records[0]["unresolved_reason"]
    assert records[1]["urn"] is not None


def test_a_caller_naming_a_missing_factory_or_pipeline_gets_a_shown_refusal() -> None:
    # ProbeArgumentError, not a plain ValueError: only a framework type's text
    # reaches the caller, and these name what to fix.
    with pytest.raises(ProbeArgumentError, match="no-such-factory"):
        _probe(_FakeClient()).pipelines("no-such-factory")
    with pytest.raises(ProbeArgumentError, match="no_such_pipeline"):
        _probe(_with_pipeline([])).activities("my-factory", "no_such_pipeline")


def test_a_forbidden_pipeline_read_degrades_with_a_warning() -> None:
    client = _FakeClient()
    client.pipelines["my-factory"] = lambda: next(_forbidden())
    probe = _probe(client)
    assert probe.activities("my-factory", "sales_pipeline") == []
    assert any("sales_pipeline" in w and "403" in w for w in probe.warnings)


def _arm_error(body: object) -> HttpResponseError:
    return HttpResponseError(
        response=SimpleNamespace(
            status_code=403,
            reason="Forbidden",
            text=lambda encoding=None: json.dumps(body),
            headers={},
            content_type="application/json",
            request=None,
        )
    )


def test_a_failure_is_labelled_with_azures_error_code_never_its_message() -> None:
    planted = _arm_error(
        {"error": {"code": "AuthorizationFailed", "message": f"client {PLANTED}"}}
    )
    assert (
        AzureDataFactoryMetadataProbe.probe_error_code(planted) == "AuthorizationFailed"
    )
    # No ARM error body, or not an Azure error: the generic readers decide.
    assert AzureDataFactoryMetadataProbe.probe_error_code(_arm_error({})) is None
    assert AzureDataFactoryMetadataProbe.probe_error_code(ValueError(PLANTED)) is None
