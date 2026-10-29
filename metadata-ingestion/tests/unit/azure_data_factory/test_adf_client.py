"""AzureDataFactoryClient over a faked azure-mgmt-datafactory SDK client:
which SDK listing it calls, that SDK errors reach the caller, and that run
queries follow continuation tokens."""

from typing import Iterator, List, cast
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import HttpResponseError
from azure.mgmt.datafactory import models as adf

from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from tests.unit.azure_data_factory.conftest import (
    activity_run,
    adf_factory,
    pipeline_run,
)

_SDK_CLIENT = (
    "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient"
)


@pytest.fixture
def client() -> Iterator[AzureDataFactoryClient]:
    # The management client and the credential are Azure SDK objects, so
    # they stay unspecced; only their call shapes are asserted.
    with patch(_SDK_CLIENT):
        yield AzureDataFactoryClient(credential=MagicMock(), subscription_id="sub")


def _sdk(client: AzureDataFactoryClient) -> MagicMock:
    return cast(MagicMock, client._client)


def test_factories_are_listed_by_resource_group_only_when_one_is_given(
    client: AzureDataFactoryClient,
) -> None:
    sdk = _sdk(client)
    sdk.factories.list.return_value = iter([adf_factory("sub-wide")])
    sdk.factories.list_by_resource_group.return_value = iter([adf_factory("rg-only")])

    assert [f.name for f in client.get_factories()] == ["sub-wide"]
    assert [f.name for f in client.get_factories(resource_group="rg")] == ["rg-only"]
    sdk.factories.list_by_resource_group.assert_called_once_with(
        resource_group_name="rg"
    )


@pytest.mark.parametrize(
    "method_name, sdk_path",
    [
        ("get_pipelines", "pipelines.list_by_factory"),
        ("get_datasets", "datasets.list_by_factory"),
        ("get_linked_services", "linked_services.list_by_factory"),
        ("get_data_flows", "data_flows.list_by_factory"),
        ("get_triggers", "triggers.list_by_factory"),
        ("get_global_parameters", "global_parameters.list_by_factory"),
    ],
)
def test_factory_scoped_listings_pass_the_factory_and_propagate_sdk_errors(
    client: AzureDataFactoryClient, method_name: str, sdk_path: str
) -> None:
    """Each listing is a thin pager over the SDK: the factory coordinates must
    reach the SDK unchanged, and an HTTP error must surface to the caller
    rather than being swallowed into an empty listing."""
    sdk_method = _sdk(client)
    for attr in sdk_path.split("."):
        sdk_method = getattr(sdk_method, attr)
    sdk_method.return_value = iter([adf.PipelineResource()])
    listing = getattr(client, method_name)

    assert len(list(listing("rg", "factory"))) == 1
    sdk_method.assert_called_once_with(resource_group_name="rg", factory_name="factory")

    sdk_method.side_effect = HttpResponseError(message="forbidden")
    with pytest.raises(HttpResponseError):
        list(listing("rg", "factory"))


def test_listing_factories_propagates_sdk_errors(
    client: AzureDataFactoryClient,
) -> None:
    _sdk(client).factories.list.side_effect = HttpResponseError(message="forbidden")
    with pytest.raises(HttpResponseError):
        list(client.get_factories())


def test_single_resource_lookups_refuse_an_empty_sdk_response(
    client: AzureDataFactoryClient,
) -> None:
    """The SDK types `get` as Optional; a None must fail at the lookup rather
    than surface later as an AttributeError on the caller's side."""
    sdk = _sdk(client)
    sdk.factories.get.return_value = None
    sdk.pipelines.get.return_value = None

    with pytest.raises(ValueError):
        client.get_factory("rg", "factory")
    with pytest.raises(ValueError):
        client.get_pipeline("rg", "factory", "pipeline")


def test_single_resource_lookups_return_the_sdk_model(
    client: AzureDataFactoryClient,
) -> None:
    sdk = _sdk(client)
    sdk.factories.get.return_value = adf_factory("factory")
    pipeline = adf.PipelineResource()
    sdk.pipelines.get.return_value = pipeline

    assert client.get_factory("rg", "factory").name == "factory"
    assert client.get_pipeline("rg", "factory", "pipeline") is pipeline
    sdk.pipelines.get.assert_called_once_with(
        resource_group_name="rg", factory_name="factory", pipeline_name="pipeline"
    )


def test_pipeline_runs_follow_continuation_tokens_within_the_history_window(
    client: AzureDataFactoryClient,
) -> None:
    """Azure pages run queries with a continuation token in the response
    body rather than an SDK pager, so the client has to loop itself; a
    missing loop would silently truncate execution history to one page."""
    query = _sdk(client).pipeline_runs.query_by_factory
    query.side_effect = [
        adf.PipelineRunsQueryResponse(
            value=[pipeline_run(None, "r1")], continuation_token="page2"
        ),
        adf.PipelineRunsQueryResponse(
            value=[pipeline_run(None, "r2")], continuation_token=None
        ),
    ]

    runs = list(client.get_pipeline_runs("rg", "factory", days=3))

    assert [r.run_id for r in runs] == ["r1", "r2"]
    first_filter: adf.RunFilterParameters = query.call_args_list[0].kwargs[
        "filter_parameters"
    ]
    second_filter: adf.RunFilterParameters = query.call_args_list[1].kwargs[
        "filter_parameters"
    ]
    assert second_filter.continuation_token == "page2"
    window = first_filter.last_updated_before - first_filter.last_updated_after
    assert window.days == 3


def test_activity_runs_follow_continuation_tokens_for_one_pipeline_run(
    client: AzureDataFactoryClient,
) -> None:
    """Every page must be queried for the same pipeline run; a token sent
    without its run id would page through another run's activities."""
    query = _sdk(client).activity_runs.query_by_pipeline_run
    query.side_effect = [
        adf.ActivityRunsQueryResponse(
            value=[activity_run("Copy", "ar-1")], continuation_token="more"
        ),
        adf.ActivityRunsQueryResponse(value=[], continuation_token=None),
    ]

    runs: List[adf.ActivityRun] = list(
        client.get_activity_runs("rg", "factory", run_id="run-1")
    )

    assert [r.activity_name for r in runs] == ["Copy"]
    assert query.call_count == 2
    assert all(c.kwargs["run_id"] == "run-1" for c in query.call_args_list)


def test_run_queries_propagate_sdk_errors(client: AzureDataFactoryClient) -> None:
    sdk = _sdk(client)
    sdk.pipeline_runs.query_by_factory.side_effect = HttpResponseError(message="x")
    sdk.activity_runs.query_by_pipeline_run.side_effect = HttpResponseError(message="x")

    with pytest.raises(HttpResponseError):
        list(client.get_pipeline_runs("rg", "factory"))
    with pytest.raises(HttpResponseError):
        list(client.get_activity_runs("rg", "factory", run_id="run-1"))


def test_close_releases_the_sdk_client(client: AzureDataFactoryClient) -> None:
    client.close()
    _sdk(client).close.assert_called_once_with()
