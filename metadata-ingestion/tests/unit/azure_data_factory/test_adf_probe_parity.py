"""ADF's probe against real ingestion: both read the same mocked management
client, and `probe filter` must keep exactly what get_workunits_internal()
emits -- factories, pipelines (DataFlows) and activities (DataJobs), including
those nested inside control activities."""

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Set
from unittest import mock

import pytest

from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import FlowContainerSubTypes
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from tests.integration.azure_data_factory.complex_mocks import (
    RESOURCE_GROUP,
    SUBSCRIPTION_ID,
    create_foreach_loop_scenario,
    create_multisource_chain_scenario,
)
from tests.integration.azure_data_factory.test_adf_source import (
    MockAzureResource,
    MockPagedIterator,
    create_complex_scenario_mock_client,
    create_mock_factory,
)
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

KEPT = "kept-factory"
DENIED = "denied-factory"

_RECIPE = {
    "subscription_id": SUBSCRIPTION_ID,
    "credential": {"authentication_method": "default"},
    "include_lineage": True,
    "include_execution_history": False,
    "env": "DEV",
    "factory_pattern": {"deny": [f"^{DENIED}$"]},
}


@contextmanager
def _two_factories(scenario: Callable[[], Dict[str, Any]]) -> Iterator[None]:
    """One mocked management client for both readers. Every factory's
    pipeline listing answers with the scenario's pipelines, so the denied
    factory's copy is the point."""
    client = create_complex_scenario_mock_client(scenario())
    factories = [
        create_mock_factory(name, RESOURCE_GROUP, SUBSCRIPTION_ID)
        for name in (KEPT, DENIED)
    ]
    client.factories.list.return_value = MockPagedIterator(factories)
    client.factories.list_by_resource_group.side_effect = lambda *a, **k: (
        MockPagedIterator(factories)
    )
    client.pipelines.list_by_factory.side_effect = lambda *a, **k: MockPagedIterator(
        scenario()["pipelines"]
    )
    by_name = {p["name"]: p for p in scenario()["pipelines"]}
    # `activities` reads one pipeline with pipelines.get, as the probe's
    # client does; ingestion only lists them.
    client.pipelines.get.side_effect = (
        lambda resource_group_name, factory_name, pipeline_name, **k: MockAzureResource(
            by_name[pipeline_name]
        )
    )
    with (
        mock.patch(
            "datahub.ingestion.source.azure_data_factory.adf_client.DataFactoryManagementClient",
            return_value=client,
        ),
        mock.patch("datahub.ingestion.source.azure.azure_auth.DefaultAzureCredential"),
    ):
        yield


def _flow_ids(index: EmittedIndex) -> Set[str]:
    return {DataFlowUrn.from_string(u).flow_id for u in index.urns("dataFlow")}


def _job_ids(factory: str) -> Callable[[EmittedIndex], Set[str]]:
    def jobs(index: EmittedIndex) -> Set[str]:
        found: Set[str] = set()
        for urn in index.urns("dataJob"):
            job = DataJobUrn.from_string(urn)
            flow_id = DataFlowUrn.from_string(job.flow).flow_id
            if flow_id.startswith(f"{factory}."):
                found.add(f"{flow_id}.{job.job_id}")
        return found

    return jobs


def _activities(factory: str) -> ParityListing:
    return ParityListing(
        f"activities in {factory}",
        "activities",
        _job_ids(factory),
        kwargs={"factory": factory},
        fan_out=FanOut("pipelines", "pipeline", parent_kwargs={"factory": factory}),
        expect_empty=factory == DENIED,
    )


_FACTORIES = ParityListing(
    "factories",
    "factories",
    lambda index: index.container_names(str(FlowContainerSubTypes.ADF_DATA_FACTORY)),
)
_PIPELINES = ParityListing(
    "pipelines",
    "pipelines",
    _flow_ids,
    fan_out=FanOut("factories", "factory"),
)


def test_factories_pipelines_and_activities_match_ingestion(tmp_path: Path) -> None:
    with _two_factories(create_foreach_loop_scenario):
        report = assert_probe_parity(
            "azure-data-factory",
            _RECIPE,
            pipeline_ingestion("azure-data-factory", tmp_path),
            [_FACTORIES, _PIPELINES, _activities(KEPT), _activities(DENIED)],
        )
    assert report.excluded_by("factories") == {DENIED: "factory_pattern"}
    # Ingestion recurses into the ForEach and gives its inner Copy a DataJob
    # too, so the probe must list it alongside the top-level activities.
    assert report.kinds[f"activities in {KEPT}"].included == {
        f"{KEPT}.ForEachTablePipeline.GetTableList",
        f"{KEPT}.ForEachTablePipeline.IterateOverTables",
        f"{KEPT}.ForEachTablePipeline.CopyTableToStaging",
    }


@pytest.fixture
def _copy_chain() -> Iterator[None]:
    # Top-level Copy activities with dataset inputs and outputs.
    with _two_factories(create_multisource_chain_scenario):
        yield


@pytest.mark.usefixtures("_copy_chain")
def test_dataset_urns_are_the_ones_ingestion_emits_as_lineage(tmp_path: Path) -> None:
    # The URNs the probe resolves, for the datasets the kept factory's
    # activities read and write, against the inputs and outputs ingestion's
    # DataJobs carry.
    emitted = pipeline_ingestion("azure-data-factory", tmp_path)(dict(_RECIPE))
    lineage: Set[str] = set()
    for urn in emitted.urns("dataJob"):
        flow = DataFlowUrn.from_string(DataJobUrn.from_string(urn).flow)
        if not flow.flow_id.startswith(f"{KEPT}."):
            continue
        for aspect in emitted.aspects[urn]:
            if isinstance(aspect, DataJobInputOutputClass):
                lineage.update(aspect.inputDatasets or [])
                lineage.update(aspect.outputDatasets or [])
    pipelines = run_probe_method(
        "azure-data-factory", dict(_RECIPE), "pipelines", {"factory": KEPT}
    ).result
    assert isinstance(pipelines, list)
    referenced: Set[str] = set()
    for pipeline in pipelines:
        activities = run_probe_method(
            "azure-data-factory",
            dict(_RECIPE),
            "activities",
            {"factory": KEPT, "pipeline": pipeline["name"]},
        ).result
        assert isinstance(activities, list)
        for activity in activities:
            referenced.update(activity["inputs"])
            referenced.update(activity["outputs"])
    datasets = run_probe_method(
        "azure-data-factory", dict(_RECIPE), "datasets", {"factory": KEPT}
    ).result
    assert isinstance(datasets, list)
    resolved = {d["urn"] for d in datasets if d["name"] in referenced and d["urn"]}
    assert resolved, "the fixture's activities reference no dataset"
    assert resolved == {
        u for u in lineage if "dataPlatform:azure-data-factory" not in u
    }
