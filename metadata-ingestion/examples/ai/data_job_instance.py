from dataclasses import dataclass
from typing import Iterable, Optional, Union

import datahub.metadata.schema_classes as models
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.urns import DatasetUrn, DataPlatformUrn
from datahub.emitter.mcp_builder import ContainerKey

ORCHESTRATOR_MLFLOW = "mlflow"
ORCHESTRATOR_AIRFLOW = "airflow"


class ContainerKeyWithId(ContainerKey):
    id: str


@dataclass
class Container:
    key: ContainerKeyWithId
    subtype: str
    name: Optional[str] = None
    description: Optional[str] = None

    def generate_mcp(
        self,
    ) -> Iterable[
        Union[models.MetadataChangeProposalClass, MetadataChangeProposalWrapper]
    ]:
        container_urn = self.key.as_urn()

        container_subtype = models.SubTypesClass(typeNames=[self.subtype])

        container_info = models.ContainerPropertiesClass(
            name=self.name or self.key.id,
            description=self.description,
            customProperties={},
        )

        browse_path = models.BrowsePathsV2Class(path=[])

        dpi = models.DataPlatformInstanceClass(
            platform=self.key.platform,
            instance=self.key.instance,
        )

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=container_urn,
            aspects=[container_subtype, container_info, browse_path, dpi],
        )


def generate_pipeline(
    pipeline_name: str,
    orchestrator: str,
) -> Iterable[Union[models.MetadataChangeProposalClass, MetadataChangeProposalWrapper]]:
    data_flow = DataFlow(
        id=pipeline_name,
        orchestrator=orchestrator,
        cluster="default",
        name=pipeline_name,
    )

    data_job = DataJob(id="training", flow_urn=data_flow.urn, name="Training")

    dataset_1 = Dataset(
        id="input_data",
        name="input_data",
        description="Input data",
        properties={},
        platform="s3",
        schema=None,
    )

    dataset_2 = Dataset(
        id="output_data",
        name="output_data",
        description="Output data",
        properties={},
        platform="s3",
        schema=None,
    )

    if orchestrator == ORCHESTRATOR_MLFLOW:
        # For Mlflow we create an experiment and a run

        experiment = Container(
            key=ContainerKeyWithId(
                platform=str(DataPlatformUrn.create_from_id("mlflow")),
                id="experiment_1",
            ),
            subtype="Experiment",
            name="Experiment 1",
            description="Experiment 1 description",
        )

        yield from experiment.generate_mcp()

        data_process_instance = DataProcessInstance.from_container(
            container_key=experiment.key, id="training_2024_01_01"
        )

    if orchestrator == ORCHESTRATOR_AIRFLOW:
        # For Airflow we create a DAG and a task
        data_process_instance = DataProcessInstance.from_datajob(
            datajob=data_job, id="training_2024_01_01"
        )
        yield from data_flow.generate_mcp()
        yield from data_job.generate_mcp()

    # data_process_instance = DataProcessInstance.from_datajob(
    #     datajob=data_job, id="training_2024_01_01"
    # )
    data_process_instance.subtype = "Training Run"
    data_process_instance.inlets = [DatasetUrn.from_string(dataset_1.urn)]
    data_process_instance.outlets = [DatasetUrn.from_string(dataset_2.urn)]

    yield from dataset_1.generate_mcp()
    yield from dataset_2.generate_mcp()
    print(f"Generating for {data_process_instance.urn}")
    yield from data_process_instance.generate_mcp(
        created_ts_millis=None, materialize_iolets=False
    )
    # Finally generate the start and end events
    # start date is Dec 3rd 2024 at 10am UTC
    start_time_millis = 1735689600000
    # the job ran for 1 hour
    end_time_millis = start_time_millis + 60 * 60 * 1000
    yield from data_process_instance.start_event_mcp(
        # 5 days ago
        start_timestamp_millis=start_time_millis
    )
    yield from data_process_instance.end_event_mcp(
        end_timestamp_millis=end_time_millis,
        result=InstanceRunResult.SUCCESS,
        start_timestamp_millis=start_time_millis,
    )


if __name__ == "__main__":
    with get_default_graph() as graph:
        for mcp in generate_pipeline(
            "training_pipeline_mlflow", orchestrator=ORCHESTRATOR_MLFLOW
        ):
            graph.emit(mcp)
        for mcp in generate_pipeline(
            "training_pipeline_airflow", orchestrator=ORCHESTRATOR_AIRFLOW
        ):
            graph.emit(mcp)