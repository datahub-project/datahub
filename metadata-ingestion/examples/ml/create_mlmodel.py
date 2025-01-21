import time
from dataclasses import dataclass
from typing import List, Optional
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.urns import MlModelGroupUrn, MlModelUrn, DatasetUrn, VersionSetUrn
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.emitter.rest_emitter import DatahubRestEmitter


class ContainerKeyWithId(ContainerKey):
    id: str


@dataclass
class Container:
    key: ContainerKeyWithId
    subtype: str
    name: Optional[str] = None
    description: Optional[str] = None

    def generate_mcp(self) -> List[MetadataChangeProposalWrapper]:
        container_urn = self.key.as_urn()
        current_time = int(time.time() * 1000)

        # Create container aspects
        container_subtype = models.SubTypesClass(typeNames=[self.subtype])
        container_info = models.ContainerPropertiesClass(
            name=self.name or self.key.id,
            description=self.description,
            created=models.TimeStampClass(
                time=current_time, actor="urn:li:corpuser:datahub"
            ),
            lastModified=models.TimeStampClass(
                time=current_time, actor="urn:li:corpuser:datahub"
            ),
            customProperties={},
        )
        browse_path = models.BrowsePathsV2Class(path=[])
        dpi = models.DataPlatformInstanceClass(
            platform=self.key.platform,
            instance=self.key.instance,
        )

        mcps = []

        # Add container aspects
        mcps.extend(
            [
                MetadataChangeProposalWrapper(
                    entityType="container",
                    entityUrn=str(container_urn),
                    aspectName="subTypes",
                    aspect=container_subtype,
                    changeType=models.ChangeTypeClass.UPSERT,
                ),
                MetadataChangeProposalWrapper(
                    entityType="container",
                    entityUrn=str(container_urn),
                    aspectName="containerProperties",
                    aspect=container_info,
                    changeType=models.ChangeTypeClass.UPSERT,
                ),
                MetadataChangeProposalWrapper(
                    entityType="container",
                    entityUrn=str(container_urn),
                    aspectName="dataPlatformInstance",
                    aspect=dpi,
                    changeType=models.ChangeTypeClass.UPSERT,
                ),
                MetadataChangeProposalWrapper(
                    entityType="container",
                    entityUrn=str(container_urn),
                    aspectName="status",
                    aspect=models.StatusClass(removed=False),
                    changeType=models.ChangeTypeClass.UPSERT,
                ),
            ]
        )

        return mcps


def create_training_job(
    experiment_key: ContainerKeyWithId, run_id: str, input_dataset_urn: str
) -> tuple[DataProcessInstance, List[MetadataChangeProposalWrapper]]:
    """Create a training job instance"""
    data_process_instance = DataProcessInstance.from_container(
        container_key=experiment_key, id=run_id
    )

    data_process_instance.platform = experiment_key.platform
    data_process_instance.subtype = "Training Run"
    data_process_instance.inlets = [DatasetUrn.from_string(input_dataset_urn)]
    data_process_instance.container = (
        experiment_key.as_urn()
    )  # Set container relationship here

    created_at = int(time.time() * 1000)

    # First get base MCPs from the instance
    mcps = list(
        data_process_instance.generate_mcp(
            created_ts_millis=created_at, materialize_iolets=True
        )
    )

    # Create and add DPI properties aspect
    dpi_props = models.DataProcessInstancePropertiesClass(
        name=f"Training {run_id}",
        created=models.AuditStampClass(
            time=created_at, actor="urn:li:corpuser:datahub"
        ),
        externalUrl="http://mlflow:5000",
        customProperties={
            "framework": "sklearn",
            "python_version": "3.8",
            "experiment_id": experiment_key.id,
        },
    )

    # Create training run properties
    training_run_props = models.MLTrainingRunPropertiesClass(
        customProperties={
            "learning_rate": "0.01",
            "batch_size": "64",
        },
        externalUrl="http://mlflow:5000",
        hyperParams=[
            models.MLHyperParamClass(
                name="n_estimators", value="100", description="Number of trees"
            ),
            models.MLHyperParamClass(
                name="max_depth", value="10", description="Maximum tree depth"
            ),
        ],
        trainingMetrics=[
            models.MLMetricClass(
                name="accuracy", value="0.95", description="Test accuracy"
            ),
            models.MLMetricClass(
                name="f1_score", value="0.93", description="Test F1 score"
            ),
        ],
        outputUrls=["s3://mlflow/outputs"],
        id=run_id,
    )

    start_time_millis = created_at
    end_time_millis = start_time_millis + 360000
    result = InstanceRunResult.SUCCESS
    result_type = "SUCCESS"

    run_start_event = models.DataProcessInstanceRunEventClass(
        status=models.DataProcessRunStatusClass.STARTED,
        timestampMillis=start_time_millis,
    )

    run_end_event = models.DataProcessInstanceRunEventClass(
        status=models.DataProcessRunStatusClass.COMPLETE,
        timestampMillis=end_time_millis,
        durationMillis=end_time_millis - start_time_millis,
        result=models.DataProcessInstanceRunResultClass(
            type=result,
            nativeResultType=(result_type if result_type is not None else "mlflow"),
        ),
    )

    # Add custom aspects
    mcps.extend(
        [
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                entityType="dataProcessInstance",
                aspectName="dataProcessInstanceProperties",
                aspect=dpi_props,
                changeType=models.ChangeTypeClass.UPSERT,
            ),
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                entityType="dataProcessInstance",
                aspectName="mlTrainingRunProperties",
                aspect=training_run_props,
                changeType=models.ChangeTypeClass.UPSERT,
            ),
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                entityType="dataProcessInstance",
                aspectName="dataProcessInstanceRunEvent",
                aspect=run_start_event,
                changeType=models.ChangeTypeClass.UPSERT,
            ),
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                entityType="dataProcessInstance",
                aspectName="dataProcessInstanceRunEvent",
                aspect=run_end_event,
                changeType=models.ChangeTypeClass.UPSERT,
            ),
        ]
    )

    # Add run events
    start_time = created_at
    end_time = start_time + (45 * 60 * 1000)  # 45 minutes duration

    # mcps.extend([
    #     MetadataChangeProposalWrapper(
    #         entityUrn=str(data_process_instance.urn),
    #         entityType="dataProcessInstance",
    #         aspectName="dataProcessInstanceRunEvent",
    #         aspect=models.DataProcessInstanceRunEventClass(
    #             timestampMillis=start_time,
    #             eventGranularity="TASK"
    #         ),
    #         changeType=models.ChangeTypeClass.UPSERT
    #     ),
    #     MetadataChangeProposalWrapper(
    #         entityUrn=str(data_process_instance.urn),
    #         entityType="dataProcessInstance",
    #         aspectName="dataProcessInstanceRunEvent",
    #         aspect=models.DataProcessInstanceRunEventClass(
    #             timestampMillis=end_time,
    #             eventGranularity="TASK",
    #             result=InstanceRunResult.SUCCESS
    #         ),
    #         changeType=models.ChangeTypeClass.UPSERT
    #     )
    # ])

    return data_process_instance, mcps


def create_model_group(
    training_job_urn: str,
) -> tuple[MlModelGroupUrn, MetadataChangeProposalWrapper]:
    """Create a model group and return its URN and MCP"""
    model_group_urn = MlModelGroupUrn(platform="mlflow", name="simple_model_group")
    current_time = int(time.time() * 1000)

    model_group_info = models.MLModelGroupPropertiesClass(
        description="Simple ML model group example",
        customProperties={
            "stage": "production",
            "team": "data_science",
        },
        created=models.TimeStampClass(
            time=current_time, actor="urn:li:corpuser:datahub"
        ),
        lastModified=models.TimeStampClass(
            time=current_time, actor="urn:li:corpuser:datahub"
        ),
        trainingJobs=[str(training_job_urn)],
    )

    return model_group_urn, MetadataChangeProposalWrapper(
        entityUrn=str(model_group_urn),
        entityType="mlModelGroup",
        aspectName="mlModelGroupProperties",
        aspect=model_group_info,
        changeType=models.ChangeTypeClass.UPSERT,
    )


def create_single_model(
    model_name: str,
    model_group_urn: str,
    training_job_urn: str,
) -> tuple[MlModelUrn, List[MetadataChangeProposalWrapper]]:
    """Create a single ML model and return its MCP"""
    model_urn = MlModelUrn(platform="mlflow", name=model_name)
    current_time = int(time.time() * 1000)

    mcps = []

    model_info = models.MLModelPropertiesClass(
        description="Simple example ML model",
        version=models.VersionTagClass(versionTag="1"),
        groups=[str(model_group_urn)],
        trainingJobs=[str(training_job_urn)],
        date=current_time,
        lastModified=models.TimeStampClass(
            time=current_time, actor="urn:li:corpuser:datahub"
        ),
        created=models.TimeStampClass(
            time=current_time, actor="urn:li:corpuser:datahub"
        ),
        tags=["stage:production", "team:data_science"],
        trainingMetrics=[
            models.MLMetricClass(
                name="accuracy", value="0.95", description="Test accuracy"
            ),
            models.MLMetricClass(
                name="f1_score", value="0.93", description="Test F1 score"
            ),
        ],
        hyperParams=[
            models.MLHyperParamClass(
                name="n_estimators", value="100", description="Number of trees"
            ),
            models.MLHyperParamClass(
                name="max_depth", value="10", description="Maximum tree depth"
            ),
        ],
    )

    print(str(model_urn))

    # define version_set
    version_set_urn = VersionSetUrn(
        id="mlmodelgroup_id", entity_type="mlModel"
    )  # since the model is version of the model group

    # somehow link the version_set and model_urn
    version_entity = models.VersionSetPropertiesClass(
        latest=str(model_urn),  # should be the version of the model
        versioningScheme="ALPHANUMERIC_GENERATED_BY_DATAHUB",
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=str(version_set_urn),
            entityType="versionSet",
            aspectName="versionSetProperties",
            aspect=version_entity,
            changeType=models.ChangeTypeClass.UPSERT,
        )
    )
    # emit version properties
    model_version_info = models.VersionPropertiesClass(
        version=models.VersionTagClass(versionTag="1"),
        versionSet=str(version_set_urn),
        aliases=[models.VersionTagClass(versionTag="latest")],
        sortId="AAAAAAAA",
    )
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=str(model_urn),
            entityType="mlModel",
            aspectName="versionProperties",
            aspect=model_version_info,
            changeType=models.ChangeTypeClass.UPSERT,
        )
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=str(model_urn),
            entityType="mlModel",
            aspectName="mlModelProperties",
            aspect=model_info,
            changeType=models.ChangeTypeClass.UPSERT,
        )
    )

    return model_urn, mcps


def main():
    # Create emitter with authentication token
    token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6ImQ2MDBlNjYyLTliYjgtNGU5MS1hMGY4LTUxZjdkYjVjMzkyYiIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3Mzk2NjA1MTgsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.6X37g3hQ2Ki3P0GABzl5_dUwZtB0b15h_E4WQyzVX_Y"
    emitter = DatahubRestEmitter(
        gms_server="http://localhost:8080",
        extra_headers={"Authorization": f"Bearer {token}"},
    )

    # Create experiment container
    experiment = Container(
        key=ContainerKeyWithId(
            platform="urn:li:dataPlatform:mlflow",
            # instance="prod",
            id="airline_forecast_experiment",
        ),
        subtype="ML Experiment",
        name="Airline Forecast Experiment",
        description="Experiment for forecasting airline passengers",
    )

    # Create training job instance
    training_job, training_mcps = create_training_job(
        experiment_key=experiment.key,
        run_id="run_1",
        input_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:s3,airline_passengers,PROD)",
    )

    # Create the model group
    model_group_urn, model_group_mcp = create_model_group(str(training_job.urn))

    # Create the model with training job reference
    model_urn, model_mcps = create_single_model(
        "simple_model", str(model_group_urn), str(training_job.urn)
    )

    # Emit all metadata
    # First, emit model group
    print("Emitting model group...")
    emitter.emit(model_group_mcp)

    # Emit experiment container
    print("Emitting container aspects...")
    for mcp in experiment.generate_mcp():
        emitter.emit(mcp)

    # Emit training job properties and events
    print("Emitting training job aspects...")
    for mcp in training_mcps:
        emitter.emit(mcp)

    # # Finally emit the model and its aspects
    print("Emitting model aspects...")
    for mcp in model_mcps:
        emitter.emit(mcp)

    print("Successfully created model group, training job, and model in DataHub")


if __name__ == "__main__":
    main()
