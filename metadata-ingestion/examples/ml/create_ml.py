import random
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Union

import datahub.metadata.schema_classes as models
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import (
    DataPlatformUrn,
    DatasetUrn,
    MlModelGroupUrn,
    MlModelUrn,
    VersionSetUrn,
)

ORCHESTRATOR_MLFLOW = "mlflow"
ORCHESTRATOR_AIRFLOW = "airflow"


class ContainerKeyWithId(ContainerKey):
    id: str


@dataclass
class RunInfo:
    start_time: int
    duration: int
    result: InstanceRunResult


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


def create_model(
    model_name: str,
    model_group_urn: str,
    data_process_instance_urn: str,
    tags: List[str],
    version_aliases: List[str],
    index: int,
    training_metrics: List[models.MLMetricClass],
    hyper_params: List[models.MLHyperParamClass],
    model_description: str,
    created_at: int,
) -> Iterable[MetadataChangeProposalWrapper]:
    model_urn = MlModelUrn(platform="mlflow", name=model_name)

    # Create model properties
    model_info = models.MLModelPropertiesClass(
        description=model_description,
        version=models.VersionTagClass(versionTag=f"{index}"),
        groups=[str(model_group_urn)],
        trainingJobs=[str(data_process_instance_urn)],
        date=created_at,
        tags=tags,
        trainingMetrics=training_metrics,
        hyperParams=hyper_params,
        created=models.TimeStampClass(time=created_at, actor="urn:li:corpuser:datahub"),
        lastModified=models.TimeStampClass(
            time=created_at, actor="urn:li:corpuser:datahub"
        ),
    )

    # Create version set
    version_set_urn = VersionSetUrn(
        id=f"mlmodel_{model_name}_versions", entity_type="mlModel"
    )
    version_entity = models.VersionSetPropertiesClass(
        latest=str(model_urn),
        versioningScheme="ALPHANUMERIC_GENERATED_BY_DATAHUB",
    )

    # Create version properties
    model_version_info = models.VersionPropertiesClass(
        version=models.VersionTagClass(versionTag=f"{index}"),
        versionSet=str(version_set_urn),
        aliases=[models.VersionTagClass(versionTag=alias) for alias in version_aliases],
        sortId="AAAAAAAA",
    )

    # Yield all MCPs
    yield MetadataChangeProposalWrapper(
        entityUrn=str(version_set_urn),
        entityType="versionSet",
        aspectName="versionSetProperties",
        aspect=version_entity,
        changeType=models.ChangeTypeClass.UPSERT,
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=str(model_urn),
        entityType="mlModel",
        aspectName="versionProperties",
        aspect=model_version_info,
        changeType=models.ChangeTypeClass.UPSERT,
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=str(model_urn),
        entityType="mlModel",
        aspectName="mlModelProperties",
        aspect=model_info,
        changeType=models.ChangeTypeClass.UPSERT,
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

    input_dataset = Dataset(
        id="airline_passengers",
        name="Airline Passengers",
        description="Monthly airline passenger data",
        properties={},
        platform="s3",
        schema=None,
    )

    if orchestrator == ORCHESTRATOR_MLFLOW:
        experiment = Container(
            key=ContainerKeyWithId(
                platform=str(DataPlatformUrn.create_from_id("mlflow")),
                id="airline_forecast_experiment",
            ),
            subtype="ML Experiment",
            name="Airline Forecast Experiment",
            description="Experiment for forecasting airline passengers",
        )

        yield from experiment.generate_mcp()

        model_group_urn = MlModelGroupUrn(
            platform="mlflow", name="airline_forecast_models"
        )
        current_time = int(time.time() * 1000)
        model_group_info = models.MLModelGroupPropertiesClass(
            description="ML models for airline passenger forecasting",
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
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=str(model_group_urn),
            entityType="mlModelGroup",
            aspectName="mlModelGroupProperties",
            aspect=model_group_info,
            changeType=models.ChangeTypeClass.UPSERT,
        )

        model_aliases = [
            "challenger",
            "champion",
            "production",
            "experimental",
            "deprecated",
        ]
        model_tags = [
            "stage:production",
            "stage:development",
            "team:data_science",
            "team:ml_engineering",
            "team:analytics",
        ]

        model_dict = {
            "arima_model_1": "ARIMA model for airline passenger forecasting",
            "arima_model_2": "Enhanced ARIMA model with seasonal components",
            "arima_model_3": "ARIMA model optimized for long-term forecasting",
            "arima_model_4": "ARIMA model with hyperparameter tuning",
            "arima_model_5": "ARIMA model trained on extended dataset",
        }

        # Generate run timestamps within the last month
        end_time = int(time.time() * 1000)
        start_time = end_time - (30 * 24 * 60 * 60 * 1000)
        run_timestamps = [start_time + (i * 5 * 24 * 60 * 60 * 1000) for i in range(5)]

        run_dict: Dict[str, RunInfo] = {
            "run_1": RunInfo(
                start_time=run_timestamps[0],
                duration=45,
                result=InstanceRunResult.SUCCESS,
            ),
            "run_2": RunInfo(
                start_time=run_timestamps[1],
                duration=60,
                result=InstanceRunResult.FAILURE,
            ),
            "run_3": RunInfo(
                start_time=run_timestamps[2],
                duration=55,
                result=InstanceRunResult.SUCCESS,
            ),
            "run_4": RunInfo(
                start_time=run_timestamps[3],
                duration=70,
                result=InstanceRunResult.SUCCESS,
            ),
            "run_5": RunInfo(
                start_time=run_timestamps[4],
                duration=50,
                result=InstanceRunResult.FAILURE,
            ),
        }

        for i, (model_name, model_description) in enumerate(
            model_dict.items(), start=1
        ):
            run_id = f"run_{i}"
            data_process_instance = DataProcessInstance.from_container(
                container_key=experiment.key, id=run_id
            )

            data_process_instance.subtype = "ML Training Run"
            if input_dataset.urn is not None:
                data_process_instance.inlets = [
                    DatasetUrn.from_string(input_dataset.urn)
                ]

            output_dataset = Dataset(
                id=f"passenger_forecast_24_12_0{i}",
                name=f"Passenger Forecast 24_12_0{i}",
                description=f"Forecasted airline passenger numbers for run {i}",
                properties={},
                platform="s3",
                schema=None,
            )
            yield from output_dataset.generate_mcp()
            if output_dataset.urn is not None:
                data_process_instance.outlets = [
                    DatasetUrn.from_string(output_dataset.urn)
                ]

            # Training metrics and hyperparameters
            training_metrics = [
                models.MLMetricClass(
                    name="accuracy",
                    value=str(random.uniform(0.7, 0.99)),
                    description="Test accuracy",
                ),
                models.MLMetricClass(
                    name="f1_score",
                    value=str(random.uniform(0.7, 0.99)),
                    description="Test F1 score",
                ),
            ]
            hyper_params = [
                models.MLHyperParamClass(
                    name="n_estimators",
                    value=str(random.randint(50, 200)),
                    description="Number of trees",
                ),
                models.MLHyperParamClass(
                    name="max_depth",
                    value=str(random.randint(5, 15)),
                    description="Maximum tree depth",
                ),
            ]

            # DPI properties
            created_at = int(time.time() * 1000)
            dpi_props = models.DataProcessInstancePropertiesClass(
                name=f"Training {run_id}",
                created=models.AuditStampClass(
                    time=created_at, actor="urn:li:corpuser:datahub"
                ),
                customProperties={
                    "framework": "statsmodels",
                    "python_version": "3.8",
                },
            )

            mlrun_props = models.MLTrainingRunPropertiesClass(
                id="run_id",
                outputUrls=["s3://mlflow/artifacts"],
                hyperParams=hyper_params,
                trainingMetrics=training_metrics,
                externalUrl="http://mlflow:5000",
            )

            yield from data_process_instance.generate_mcp(
                created_ts_millis=created_at, materialize_iolets=True
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=dpi_props,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=mlrun_props,
            )

            # Generate start and end events
            run_info = run_dict[run_id]
            start_time_millis = run_info.start_time
            duration_minutes = run_info.duration
            end_time_millis = start_time_millis + (duration_minutes * 60000)
            result = run_info.result

            result_type = (
                "SUCCESS" if result == InstanceRunResult.SUCCESS else "FAILURE"
            )

            yield from data_process_instance.start_event_mcp(
                start_timestamp_millis=start_time_millis
            )
            yield from data_process_instance.end_event_mcp(
                end_timestamp_millis=end_time_millis,
                result=result,
                result_type=result_type,
                start_timestamp_millis=start_time_millis,
            )

            # Model
            selected_aliases = random.sample(model_aliases, k=random.randint(1, 2))
            selected_tags = random.sample(model_tags, 2)
            yield from create_model(
                model_name=model_name,
                model_group_urn=str(model_group_urn),
                data_process_instance_urn=str(data_process_instance.urn),
                tags=selected_tags,
                version_aliases=selected_aliases,
                index=i,
                training_metrics=training_metrics,
                hyper_params=hyper_params,
                model_description=model_description,
                created_at=created_at,
            )

    if orchestrator == ORCHESTRATOR_AIRFLOW:
        yield from data_flow.generate_mcp()
        yield from data_job.generate_mcp()

    yield from input_dataset.generate_mcp()


if __name__ == "__main__":
    token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjY0OWU5Zjk0LWI3MzctNGJiOS1iZmQ2LTA5MzU3OWI5ZjJmNiIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NDAwNjUyNjYsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.K-1U2sw2yMLpWRf4Nqf0zysUbXREx2qrVlP-Irl_T_Q"
    graph_config = DatahubClientConfig(
        server="http://localhost:8080",
        token=token,
        extra_headers={"Authorization": f"Bearer {token}"},
    )
    graph = DataHubGraph(graph_config)
    with graph:
        for mcp in generate_pipeline(
            "airline_forecast_pipeline_mlflow", orchestrator=ORCHESTRATOR_MLFLOW
        ):
            graph.emit(mcp)
        for mcp in generate_pipeline(
            "airline_forecast_pipeline_airflow", orchestrator=ORCHESTRATOR_AIRFLOW
        ):
            graph.emit(mcp)
