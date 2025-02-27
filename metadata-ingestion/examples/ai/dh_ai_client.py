import logging
import time
from typing import Any, Dict, List, Optional, Union

import datahub.metadata.schema_classes as models
from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceInput,
    DataProcessInstanceOutput,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
)
from datahub.metadata.urns import (
    ContainerUrn,
    DataPlatformUrn,
    MlModelGroupUrn,
    MlModelUrn,
    VersionSetUrn,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatahubAIClient:
    """Client for creating and managing MLflow metadata in DataHub."""

    def __init__(
        self,
        token: Optional[str] = None,
        server_url: str = "http://localhost:8080",
        platform: str = "mlflow",
    ) -> None:
        """Initialize the DataHub AI client.

        Args:
            token: DataHub access token
            server_url: DataHub server URL (defaults to http://localhost:8080)
            platform: Platform name (defaults to mlflow)
        """
        self.token = token
        self.server_url = server_url
        self.platform = platform
        self.graph = DataHubGraph(
            DatahubClientConfig(
                server=server_url,
                token=token,
                extra_headers={"Authorization": f"Bearer {token}"},
            )
        )

    def _create_timestamp(
        self, timestamp: Optional[int] = None
    ) -> models.TimeStampClass:
        """Helper to create timestamp with current time if not provided"""
        return models.TimeStampClass(
            time=timestamp or int(time.time() * 1000), actor="urn:li:corpuser:datahub"
        )

    def _emit_mcps(self, mcps: Union[MetadataChangeProposalWrapper, List[Any]]) -> None:
        """Helper to emit MCPs with proper connection handling"""
        if not isinstance(mcps, list):
            mcps = [mcps]
        with self.graph:
            for mcp in mcps:
                self.graph.emit(mcp)

    def _get_aspect(
        self, entity_urn: str, aspect_type: Any, default_constructor: Any = None
    ) -> Any:
        """Helper to safely get an aspect with fallback"""
        try:
            return self.graph.get_aspect(entity_urn=entity_urn, aspect_type=aspect_type)
        except Exception as e:
            logger.warning(f"Could not fetch aspect for {entity_urn}: {e}")
            return default_constructor() if default_constructor else None

    def _create_properties_class(
        self, props_class: Any, props_dict: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Helper to create properties class with provided values"""
        if props_dict is None:
            props_dict = {}

        filtered_props = {k: v for k, v in props_dict.items() if v is not None}

        if hasattr(props_class, "created"):
            filtered_props.setdefault("created", self._create_timestamp())
        if hasattr(props_class, "lastModified"):
            filtered_props.setdefault("lastModified", self._create_timestamp())

        return props_class(**filtered_props)

    def _update_list_property(
        self, existing_list: Optional[List[str]], new_item: str
    ) -> List[str]:
        """Helper to update a list property while maintaining uniqueness"""
        items = set(existing_list if existing_list else [])
        items.add(new_item)
        return list(items)

    def _create_mcp(
        self,
        entity_urn: str,
        aspect: Any,
        entity_type: Optional[str] = None,
        aspect_name: Optional[str] = None,
        change_type: str = ChangeTypeClass.UPSERT,
    ) -> MetadataChangeProposalWrapper:
        """Helper to create an MCP with standard parameters"""
        mcp_args = {"entityUrn": entity_urn, "aspect": aspect}
        if entity_type:
            mcp_args["entityType"] = entity_type
        if aspect_name:
            mcp_args["aspectName"] = aspect_name
        mcp_args["changeType"] = change_type
        return MetadataChangeProposalWrapper(**mcp_args)

    def _update_entity_properties(
        self,
        entity_urn: str,
        aspect_type: Any,
        updates: Dict[str, Any],
        entity_type: str,
        skip_properties: Optional[List[str]] = None,
    ) -> None:
        """Helper to update entity properties while preserving existing ones"""
        existing_props = self._get_aspect(entity_urn, aspect_type, aspect_type)
        skip_list = [] if skip_properties is None else skip_properties
        props = self._copy_existing_properties(existing_props, skip_list) or {}

        for key, value in updates.items():
            if isinstance(value, str) and hasattr(existing_props, key):
                existing_value = getattr(existing_props, key, [])
                props[key] = self._update_list_property(existing_value, value)
            else:
                props[key] = value

        updated_props = self._create_properties_class(aspect_type, props)
        mcp = self._create_mcp(
            entity_urn, updated_props, entity_type, f"{entity_type}Properties"
        )
        self._emit_mcps(mcp)

    def _copy_existing_properties(
        self, existing_props: Any, skip_properties: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Helper to copy existing properties while skipping specified ones"""
        skip_list = [] if skip_properties is None else skip_properties

        internal_props = {
            "ASPECT_INFO",
            "ASPECT_NAME",
            "ASPECT_TYPE",
            "RECORD_SCHEMA",
        }
        skip_list.extend(internal_props)

        props: Dict[str, Any] = {}
        if existing_props:
            for prop in dir(existing_props):
                if (
                    prop.startswith("_")
                    or callable(getattr(existing_props, prop))
                    or prop in skip_list
                ):
                    continue

                value = getattr(existing_props, prop)
                if value is not None:
                    props[prop] = value

            if hasattr(existing_props, "created"):
                props.setdefault("created", self._create_timestamp())
            if hasattr(existing_props, "lastModified"):
                props.setdefault("lastModified", self._create_timestamp())

        return props

    def _create_run_event(
        self,
        status: str,
        timestamp: int,
        result: Optional[str] = None,
        duration_millis: Optional[int] = None,
    ) -> models.DataProcessInstanceRunEventClass:
        """Helper to create run event with common parameters."""
        event_args: Dict[str, Any] = {
            "timestampMillis": timestamp,
            "status": status,
            "attempt": 1,
        }

        if result:
            event_args["result"] = DataProcessInstanceRunResultClass(
                type=result, nativeResultType=str(result)
            )
        if duration_millis:
            event_args["durationMillis"] = duration_millis

        return models.DataProcessInstanceRunEventClass(**event_args)

    def create_model_group(
        self,
        group_id: str,
        properties: Optional[models.MLModelGroupPropertiesClass] = None,
        **kwargs: Any,
    ) -> str:
        """Create an ML model group with either property class or kwargs."""
        model_group_urn = MlModelGroupUrn(platform=self.platform, name=group_id)

        if properties is None:
            properties = self._create_properties_class(
                models.MLModelGroupPropertiesClass, kwargs
            )

        mcp = self._create_mcp(
            str(model_group_urn), properties, "mlModelGroup", "mlModelGroupProperties"
        )
        self._emit_mcps(mcp)
        logger.info(f"Created model group: {model_group_urn}")
        return str(model_group_urn)

    def create_model(
        self,
        model_id: str,
        version: str,
        alias: Optional[str] = None,
        properties: Optional[models.MLModelPropertiesClass] = None,
        **kwargs: Any,
    ) -> str:
        """Create an ML model with either property classes or kwargs."""
        model_urn = MlModelUrn(platform=self.platform, name=model_id)
        version_set_urn = VersionSetUrn(
            id=f"mlmodel_{model_id}_versions", entity_type="mlModel"
        )

        # Handle model properties
        if properties is None:
            # If no properties provided, create from kwargs
            properties = self._create_properties_class(
                models.MLModelPropertiesClass, kwargs
            )

        # Ensure version is set in model properties
        version_tag = models.VersionTagClass(versionTag=str(version))
        properties.version = version_tag

        # Create version properties
        version_props = {
            "version": version_tag,
            "versionSet": str(version_set_urn),
            "sortId": "AAAAAAAA",
        }

        # Add alias if provided
        if alias:
            version_props["aliases"] = [models.VersionTagClass(versionTag=alias)]

        version_properties = self._create_properties_class(
            models.VersionPropertiesClass, version_props
        )

        # Create version set properties
        version_set_properties = models.VersionSetPropertiesClass(
            latest=str(model_urn),
            versioningScheme="ALPHANUMERIC_GENERATED_BY_DATAHUB",
        )

        mcps = [
            self._create_mcp(
                str(model_urn), properties, "mlModel", "mlModelProperties"
            ),
            self._create_mcp(
                str(version_set_urn),
                version_set_properties,
                "versionSet",
                "versionSetProperties",
            ),
            self._create_mcp(
                str(model_urn), version_properties, "mlModel", "versionProperties"
            ),
        ]
        self._emit_mcps(mcps)
        logger.info(f"Created model: {model_urn}")
        return str(model_urn)

    def create_experiment(
        self,
        experiment_id: str,
        properties: Optional[models.ContainerPropertiesClass] = None,
        **kwargs: Any,
    ) -> str:
        """Create an ML experiment with either property class or kwargs."""
        container_urn = ContainerUrn(guid=experiment_id)
        platform_urn = DataPlatformUrn(platform_name=self.platform)

        if properties is None:
            properties = self._create_properties_class(
                models.ContainerPropertiesClass, kwargs
            )

        container_subtype = models.SubTypesClass(typeNames=["ML Experiment"])
        browse_path = models.BrowsePathsV2Class(path=[])
        platform_instance = models.DataPlatformInstanceClass(platform=str(platform_urn))

        mcps = MetadataChangeProposalWrapper.construct_many(
            entityUrn=str(container_urn),
            aspects=[container_subtype, properties, browse_path, platform_instance],
        )
        self._emit_mcps(mcps)
        logger.info(f"Created experiment: {container_urn}")
        return str(container_urn)

    def create_training_run(
        self,
        run_id: str,
        properties: Optional[models.DataProcessInstancePropertiesClass] = None,
        training_run_properties: Optional[models.MLTrainingRunPropertiesClass] = None,
        run_result: Optional[str] = None,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        **kwargs: Any,
    ) -> str:
        """Create a training run with properties and events."""
        dpi_urn = f"urn:li:dataProcessInstance:{run_id}"

        # Create basic properties and aspects
        aspects = [
            (
                properties
                or self._create_properties_class(
                    models.DataProcessInstancePropertiesClass, kwargs
                )
            ),
            models.SubTypesClass(typeNames=["ML Training Run"]),
        ]

        # Add training run properties if provided
        if training_run_properties:
            aspects.append(training_run_properties)

        # Handle run events
        current_time = int(time.time() * 1000)
        start_ts = start_timestamp or current_time
        end_ts = end_timestamp or current_time

        # Create events
        aspects.append(
            self._create_run_event(
                status=DataProcessRunStatusClass.STARTED, timestamp=start_ts
            )
        )

        if run_result:
            aspects.append(
                self._create_run_event(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestamp=end_ts,
                    result=run_result,
                    duration_millis=end_ts - start_ts,
                )
            )

        # Create and emit MCPs
        mcps = [self._create_mcp(dpi_urn, aspect) for aspect in aspects]
        self._emit_mcps(mcps)
        logger.info(f"Created training run: {dpi_urn}")
        return dpi_urn

    def create_dataset(self, name: str, platform: str, **kwargs: Any) -> str:
        """Create a dataset with flexible properties."""
        dataset = Dataset(id=name, platform=platform, name=name, **kwargs)
        mcps = list(dataset.generate_mcp())
        self._emit_mcps(mcps)
        if dataset.urn is None:
            raise ValueError(f"Failed to create dataset URN for {name}")
        return dataset.urn

    def add_run_to_model(self, model_urn: str, run_urn: str) -> None:
        """Add a run to a model while preserving existing properties."""
        self._update_entity_properties(
            entity_urn=model_urn,
            aspect_type=models.MLModelPropertiesClass,
            updates={"trainingJobs": run_urn},
            entity_type="mlModel",
            skip_properties=["trainingJobs"],
        )
        logger.info(f"Added run {run_urn} to model {model_urn}")

    def add_run_to_model_group(self, model_group_urn: str, run_urn: str) -> None:
        """Add a run to a model group while preserving existing properties."""
        self._update_entity_properties(
            entity_urn=model_group_urn,
            aspect_type=models.MLModelGroupPropertiesClass,
            updates={"trainingJobs": run_urn},
            entity_type="mlModelGroup",
            skip_properties=["trainingJobs"],
        )
        logger.info(f"Added run {run_urn} to model group {model_group_urn}")

    def add_model_to_model_group(self, model_urn: str, group_urn: str) -> None:
        """Add a model to a group while preserving existing properties"""
        self._update_entity_properties(
            entity_urn=model_urn,
            aspect_type=models.MLModelPropertiesClass,
            updates={"groups": group_urn},
            entity_type="mlModel",
            skip_properties=["groups"],
        )
        logger.info(f"Added model {model_urn} to group {group_urn}")

    def add_run_to_experiment(self, run_urn: str, experiment_urn: str) -> None:
        """Add a run to an experiment"""
        mcp = self._create_mcp(
            entity_urn=run_urn, aspect=models.ContainerClass(container=experiment_urn)
        )
        self._emit_mcps(mcp)
        logger.info(f"Added run {run_urn} to experiment {experiment_urn}")

    def add_input_datasets_to_run(self, run_urn: str, dataset_urns: List[str]) -> None:
        """Add input datasets to a run"""
        mcp = self._create_mcp(
            entity_urn=run_urn,
            entity_type="dataProcessInstance",
            aspect_name="dataProcessInstanceInput",
            aspect=DataProcessInstanceInput(inputs=dataset_urns),
        )
        self._emit_mcps(mcp)
        logger.info(f"Added input datasets to run {run_urn}")

    def add_output_datasets_to_run(self, run_urn: str, dataset_urns: List[str]) -> None:
        """Add output datasets to a run"""
        mcp = self._create_mcp(
            entity_urn=run_urn,
            entity_type="dataProcessInstance",
            aspect_name="dataProcessInstanceOutput",
            aspect=DataProcessInstanceOutput(outputs=dataset_urns),
        )
        self._emit_mcps(mcp)
        logger.info(f"Added output datasets to run {run_urn}")
