import json
from typing import Dict, Iterable, List, Optional, Union

import pydantic
import requests
from pydantic import Field, validator
from requests.models import HTTPBasicAuth, HTTPError

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.amplitude.dataclasses import (
    AmplitudeEvent,
    AmplitudeEventProperties,
    AmplitudeProject,
    AmplitudeUserProperty,
    ProjectKey,
)
from datahub.ingestion.source.amplitude.utils import (
    FIELD_TYPE_MAPPING,
    MetadataIngestionException,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NullTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
)
from datahub.utilities import config_clean


class AmplitudeProjectConfig(ConfigModel):
    name: str = Field(description="The name of the Amplitude project")
    description: str = Field(default=None, description="A description of the project")
    api_key: pydantic.SecretStr = Field(description="The project API key")
    secret_key: pydantic.SecretStr = Field(description="The project API secret key")


class AmplitudeConfig(ConfigModel):
    connect_uri: str = Field(
        default="https://amplitude.com/api/2", description="Amplitude API endpoint"
    )
    projects: list[AmplitudeProjectConfig] = Field(
        description="Config for an Amplitude project"
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, uri):
        return config_clean.remove_trailing_slashes(uri)


class AmplitudeTaxonomyEndpoints:
    EVENT_TYPE: str = "/taxonomy/event"
    EVENT_PROPERTIES: str = "/taxonomy/event-property"
    USER_PROPERTIES: str = "/taxonomy/user-property"


@platform_name("Amplitude")
@config_class(AmplitudeConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Not supported", supported=False)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
class AmplitudeSource(Source):
    config: AmplitudeConfig
    report: SourceReport
    endpoints: AmplitudeTaxonomyEndpoints
    platform: str = "Amplitude"
    env: str = "PROD"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: AmplitudeConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.session = requests.session()
        self.endpoints = AmplitudeTaxonomyEndpoints()

    def build_project_from_config(
        self, config: AmplitudeProjectConfig
    ) -> Optional[AmplitudeProject]:
        project: Optional[AmplitudeProject] = self.map_config_project_values(config)
        return project

    def generate_project_key(self, project: AmplitudeProject) -> ProjectKey:
        return ProjectKey(
            instance=project.name, project_id=project.name, platform=self.platform
        )

    def make_get_request(
        self,
        endpoint: str,
        data: Optional[Dict],
        api_key: pydantic.SecretStr,
        secret_key: pydantic.SecretStr,
    ) -> json:
        try:
            assert api_key.get_secret_value().strip(), "Project api_key is required"
            assert (
                secret_key.get_secret_value().strip()
            ), "Project secret_key is required"

            response = self.session.get(
                url=f"{self.config.connect_uri}{endpoint}",
                data=data,
                auth=HTTPBasicAuth(
                    api_key.get_secret_value(), secret_key.get_secret_value()
                ),
            )
            response.raise_for_status()
            return response.json()
        except (AssertionError, HTTPError) as error:
            self.report.report_failure(key=f"{endpoint}", reason=f"Error: {error}")

    @staticmethod
    def map_config_project_values(
        project_config: AmplitudeProjectConfig,
    ) -> AmplitudeProject:
        return AmplitudeProject(
            name=project_config.name,
            description=project_config.description,
            events=[],
            user_properties=[],
        )

    def map_project_event_values(self, events: json, project: AmplitudeProject):
        event_data: List[Dict] = events.get("data")
        if event_data:
            for event in event_data:
                project.events.append(
                    AmplitudeEvent(
                        event_type=event["event_type"],
                        category=event["category"],
                        description=event["description"],
                        properties=[],
                    )
                )
        else:
            self.report.report_warning(
                key="Map project events", reason=f"No events for project {project.name}"
            )

    @staticmethod
    def map_project_user_properties_values(
        user_properties: json, project: AmplitudeProject
    ):
        user_properties_data: List[Dict] = user_properties.get("data")
        if user_properties_data:
            for user_prop in user_properties_data:
                project.user_properties.append(
                    AmplitudeUserProperty(
                        user_property=user_prop["user_property"],
                        description=user_prop["description"],
                        type=user_prop["type"],
                        enum_values=user_prop["enum_values"],
                        regex=user_prop["regex"],
                        is_array_type=user_prop["is_array_type"],
                    )
                )

    def get_event_category(self, event: AmplitudeEvent) -> str:
        try:
            event_category = event.category
            category_name = event_category["name"]
            return category_name
        except TypeError:
            self.report.report_warning(
                key="Get event category",
                reason=f"Event {event.event_type} has no category, assigning empty value",
            )
            return ""

    def add_properties_to_event(
        self,
        endpoint: str,
        project: AmplitudeProject,
        project_config: AmplitudeProjectConfig,
    ) -> None:
        events: Optional[List[AmplitudeEvent]] = project.get_events()
        if events:
            for event in events:
                event_type_dict: Dict = {"event_type": event.event_type}
                event_properties: json = self.make_get_request(
                    endpoint=endpoint,
                    data=event_type_dict,
                    api_key=project_config.api_key,
                    secret_key=project_config.secret_key,
                )
                event_properties_data: List[Dict] = event_properties.get("data", [])

                if event_properties_data:
                    for data in event_properties_data:
                        event.properties.append(
                            AmplitudeEventProperties(
                                event_property=data["event_property"],
                                event_type=data["event_type"],
                                description=data["description"],
                                type=data["type"],
                                regex=data["regex"],
                                enum_values=data["enum_values"],
                                is_array_type=data["is_array_type"],
                                is_required=data["is_required"],
                            )
                        )
                else:
                    self.report.report_warning(
                        key="Get event properties",
                        reason=f"No properties to add to event {event.event_type}",
                    )
        else:
            self.report.report_warning(
                key="Add properties to event",
                reason=f"No events to add properties to for project {project.name}",
            )

    def add_events_to_project(
        self,
        endpoint: str,
        project: AmplitudeProject,
        project_config: AmplitudeProjectConfig,
    ) -> None:
        events: json = self.make_get_request(
            endpoint=endpoint,
            data=None,
            api_key=project_config.api_key,
            secret_key=project_config.secret_key,
        )
        if events:
            self.map_project_event_values(events, project)
        else:
            self.report.report_warning(
                key="Add events to project",
                reason=f"No events to add to project {project.name}",
            )

    def add_user_properties_to_project(
        self,
        endpoint: str,
        project: AmplitudeProject,
        project_config: AmplitudeProjectConfig,
    ) -> None:
        user_properties = self.make_get_request(
            endpoint=endpoint,
            data=None,
            api_key=project_config.api_key,
            secret_key=project_config.secret_key,
        )
        if user_properties:
            self.map_project_user_properties_values(user_properties, project)
        else:
            self.report.report_failure(
                key="Add user properties to project",
                reason=f"No user properties for project {project.name}",
            )

    def get_metadata_change_event(
        self, snap_shot: Union["DatasetSnapshot"]
    ) -> MetadataWorkUnit:
        mce = MetadataChangeEvent(proposedSnapshot=snap_shot)
        wu = MetadataWorkUnit(id=snap_shot.urn, mce=mce)
        self.report.report_workunit(wu)
        return wu

    def emit_project_as_container(
        self, project: AmplitudeProject
    ) -> Iterable[MetadataWorkUnit]:
        project_container_key: ProjectKey = self.generate_project_key(project)
        container_workunits = gen_containers(
            container_key=project_container_key,
            name=project.name,
            sub_types=["Project"],
            description=project.description,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def emit_events_to_project(
        self, project: AmplitudeProject
    ) -> Iterable[MetadataWorkUnit]:
        project_container_key = self.generate_project_key(project)
        events: Optional[List[AmplitudeEvent]] = project.get_events()
        for event in events:
            event_metadata = self.map_properties_metadata_for_event_schema(event)
            event_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform, name=event.event_type, platform_instance=None
            )
            dataset_snapshot = DatasetSnapshot(urn=event_urn, aspects=[])
            dataset_snapshot.aspects.append(event_metadata)

            # dataset properties
            dataset_properties = DatasetPropertiesClass(
                name=event.event_type,
                description=event.description,
                customProperties={"event_category": self.get_event_category(event)},
            )
            dataset_snapshot.aspects.append(dataset_properties)

            event_workunits = add_entity_to_container(
                container_key=project_container_key,
                entity_type="dataset",
                entity_urn=dataset_snapshot.urn,
            )

            for wu in event_workunits:
                self.report.report_workunit(wu)
                yield wu
            yield self.get_metadata_change_event(dataset_snapshot)

    def emit_user_properties_to_project(
        self, project: AmplitudeProject
    ) -> Iterable[MetadataWorkUnit]:
        project_container_key = self.generate_project_key(project)
        user_prop_metadata = (
            self.map_user_properties_metadata_for_user_properties_schema(project)
        )
        user_prop_urn = builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{project.name}_user_properties",
            platform_instance=None,
        )
        dataset_snapshot = DatasetSnapshot(urn=user_prop_urn, aspects=[])
        dataset_snapshot.aspects.append(user_prop_metadata)

        # dataset properties
        dataset_properties = DatasetPropertiesClass(
            name=f"{project.name} project User Properties",
            description=(
                "A user property is an attribute that describes a useful "
                "detail about the user it's attached to. Amplitude sends "
                "user properties with every event"
            ),
        )
        dataset_snapshot.aspects.append(dataset_properties)

        user_prop_workunits = add_entity_to_container(
            container_key=project_container_key,
            entity_type="dataset",
            entity_urn=dataset_snapshot.urn,
        )

        for wu in user_prop_workunits:
            self.report.report_workunit(wu)
            yield wu
        yield self.get_metadata_change_event(dataset_snapshot)

    def map_properties_metadata_for_event_schema(
        self, event: AmplitudeEvent
    ) -> Optional[SchemaMetadata]:
        fields: List = []
        schema_name: str = event.event_type
        prop: Optional[List[AmplitudeEventProperties]] = event.get_properties()
        for p in prop:
            data_type = "string"
            TypeClass = FIELD_TYPE_MAPPING.get(p.type, NullTypeClass)

            schema_field = SchemaField(
                fieldPath=p.event_property,
                type=SchemaFieldDataType(type=TypeClass()),
                nativeDataType=data_type,
                description=p.description,
            )
            fields.append(schema_field)

        schema_metadata = SchemaMetadata(
            schemaName=schema_name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
        )
        return schema_metadata

    def map_user_properties_metadata_for_user_properties_schema(
        self, project: AmplitudeProject
    ) -> Optional[SchemaMetadata]:
        fields: List = []
        schema_name: str = "user_properties"
        user_props: Optional[
            List[AmplitudeUserProperty]
        ] = project.get_user_properties()
        for user_prop in user_props:
            data_type = "string"
            TypeClass = FIELD_TYPE_MAPPING.get(user_prop.type, NullTypeClass)

            schema_field = SchemaField(
                fieldPath=user_prop.user_property,
                type=SchemaFieldDataType(type=TypeClass()),
                nativeDataType=data_type,
                description=user_prop.description,
            )
            fields.append(schema_field)

        schema_metadata = SchemaMetadata(
            schemaName=schema_name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
        )
        return schema_metadata

    def emit_projects(self) -> Iterable[MetadataWorkUnit]:
        for project_conf in self.config.projects:

            # projects -> containers
            amplitude_project: AmplitudeProject = self.build_project_from_config(
                project_conf
            )
            yield from self.emit_project_as_container(amplitude_project)

            # events -> projects
            self.add_events_to_project(
                AmplitudeTaxonomyEndpoints.EVENT_TYPE, amplitude_project, project_conf
            )

            # properties -> events
            self.add_properties_to_event(
                AmplitudeTaxonomyEndpoints.EVENT_PROPERTIES,
                amplitude_project,
                project_conf,
            )
            yield from self.emit_events_to_project(amplitude_project)

            # user_properties -> project
            self.add_user_properties_to_project(
                AmplitudeTaxonomyEndpoints.USER_PROPERTIES,
                amplitude_project,
                project_conf,
            )
            yield from self.emit_user_properties_to_project(amplitude_project)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = AmplitudeConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self.emit_projects()
        except MetadataIngestionException as ingestion_exception:
            self.report.report_failure(
                key="Ingestion error",
                reason=f"Unable to ingest data from Amplitude. \n"
                f"Error: {str(ingestion_exception)}",
            )

    def get_report(self) -> SourceReport:
        return self.report
