from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
)


@dataclass
class ProcedureDependency:
    db: str
    schema: str
    name: str
    type: str
    env: str
    server: Optional[str]
    source: str = "mssql"


@dataclass
class ProcedureLineageStream:
    dependencies: List[ProcedureDependency]

    @property
    def as_property(self) -> Dict[str, str]:
        return {
            f"{dep.db}.{dep.schema}.{dep.name}": dep.type for dep in self.dependencies
        }


@dataclass
class MSSQLJob:
    db: str
    platform_instance: Optional[str]
    name: str
    env: str
    source: str = "mssql"
    type: str = "JOB"

    @property
    def formatted_name(self) -> str:
        return self.name.replace(",", "-")

    @property
    def full_type(self) -> str:
        return f"({self.source},{self.formatted_name},{self.env})"

    @property
    def orchestrator(self) -> str:
        return self.source

    @property
    def cluster(self) -> str:
        return f"{self.env}"


@dataclass
class MSSQLProceduresContainer:
    db: str
    platform_instance: Optional[str]
    name: str
    env: str
    source: str = "mssql"
    type: str = "JOB"

    @property
    def formatted_name(self) -> str:
        return self.name.replace(",", "-")

    @property
    def orchestrator(self) -> str:
        return self.source

    @property
    def cluster(self) -> str:
        return f"{self.env}"

    @property
    def full_type(self) -> str:
        return f"({self.source},{self.name},{self.env})"


@dataclass
class ProcedureParameter:
    name: str
    type: str

    @property
    def properties(self) -> Dict[str, str]:
        return {"type": self.type}


@dataclass
class StoredProcedure:
    db: str
    schema: str
    name: str
    flow: Union[MSSQLJob, MSSQLProceduresContainer]
    type: str = "STORED_PROCEDURE"
    source: str = "mssql"
    code: Optional[str] = None

    @property
    def full_type(self) -> str:
        return self.source.upper() + "_" + self.type

    @property
    def formatted_name(self) -> str:
        return self.name.replace(",", "-")

    @property
    def full_name(self) -> str:
        return f"{self.db}.{self.schema}.{self.formatted_name}"

    @property
    def escape_full_name(self) -> str:
        return f"[{self.db}].[{self.schema}].[{self.formatted_name}]"


@dataclass
class JobStep:
    job_name: str
    step_name: str
    flow: MSSQLJob
    type: str = "JOB_STEP"
    source: str = "mssql"

    @property
    def formatted_step(self) -> str:
        return self.step_name.replace(",", "-").replace(" ", "_").lower()

    @property
    def formatted_name(self) -> str:
        return self.job_name.replace(",", "-")

    @property
    def full_type(self) -> str:
        return self.source.upper() + "_" + self.type

    @property
    def full_name(self) -> str:
        return self.formatted_name


@dataclass
class MSSQLDataJob:
    entity: Union[StoredProcedure, JobStep]
    type: str = "dataJob"
    source: str = "mssql"
    external_url: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    incoming: List[str] = field(default_factory=list)
    outgoing: List[str] = field(default_factory=list)
    input_jobs: List[str] = field(default_factory=list)
    job_properties: Dict[str, str] = field(default_factory=dict)

    @property
    def urn(self) -> str:
        return make_data_job_urn(
            orchestrator=self.entity.flow.orchestrator,
            flow_id=self.entity.flow.formatted_name,
            job_id=self.entity.formatted_name,
            cluster=self.entity.flow.cluster,
            platform_instance=self.entity.flow.platform_instance,
        )

    def add_property(
        self,
        name: str,
        value: str,
    ) -> None:
        self.job_properties[name] = value

    @property
    def valued_properties(self) -> Dict[str, str]:
        if self.job_properties:
            return {k: v for k, v in self.job_properties.items() if v is not None}
        return self.job_properties

    @property
    def as_datajob_input_output_aspect(self) -> DataJobInputOutputClass:
        return DataJobInputOutputClass(
            inputDatasets=sorted(self.incoming),
            outputDatasets=sorted(self.outgoing),
            inputDatajobs=sorted(self.input_jobs),
        )

    @property
    def as_datajob_info_aspect(self) -> DataJobInfoClass:
        return DataJobInfoClass(
            name=self.entity.full_name,
            type=self.entity.full_type,
            description=self.description,
            customProperties=self.valued_properties,
            externalUrl=self.external_url,
            status=self.status,
        )

    @property
    def as_maybe_platform_instance_aspect(self) -> Optional[DataPlatformInstanceClass]:
        if self.entity.flow.platform_instance:
            return DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.entity.flow.orchestrator),
                instance=make_dataplatform_instance_urn(
                    platform=self.entity.flow.orchestrator,
                    instance=self.entity.flow.platform_instance,
                ),
            )
        return None

    @property
    def as_container_aspect(self) -> ContainerClass:
        key_args = dict(
            platform=self.entity.flow.orchestrator,
            instance=self.entity.flow.platform_instance,
            env=self.entity.flow.env,
            database=self.entity.flow.db,
        )
        container_key = (
            SchemaKey(
                schema=self.entity.schema,
                **key_args,
            )
            if isinstance(self.entity, StoredProcedure)
            else DatabaseKey(
                **key_args,
            )
        )
        return ContainerClass(container=container_key.as_urn())


@dataclass
class MSSQLDataFlow:
    entity: Union[MSSQLJob, MSSQLProceduresContainer]
    type: str = "dataFlow"
    source: str = "mssql"
    external_url: Optional[str] = None
    flow_properties: Dict[str, str] = field(default_factory=dict)

    def add_property(
        self,
        name: str,
        value: str,
    ) -> None:
        self.flow_properties[name] = value

    @property
    def urn(self) -> str:
        return make_data_flow_urn(
            orchestrator=self.entity.orchestrator,
            flow_id=self.entity.formatted_name,
            cluster=self.entity.cluster,
            platform_instance=self.entity.platform_instance,
        )

    @property
    def as_dataflow_info_aspect(self) -> DataFlowInfoClass:
        return DataFlowInfoClass(
            name=self.entity.formatted_name,
            customProperties=self.flow_properties,
            externalUrl=self.external_url,
        )

    @property
    def as_maybe_platform_instance_aspect(self) -> Optional[DataPlatformInstanceClass]:
        if self.entity.platform_instance:
            return DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.entity.orchestrator),
                instance=make_dataplatform_instance_urn(
                    self.entity.orchestrator, self.entity.platform_instance
                ),
            )
        return None

    @property
    def as_container_aspect(self) -> ContainerClass:
        databaseKey = DatabaseKey(
            platform=self.entity.orchestrator,
            instance=self.entity.platform_instance,
            env=self.entity.env,
            database=self.entity.db,
        )
        return ContainerClass(container=databaseKey.as_urn())
