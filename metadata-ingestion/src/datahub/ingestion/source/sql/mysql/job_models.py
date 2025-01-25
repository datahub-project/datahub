from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
)


@dataclass
class MySQLProcedureDependency:
    db: str
    routine_schema: str
    routine_name: str
    type: str
    env: str
    server: Optional[str]
    source: str = "mysql"


@dataclass
class MySQLProcedureContainer:
    db: str
    platform_instance: Optional[str]
    name: str
    env: str
    source: str = "mysql"
    type: str = "STORED_PROCEDURE"

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
class MySQLStoredProcedure:
    routine_schema: str
    routine_name: str
    flow: MySQLProcedureContainer
    type: str = "STORED_PROCEDURE"
    source: str = field(init=False)
    code: Optional[str] = None

    def __post_init__(self):
        self.source = self.flow.source

    @property
    def full_type(self) -> str:
        return self.source.upper() + "_" + self.type

    @property
    def formatted_name(self) -> str:
        return self.routine_name.replace(",", "-")

    @property
    def full_name(self) -> str:
        return f"{self.routine_schema}.{self.formatted_name}"


@dataclass
class MySQLDataFlow:
    entity: MySQLProcedureContainer
    type: str = "dataFlow"
    source: str = "mysql"
    external_url: str = ""
    flow_properties: Dict[str, str] = field(default_factory=dict)

    def add_property(self, name: str, value: str) -> None:
        self.flow_properties[name] = value

    @property
    def urn(self) -> str:
        return make_data_flow_urn(
            orchestrator=self.entity.orchestrator,
            flow_id=self.entity.formatted_name,
            cluster=self.entity.cluster,
            platform_instance=(
                self.entity.platform_instance if self.entity.platform_instance else None
            ),
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


@dataclass
class MySQLDataJob:
    entity: MySQLStoredProcedure
    type: str = "dataJob"
    source: str = "mysql"
    external_url: str = ""
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
            platform_instance=(
                self.entity.flow.platform_instance
                if self.entity.flow.platform_instance
                else None
            ),
        )

    def add_property(self, name: str, value: str) -> None:
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
