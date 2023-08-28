from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from datahub.emitter.mce_builder import make_data_flow_urn, make_data_job_urn
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)


@dataclass
class ProcedureDependency:
    db: str
    schema: str
    name: str
    type: str
    env: str
    server: str
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
    platform_instance: str
    name: str
    env: str
    source: str = "mssql"
    type: str = "JOB"

    @property
    def formatted_name(self) -> str:
        return f"{self.formatted_platform_instance}.{self.name.replace(',', '-')}"

    @property
    def full_type(self) -> str:
        return f"({self.source},{self.formatted_name},{self.env})"

    @property
    def orchestrator(self) -> str:
        return self.source

    @property
    def formatted_platform_instance(self) -> str:
        return self.platform_instance.replace(".", "/")

    @property
    def cluster(self) -> str:
        return f"{self.env}"


@dataclass
class MSSQLProceduresContainer:
    db: str
    platform_instance: str
    name: str
    env: str
    source: str = "mssql"
    type: str = "JOB"

    @property
    def formatted_name(self) -> str:
        return f"{self.formatted_platform_instance}.{self.name.replace(',', '-')}"

    @property
    def orchestrator(self) -> str:
        return self.source

    @property
    def formatted_platform_instance(self) -> str:
        return self.platform_instance.replace(".", "/")

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
        return f"{self.formatted_name}.{self.formatted_name}"


@dataclass
class MSSQLDataJob:
    entity: Union[StoredProcedure, JobStep]
    type: str = "dataJob"
    source: str = "mssql"
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


@dataclass
class MSSQLDataFlow:
    entity: Union[MSSQLJob, MSSQLProceduresContainer]
    type: str = "dataFlow"
    source: str = "mssql"
    external_url: str = ""
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
        )

    @property
    def as_dataflow_info_aspect(self) -> DataFlowInfoClass:
        return DataFlowInfoClass(
            name=self.entity.formatted_name,
            customProperties=self.flow_properties,
            externalUrl=self.external_url,
        )
