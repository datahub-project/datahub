import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Union

from datahub.api.dataflow import DataFlow
from datahub.api.datajob import DataJob
from datahub.api.urn import DataFlowUrn, DataJobUrn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceInput,
    DataProcessInstanceOutput,
    DataProcessInstanceProperties,
    DataProcessInstanceRelationships,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DataProcessTypeClass,
    RunResultTypeClass,
)
from datahub_provider.entities import _Entity


class DataProcessInstanceKey(DatahubKey):
    cluster: str
    orchestrator: str
    id: str


def make_data_process_instance_urn(dataProcessInstanceId: str) -> str:
    return f"urn:li:dataProcessInstance:{dataProcessInstanceId}"


@dataclass(frozen=True, eq=True)
class DataProcessInstanceUrn:
    key: DataProcessInstanceKey

    @property
    def urn(self):
        return make_data_process_instance_urn(f"{self.key.guid()}")


@dataclass
class DataProcessInstance:
    """This is a DataProcessInstance class which represent an instance of a DataFlow or DataJob.

    Args:
        id (str): The id of the dataprocess instance execution. This should be unique per execution but not needed to be globally unique.
        orchestrator (str): The orchestrator which does the execution. For example airflow.
        type (str): The execution type like Batch, Streaming, Ad-hoc, etc..  See valid values at DataProcessTypeClass
        template_urn (Optional[Union[DataJobUrn, DataFlowUrn]]): The parent DataJob or DataFlow which was instantiated if applicable
        parent_instance (Optional[DataProcessInstanceUrn]): The parent execution's urn if applicable
        properties Dict[str, str]: Custom properties to set for the DataProcessInstance
        url (Optional[str]): Url which points to the exection at the orchestrator
        inlets (List[_Entity]): List of entities the DataProcessInstance consumes
        outlest (List[_Entity]): List of entities the DataProcessInstance produces
    """

    id: str
    urn: DataProcessInstanceUrn = field(init=False)
    orchestrator: str
    cluster: str
    type: str = DataProcessTypeClass.BATCH_SCHEDULED
    template_urn: Optional[Union[DataJobUrn, DataFlowUrn]] = None
    parent_instance: Optional[DataProcessInstanceUrn] = None
    properties: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    inlets: List[_Entity] = field(default_factory=list)
    outlets: List[_Entity] = field(default_factory=list)
    upstreams_urns: List[DataProcessInstanceUrn] = field(default_factory=list)
    template_object: Optional[Union[DataJob, DataFlow]] = field(
        init=False, default=None
    )

    def __post_init__(self):
        self.urn = DataProcessInstanceUrn(
            key=DataProcessInstanceKey(
                cluster=self.cluster,
                orchestrator=self.orchestrator,
                id=self.id,
            )
        )

    @staticmethod
    def _entities_to_urn_list(iolets: List[_Entity]) -> List[str]:
        return [let.urn for let in iolets]

    def start_event_mcp(
        self, start_timestamp_millis: int, attempt: Optional[int] = None
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """

        :rtype: (Iterable[MetadataChangeProposalWrapper])
        :param start_timestamp_millis:  (int) the execution start time in milliseconds
        :param attempt: the number of attempt of the execution with the same execution id
        """
        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=self.urn.urn,
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.STARTED,
                timestampMillis=start_timestamp_millis,
                attempt=attempt,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

    def emit_process_start(
        self,
        emitter: DatahubRestEmitter,
        start_timestamp_millis: int,
        attempt: Optional[int] = None,
        emit_template: bool = True,
    ) -> None:
        """

        :rtype: None
        :param emitter: Datahub Emitter to emit the proccess event
        :param start_timestamp_millis: (int) the execution start time in milliseconds
        :param attempt: the number of attempt of the execution with the same execution id
        :param emit_template: (bool) If it is set the template of the execution (datajob, datflow) will be emitted as well.
        """
        if emit_template and self.template_urn is not None:
            template_object: Union[DataJob, DataFlow]
            if self.template_object is None:
                input_datajob_urns: List[DataJobUrn] = []
                if isinstance(self.template_urn, DataFlowUrn):
                    job_flow_urn = self.template_urn
                    template_object = DataFlow(
                        cluster=self.template_urn.cluster,
                        orchestrator=self.template_urn.orchestrator,
                        id=self.template_urn.id,
                    )
                    for mcp in template_object.generate_mcp():
                        emitter.emit(mcp)
                elif isinstance(self.template_urn, DataJobUrn):
                    job_flow_urn = self.template_urn.flow_urn
                    template_object = DataJob(
                        id=self.template_urn.id,
                        input_datajob_urns=input_datajob_urns,
                        flow_urn=self.template_urn.flow_urn,
                    )
                    for mcp in template_object.generate_mcp():
                        emitter.emit(mcp)
                else:
                    raise Exception(
                        f"Invalid urn type {self.template_urn.__class__.__name__}"
                    )
                for trigger in self.upstreams_urns:
                    input_datajob_urns.append(
                        DataJobUrn(id=trigger.key.id, flow_urn=job_flow_urn)
                    )
            else:
                template_object = self.template_object

            for mcp in template_object.generate_mcp():
                emitter.emit(mcp)

        for mcp in self.generate_mcp():
            emitter.emit(mcp)
        for mcp in self.start_event_mcp(start_timestamp_millis, attempt):
            emitter.emit(mcp)

    def end_event_mcp(
        self,
        end_timestamp_millis: int,
        result: RunResultTypeClass,
        result_type: Optional[str] = None,
        result_dict: Optional[Dict[str, str]] = None,
        attempt: Optional[int] = None,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """

        :param end_timestamp_millis:
        :param result:
        :param result_type:
        :param result_dict:
        :param attempt:
        """
        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=self.urn.urn,
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.COMPLETE,
                timestampMillis=end_timestamp_millis,
                result=DataProcessInstanceRunResultClass(
                    type=result,
                    nativeResultType=result_type
                    if result_type is not None
                    else self.orchestrator,
                ),
                attempt=attempt,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

    def emit_process_end(
        self,
        emitter: DatahubRestEmitter,
        end_timestamp_millis: int,
        result: RunResultTypeClass,
        result_type: Optional[str] = None,
        result_dict: Optional[Dict[str, str]] = None,
        attempt: Optional[int] = None,
    ) -> None:
        """

        :param emitter:
        :param end_timestamp_millis:
        :param result:
        :param result_type:
        :param result_dict:
        :param attempt:
        """
        for mcp in self.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            result_type=result_type,
            result_dict=result_dict,
            attempt=attempt,
        ):
            emitter.emit(mcp)

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Generates mcps from the object
        :rtype: Iterable[MetadataChangeProposalWrapper]
        """
        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=self.urn.urn,
            aspectName="dataProcessInstanceProperties",
            aspect=DataProcessInstanceProperties(
                name=self.id,
                created=AuditStampClass(
                    time=int(time.time() * 1000),
                    actor="urn:li:corpuser:datahub",
                ),
                type=self.type,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=self.urn.urn,
            aspectName="dataProcessInstanceRelationships",
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[urn.urn for urn in self.upstreams_urns],
                parentTemplate=self.template_urn.urn if self.template_urn else None,
                parentInstance=self.parent_instance.urn
                if self.parent_instance is not None
                else None,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        yield from self.generate_inlet_outlet_mcp()

    def emit(self, emitter: DatahubRestEmitter) -> None:
        """

        :param emitter:
        """
        for mcp in self.generate_mcp():
            emitter.emit(mcp)

    @staticmethod
    def from_datajob(
        data_job: DataJob,
        id: str,
        clone_inlets: bool = False,
        clone_outlets: bool = False,
    ) -> "DataProcessInstance":
        dpi: DataProcessInstance = DataProcessInstance(
            orchestrator=data_job.flow_urn.orchestrator,
            cluster=data_job.flow_urn.cluster,
            template_urn=data_job.urn,
            id=id,
        )
        dpi.template_object = data_job

        if clone_inlets:
            dpi.inlets = data_job.inlets
        if clone_outlets:
            dpi.outlets = data_job.outlets
        return dpi

    @staticmethod
    def from_dataflow(dataflow: DataFlow, id: str) -> "DataProcessInstance":
        dpi = DataProcessInstance(
            id=id,
            orchestrator=dataflow.orchestrator,
            cluster=dataflow.cluster,
            template_urn=dataflow.urn,
        )
        dpi.template_object = dataflow
        return dpi

    def generate_inlet_outlet_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.inlets:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataProcessInstance",
                entityUrn=self.urn.urn,
                aspectName="dataProcessInstanceInput",
                aspect=DataProcessInstanceInput(
                    inputs=[urn.urn for urn in self.inlets]
                ),
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

        if self.outlets:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataProcessInstance",
                entityUrn=self.urn.urn,
                aspectName="dataProcessInstanceOutput",
                aspect=DataProcessInstanceOutput(
                    outputs=[urn.urn for urn in self.outlets]
                ),
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

    def emit_inlet_outlet(self, emitter: DatahubRestEmitter) -> None:
        for mcp in self.generate_inlet_outlet_mcp():
            emitter.emit(mcp)
