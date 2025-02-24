import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Union, cast

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, DatahubKey
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceInput,
    DataProcessInstanceOutput,
    DataProcessInstanceProperties,
    DataProcessInstanceRelationships,
    RunResultType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataPlatformInstanceClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DataProcessTypeClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataPlatformInstanceUrn, DataPlatformUrn
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.data_process_instance_urn import DataProcessInstanceUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn


class DataProcessInstanceKey(DatahubKey):
    cluster: Optional[str] = None
    orchestrator: str
    id: str


class InstanceRunResult(StrEnum):
    SUCCESS = RunResultType.SUCCESS
    SKIPPED = RunResultType.SKIPPED
    FAILURE = RunResultType.FAILURE
    UP_FOR_RETRY = RunResultType.UP_FOR_RETRY


@dataclass
class DataProcessInstance:
    """This is a DataProcessInstance class which represents an instance of a DataFlow, DataJob, or a standalone process within a Container.

    Args:
        id: The id of the dataprocess instance execution.
        orchestrator: The orchestrator which does the execution. For example airflow.
        type: The execution type like Batch, Streaming, Ad-hoc, etc..  See valid values at DataProcessTypeClass
        template_urn: The parent DataJob or DataFlow which was instantiated if applicable
        parent_instance: The parent execution's urn if applicable
        properties: Custom properties to set for the DataProcessInstance
        url: Url which points to the execution at the orchestrator
        inlets: List of entities the DataProcessInstance consumes
        outlets: List of entities the DataProcessInstance produces
    """

    urn: DataProcessInstanceUrn = field(init=False)
    id: str
    orchestrator: str
    cluster: Optional[str] = None
    type: str = DataProcessTypeClass.BATCH_SCHEDULED
    template_urn: Optional[Union[DataJobUrn, DataFlowUrn, DatasetUrn]] = None
    parent_instance: Optional[DataProcessInstanceUrn] = None
    properties: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    inlets: List[DatasetUrn] = field(default_factory=list)
    outlets: List[DatasetUrn] = field(default_factory=list)
    upstream_urns: List[DataProcessInstanceUrn] = field(default_factory=list)
    _template_object: Optional[Union[DataJob, DataFlow]] = field(
        init=False, default=None, repr=False
    )
    data_platform_instance: Optional[str] = None
    subtype: Optional[str] = None
    container_urn: Optional[str] = None
    _platform: Optional[str] = field(init=False, repr=False, default=None)

    def __post_init__(self):
        self.urn = DataProcessInstanceUrn(
            id=DataProcessInstanceKey(
                cluster=self.cluster,
                orchestrator=self.orchestrator,
                id=self.id,
            ).guid()
        )
        self._platform = self.orchestrator

        try:
            # We first try to create from string assuming its an urn
            self._platform = str(DataPlatformUrn.from_string(self._platform))
        except Exception:
            # If it fails, we assume its an id
            self._platform = str(DataPlatformUrn(self._platform))

        if self.data_platform_instance is not None:
            try:
                # We first try to create from string assuming its an urn
                self.data_platform_instance = str(
                    DataPlatformInstanceUrn.from_string(self.data_platform_instance)
                )
            except Exception:
                # If it fails, we assume its an id
                self.data_platform_instance = str(
                    DataPlatformInstanceUrn(
                        platform=self._platform, instance=self.data_platform_instance
                    )
                )

    def start_event_mcp(
        self, start_timestamp_millis: int, attempt: Optional[int] = None
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """

        :rtype: (Iterable[MetadataChangeProposalWrapper])
        :param start_timestamp_millis:  (int) the execution start time in milliseconds
        :param attempt: (int) the number of attempt of the execution with the same execution id
        """
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.STARTED,
                timestampMillis=start_timestamp_millis,
                attempt=attempt,
            ),
        )
        yield mcp

    def emit_process_start(
        self,
        emitter: Emitter,
        start_timestamp_millis: int,
        attempt: Optional[int] = None,
        emit_template: bool = True,
        materialize_iolets: bool = True,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """

        :rtype: None
        :param emitter: Datahub Emitter to emit the process event
        :param start_timestamp_millis: the execution start time in milliseconds
        :param attempt: the number of attempt of the execution with the same execution id
        :param emit_template: If it is set the template of the execution (datajob, dataflow) will be emitted as well.
        :param materialize_iolets: If it is set the iolets will be materialized
        :param callback: the callback method for KafkaEmitter if it is used
        """
        if emit_template and self.template_urn is not None:
            template_object: Union[DataJob, DataFlow]
            if self._template_object is None:
                input_datajob_urns: List[DataJobUrn] = []
                if isinstance(self.template_urn, DataFlowUrn):
                    job_flow_urn = self.template_urn
                    template_object = DataFlow(
                        env=self.template_urn.get_env(),
                        orchestrator=self.template_urn.get_orchestrator_name(),
                        id=self.template_urn.get_flow_id(),
                    )
                    for mcp in template_object.generate_mcp():
                        self._emit_mcp(mcp, emitter, callback)
                elif isinstance(self.template_urn, DataJobUrn):
                    job_flow_urn = self.template_urn.get_data_flow_urn()
                    template_object = DataJob(
                        id=self.template_urn.get_job_id(),
                        upstream_urns=input_datajob_urns,
                        flow_urn=self.template_urn.get_data_flow_urn(),
                    )
                    for mcp in template_object.generate_mcp():
                        self._emit_mcp(mcp, emitter, callback)
                else:
                    raise Exception(
                        f"Invalid urn type {self.template_urn.__class__.__name__}"
                    )
                for upstream in self.upstream_urns:
                    input_datajob_urns.append(
                        DataJobUrn.create_from_ids(
                            job_id=upstream.get_dataprocessinstance_id(),
                            data_flow_urn=str(job_flow_urn),
                        )
                    )
            else:
                template_object = self._template_object

            for mcp in template_object.generate_mcp():
                self._emit_mcp(mcp, emitter, callback)

        for mcp in self.generate_mcp(
            created_ts_millis=start_timestamp_millis,
            materialize_iolets=materialize_iolets,
        ):
            self._emit_mcp(mcp, emitter, callback)
        for mcp in self.start_event_mcp(start_timestamp_millis, attempt):
            self._emit_mcp(mcp, emitter, callback)

    def end_event_mcp(
        self,
        end_timestamp_millis: int,
        result: InstanceRunResult,
        result_type: Optional[str] = None,
        attempt: Optional[int] = None,
        start_timestamp_millis: Optional[int] = None,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """

        :param end_timestamp_millis: the end time of the execution in milliseconds
        :param result: (InstanceRunResult) the result of the run
        :param result_type: (string) It identifies the system where the native result comes from like Airflow, Azkaban
        :param attempt: (int) the attempt number of this execution
        :param start_timestamp_millis: (Optional[int]) the start time of the execution in milliseconds

        """
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.COMPLETE,
                timestampMillis=end_timestamp_millis,
                result=DataProcessInstanceRunResultClass(
                    type=result,
                    nativeResultType=(
                        result_type if result_type is not None else self.orchestrator
                    ),
                ),
                attempt=attempt,
                durationMillis=(
                    (end_timestamp_millis - start_timestamp_millis)
                    if start_timestamp_millis
                    else None
                ),
            ),
        )
        yield mcp

    def emit_process_end(
        self,
        emitter: Emitter,
        end_timestamp_millis: int,
        result: InstanceRunResult,
        result_type: Optional[str] = None,
        attempt: Optional[int] = None,
        start_timestamp_millis: Optional[int] = None,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Generate an DataProcessInstance finish event and emits is

        :param emitter: (Emitter) the datahub emitter to emit generated mcps
        :param end_timestamp_millis: (int) the end time of the execution in milliseconds
        :param result: (InstanceRunResult) The result of the run
        :param result_type: (string) It identifies the system where the native result comes from like Airflow, Azkaban
        :param attempt: (int) the attempt number of this execution
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        :param start_timestamp_millis: (Optional[int]) the start time of the execution in milliseconds

        """
        for mcp in self.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            result_type=result_type,
            attempt=attempt,
            start_timestamp_millis=start_timestamp_millis,
        ):
            self._emit_mcp(mcp, emitter, callback)

    def generate_mcp(
        self, created_ts_millis: Optional[int], materialize_iolets: bool
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Generates mcps from the object"""

        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataProcessInstanceProperties(
                name=self.id,
                created=AuditStampClass(
                    time=created_ts_millis or int(time.time() * 1000),
                    actor="urn:li:corpuser:datahub",
                ),
                type=self.type,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
        )
        yield mcp

        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[str(urn) for urn in self.upstream_urns],
                parentTemplate=str(self.template_urn) if self.template_urn else None,
                parentInstance=(
                    str(self.parent_instance)
                    if self.parent_instance is not None
                    else None
                ),
            ),
        )
        yield mcp

        assert self._platform
        if self.data_platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=DataPlatformInstanceClass(
                    platform=self._platform, instance=self.data_platform_instance
                ),
            )
            yield mcp

        if self.subtype:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn), aspect=SubTypesClass(typeNames=[self.subtype])
            )
            yield mcp

        if self.container_urn:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=ContainerClass(container=self.container_urn),
            )
            yield mcp

        yield from self.generate_inlet_outlet_mcp(materialize_iolets=materialize_iolets)

    @staticmethod
    def _emit_mcp(
        mcp: MetadataChangeProposalWrapper,
        emitter: Emitter,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """

        :param emitter: (Emitter) the datahub emitter to emit generated mcps
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        emitter.emit(mcp, callback)

    def emit(
        self,
        emitter: Emitter,
        callback: Optional[Callable[[Exception, str], None]] = None,
        created_ts_millis: Optional[int] = None,
    ) -> None:
        """

        :param emitter: (Emitter) the datahub emitter to emit generated mcps
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp(
            created_ts_millis=created_ts_millis,
            materialize_iolets=True,
        ):
            self._emit_mcp(mcp, emitter, callback)

    @staticmethod
    def from_datajob(
        datajob: DataJob,
        id: str,
        clone_inlets: bool = False,
        clone_outlets: bool = False,
    ) -> "DataProcessInstance":
        """
        Generates a DataProcessInstance from a given DataJob.

        This method creates a DataProcessInstance object using the provided DataJob
        and assigns it a unique identifier. Optionally, it can clone the inlets and
        outlets from the DataJob to the DataProcessInstance.

        Args:
            datajob (DataJob): The DataJob instance from which to generate the DataProcessInstance.
            id (str): The unique identifier for the DataProcessInstance.
            clone_inlets (bool, optional): If True, clones the inlets from the DataJob to the DataProcessInstance. Defaults to False.
            clone_outlets (bool, optional): If True, clones the outlets from the DataJob to the DataProcessInstance. Defaults to False.

        Returns:
            DataProcessInstance: The generated DataProcessInstance object.
        """
        dpi: DataProcessInstance = DataProcessInstance(
            orchestrator=datajob.flow_urn.orchestrator,
            cluster=datajob.flow_urn.cluster,
            template_urn=datajob.urn,
            id=id,
        )
        dpi._template_object = datajob

        if clone_inlets:
            dpi.inlets = datajob.inlets
        if clone_outlets:
            dpi.outlets = datajob.outlets
        return dpi

    @staticmethod
    def from_container(
        container_key: ContainerKey,
        id: str,
    ) -> "DataProcessInstance":
        """
        Create a DataProcessInstance that is located within a Container.
        Use this method when you need to represent a DataProcessInstance that
        is not an instance of a DataJob or a DataFlow.
        e.g. If recording an ad-hoc training run that is just associated with an Experiment.

        :param container_key: (ContainerKey) the container key to generate the DataProcessInstance
        :param id: (str) the id for the DataProcessInstance
        :return: DataProcessInstance
        """
        dpi: DataProcessInstance = DataProcessInstance(
            id=id,
            orchestrator=DataPlatformUrn.from_string(
                container_key.platform
            ).platform_name,
            template_urn=None,
            container_urn=container_key.as_urn(),
        )

        return dpi

    @staticmethod
    def from_dataflow(dataflow: DataFlow, id: str) -> "DataProcessInstance":
        """
        Creates a DataProcessInstance from a given DataFlow.

        This method generates a DataProcessInstance object using the provided DataFlow
        and a specified id. The DataProcessInstance will inherit properties from the
        DataFlow such as orchestrator, environment, and template URN.

        Args:
            dataflow (DataFlow): The DataFlow object from which to generate the DataProcessInstance.
            id (str): The unique identifier for the DataProcessInstance.

        Returns:
            DataProcessInstance: The newly created DataProcessInstance object.
        """
        dpi = DataProcessInstance(
            id=id,
            orchestrator=dataflow.orchestrator,
            cluster=cast(str, dataflow.env),
            template_urn=dataflow.urn,
        )
        dpi._template_object = dataflow
        return dpi

    def generate_inlet_outlet_mcp(
        self, materialize_iolets: bool
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if self.inlets:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=DataProcessInstanceInput(
                    inputs=[str(urn) for urn in self.inlets]
                ),
            )
            yield mcp

        if self.outlets:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=DataProcessInstanceOutput(
                    outputs=[str(urn) for urn in self.outlets]
                ),
            )
            yield mcp

        # Force entity materialization
        if materialize_iolets:
            for iolet in self.inlets + self.outlets:
                yield MetadataChangeProposalWrapper(
                    entityUrn=str(iolet),
                    aspect=iolet.to_key_aspect(),
                )
