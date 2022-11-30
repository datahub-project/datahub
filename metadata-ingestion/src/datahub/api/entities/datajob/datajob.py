from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Set, Union

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    AzkabanJobTypeClass,
    ChangeTypeClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataJobSnapshotClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

if TYPE_CHECKING:
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
    from datahub.emitter.rest_emitter import DatahubRestEmitter


@dataclass
class DataJob:
    """This is a DataJob class which represent a DataJob.

    Args:
        id (str): The id of the dataprocess instance execution. This should be unique per execution but not needed to be globally unique.
        orchestrator (str): The orchestrator which does the execution. For example airflow.
        type (str): The execution type like Batch, Streaming, Ad-hoc, etc..  See valid values at DataProcessTypeClass
        template_urn (Optional[Union[DataJobUrn, DataFlowUrn]]): The parent DataJob or DataFlow which was instantiated if applicable
        parent_instance (Optional[DataProcessInstanceUrn]): The parent execution's urn if applicable
        properties Dict[str, str]: Custom properties to set for the DataProcessInstance
        url (Optional[str]): Url which points to the DataJob at the orchestrator
        owners Set[str]): A list of user ids that own this job.
        group_owners Set[str]): A list of group ids that own this job.
        inlets (List[str]): List of urns the DataProcessInstance consumes
        outlets (List[str]): List of urns the DataProcessInstance produces
        input_datajob_urns: List[DataJobUrn] = field(default_factory=list)
    """

    id: str
    urn: DataJobUrn = field(init=False)
    flow_urn: DataFlowUrn
    name: Optional[str] = None
    description: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    owners: Set[str] = field(default_factory=set)
    group_owners: Set[str] = field(default_factory=set)
    inlets: List[DatasetUrn] = field(default_factory=list)
    outlets: List[DatasetUrn] = field(default_factory=list)
    upstream_urns: List[DataJobUrn] = field(default_factory=list)

    def __post_init__(self):
        job_flow_urn = DataFlowUrn.create_from_ids(
            env=self.flow_urn.get_env(),
            orchestrator=self.flow_urn.get_orchestrator_name(),
            flow_id=self.flow_urn.get_flow_id(),
        )
        self.urn = DataJobUrn.create_from_ids(
            data_flow_urn=str(job_flow_urn), job_id=self.id
        )

    def generate_ownership_aspect(self) -> Iterable[OwnershipClass]:
        owners = set([builder.make_user_urn(owner) for owner in self.owners]) | set(
            [builder.make_group_urn(owner) for owner in self.group_owners]
        )
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.DEVELOPER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SERVICE,
                        # url=dag.filepath,
                    ),
                )
                for urn in (owners or [])
            ],
            lastModified=AuditStampClass(
                time=0,
                actor=builder.make_user_urn(self.flow_urn.get_orchestrator_name()),
            ),
        )
        return [ownership]

    def generate_tags_aspect(self) -> Iterable[GlobalTagsClass]:
        tags = GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=builder.make_tag_urn(tag))
                for tag in (sorted(self.tags) or [])
            ]
        )
        return [tags]

    def generate_mce(self) -> MetadataChangeEventClass:
        job_mce = MetadataChangeEventClass(
            proposedSnapshot=DataJobSnapshotClass(
                urn=str(self.urn),
                aspects=[
                    DataJobInfoClass(
                        name=self.name if self.name is not None else self.id,
                        type=AzkabanJobTypeClass.COMMAND,
                        description=self.description,
                        customProperties=self.properties,
                        externalUrl=self.url,
                    ),
                    DataJobInputOutputClass(
                        inputDatasets=[str(urn) for urn in self.inlets],
                        outputDatasets=[str(urn) for urn in self.outlets],
                        inputDatajobs=[str(urn) for urn in self.upstream_urns],
                    ),
                    *self.generate_ownership_aspect(),
                    *self.generate_tags_aspect(),
                ],
            )
        )

        return job_mce

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityType="datajob",
            entityUrn=str(self.urn),
            aspectName="dataJobInfo",
            aspect=DataJobInfoClass(
                name=self.name if self.name is not None else self.id,
                type=AzkabanJobTypeClass.COMMAND,
                description=self.description,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        yield from self.generate_data_input_output_mcp()

        for owner in self.generate_ownership_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="datajob",
                entityUrn=str(self.urn),
                aspectName="ownership",
                aspect=owner,
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

        for tag in self.generate_tags_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="datajob",
                entityUrn=str(self.urn),
                aspectName="globalTags",
                aspect=tag,
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

    def emit(
        self,
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Emit the DataJob entity to Datahub

        :param emitter: Datahub Emitter to emit the process event
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        from datahub.emitter.kafka_emitter import DatahubKafkaEmitter

        for mcp in self.generate_mcp():
            if isinstance(emitter, DatahubKafkaEmitter):
                emitter.emit(mcp, callback)
            else:
                emitter.emit(mcp)

    def generate_data_input_output_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityType="datajob",
            entityUrn=str(self.urn),
            aspectName="dataJobInputOutput",
            aspect=DataJobInputOutputClass(
                inputDatasets=[str(urn) for urn in self.inlets],
                outputDatasets=[str(urn) for urn in self.outlets],
                inputDatajobs=[str(urn) for urn in self.upstream_urns],
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        # Force entity materialization
        for iolet in self.inlets + self.outlets:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=str(iolet),
                aspectName="status",
                aspect=StatusClass(removed=False),
                changeType=ChangeTypeClass.UPSERT,
            )

            yield mcp
