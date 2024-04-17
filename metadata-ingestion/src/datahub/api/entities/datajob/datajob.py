from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    AzkabanJobTypeClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
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
        fine_grained_lineages: Column lineage for the inlets and outlets
        upstream_urns: List[DataJobUrn] = field(default_factory=list)
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
    fine_grained_lineages: List[FineGrainedLineageClass] = field(default_factory=list)
    upstream_urns: List[DataJobUrn] = field(default_factory=list)

    def __post_init__(self):
        job_flow_urn = DataFlowUrn.create_from_ids(
            env=self.flow_urn.cluster,
            orchestrator=self.flow_urn.orchestrator,
            flow_id=self.flow_urn.flow_id,
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
                actor=builder.make_user_urn(self.flow_urn.orchestrator),
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

    def generate_mcp(
        self, materialize_iolets: bool = True
    ) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataJobInfoClass(
                name=self.name if self.name is not None else self.id,
                type=AzkabanJobTypeClass.COMMAND,
                description=self.description,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
        )
        yield mcp

        yield from self.generate_data_input_output_mcp(
            materialize_iolets=materialize_iolets
        )

        for owner in self.generate_ownership_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=owner,
            )
            yield mcp

        for tag in self.generate_tags_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=tag,
            )
            yield mcp

    def emit(
        self,
        emitter: Emitter,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Emit the DataJob entity to Datahub

        :param emitter: Datahub Emitter to emit the process event
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """

        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)

    def generate_data_input_output_mcp(
        self, materialize_iolets: bool
    ) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataJobInputOutputClass(
                inputDatasets=[str(urn) for urn in self.inlets],
                outputDatasets=[str(urn) for urn in self.outlets],
                inputDatajobs=[str(urn) for urn in self.upstream_urns],
                fineGrainedLineages=self.fine_grained_lineages,
            ),
        )
        yield mcp

        # Force entity materialization
        if materialize_iolets:
            for iolet in self.inlets + self.outlets:
                yield MetadataChangeProposalWrapper(
                    entityUrn=str(iolet),
                    aspect=StatusClass(removed=False),
                )
