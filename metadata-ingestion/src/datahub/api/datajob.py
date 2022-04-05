from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.api.dataflow import DataFlowUrn
from datahub.api.urn import DataJobUrn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub_provider.entities import _Entity


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
        inlets (List[_Entity]): List of entities the DataProcessInstance consumes
        outlest (List[_Entity]): List of entities the DataProcessInstance produces
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
    inlets: List[_Entity] = field(default_factory=list)
    outlets: List[_Entity] = field(default_factory=list)
    input_datajob_urns: List[DataJobUrn] = field(default_factory=list)

    def __post_init__(self):
        job_flow_urn = DataFlowUrn(
            cluster=self.flow_urn.cluster,
            orchestrator=self.flow_urn.orchestrator,
            id=self.flow_urn.id,
        )
        self.urn = DataJobUrn(flow_urn=job_flow_urn, id=self.id)

    def generate_ownership_aspect(self) -> Iterable[models.OwnershipClass]:
        ownership = models.OwnershipClass(
            owners=[
                models.OwnerClass(
                    owner=builder.make_user_urn(owner),
                    type=models.OwnershipTypeClass.DEVELOPER,
                    source=models.OwnershipSourceClass(
                        type=models.OwnershipSourceTypeClass.SERVICE,
                        # url=dag.filepath,
                    ),
                )
                for owner in (self.owners or [])
            ],
            lastModified=models.AuditStampClass(
                time=0, actor=builder.make_user_urn(self.flow_urn.orchestrator)
            ),
        )
        return [ownership]

    def generate_tags_aspect(self) -> Iterable[models.GlobalTagsClass]:
        tags = models.GlobalTagsClass(
            tags=[
                models.TagAssociationClass(tag=builder.make_tag_urn(tag))
                for tag in (self.tags or [])
            ]
        )
        return [tags]

    @staticmethod
    def _entities_to_urn_list(iolets: List[_Entity]) -> List[str]:
        return [let.urn for let in iolets]

    def generate_mce(self) -> models.MetadataChangeEventClass:
        job_mce = models.MetadataChangeEventClass(
            proposedSnapshot=models.DataJobSnapshotClass(
                urn=self.urn.urn,
                aspects=[
                    models.DataJobInfoClass(
                        name=self.name if self.name is not None else self.id,
                        type=models.AzkabanJobTypeClass.COMMAND,
                        description=self.description,
                        customProperties=self.properties,
                        externalUrl=self.url,
                    ),
                    models.DataJobInputOutputClass(
                        inputDatasets=self._entities_to_urn_list(self.inlets or []),
                        outputDatasets=self._entities_to_urn_list(self.outlets or []),
                        inputDatajobs=[urn.urn for urn in self.input_datajob_urns],
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
            entityUrn=self.urn.urn,
            aspectName="dataJobInfo",
            aspect=models.DataJobInfoClass(
                name=self.name if self.name is not None else self.id,
                type=models.AzkabanJobTypeClass.COMMAND,
                description=self.description,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
            changeType=models.ChangeTypeClass.UPSERT,
        )
        yield mcp

        yield from self.generate_data_input_output_mcp()

        for owner in self.generate_ownership_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="datajob",
                entityUrn=self.urn.urn,
                aspectName="ownership",
                aspect=owner,
                changeType=models.ChangeTypeClass.UPSERT,
            )
            yield mcp

        for tag in self.generate_tags_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="datajob",
                entityUrn=self.urn.urn,
                aspectName="globalTags",
                aspect=tag,
                changeType=models.ChangeTypeClass.UPSERT,
            )
            yield mcp

    def emit(self, emitter: DatahubRestEmitter) -> None:
        for mcp in self.generate_mcp():
            emitter.emit(mcp)

    def generate_data_input_output_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityType="datajob",
            entityUrn=self.urn.urn,
            aspectName="dataJobInputOutput",
            aspect=models.DataJobInputOutputClass(
                inputDatasets=self._entities_to_urn_list(self.inlets or []),
                outputDatasets=self._entities_to_urn_list(self.outlets or []),
                inputDatajobs=[urn.urn for urn in self.input_datajob_urns],
            ),
            changeType=models.ChangeTypeClass.UPSERT,
        )
        yield mcp

    def emit_data_inputs_output(self, emitter: DatahubRestEmitter) -> None:
        for mcp in self.generate_data_input_output_mcp():
            emitter.emit(mcp)
