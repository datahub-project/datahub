from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
    cast,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DataFlowInfoClass,
    DataFlowSnapshotClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.utilities.urns.data_flow_urn import DataFlowUrn

if TYPE_CHECKING:
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
    from datahub.emitter.rest_emitter import DatahubRestEmitter


@dataclass
class DataFlow:
    urn: DataFlowUrn = field(init=False)
    id: str
    orchestrator: str
    cluster: str
    name: Optional[str] = None
    description: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    owners: Set[str] = field(default_factory=set)

    def __post_init__(self):
        self.urn = DataFlowUrn.create_from_ids(
            orchestrator=self.orchestrator, env=self.cluster, flow_id=self.id
        )

    def generate_ownership_aspect(self):
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(owner),
                    type=OwnershipTypeClass.DEVELOPER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SERVICE,
                        # url=dag.filepath,
                    ),
                )
                for owner in (self.owners or [])
            ],
            lastModified=AuditStampClass(
                time=0, actor=builder.make_user_urn(self.orchestrator)
            ),
        )
        return [ownership]

    def generate_tags_aspect(self) -> List[GlobalTagsClass]:
        tags = GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=builder.make_tag_urn(tag))
                for tag in (sorted(self.tags) or [])
            ]
        )
        return [tags]

    def generate_mce(self) -> MetadataChangeEventClass:
        flow_mce = MetadataChangeEventClass(
            proposedSnapshot=DataFlowSnapshotClass(
                urn=str(self.urn),
                aspects=[
                    DataFlowInfoClass(
                        name=self.id,
                        description=self.description,
                        customProperties=self.properties,
                        externalUrl=self.url,
                    ),
                    *self.generate_ownership_aspect(),
                    *self.generate_tags_aspect(),
                ],
            )
        )

        return flow_mce

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityType="dataflow",
            entityUrn=str(self.urn),
            aspectName="dataFlowInfo",
            aspect=DataFlowInfoClass(
                name=self.name if self.name is not None else self.id,
                description=self.description,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        for owner in self.generate_ownership_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="dataflow",
                entityUrn=str(self.urn),
                aspectName="ownership",
                aspect=owner,
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

        for tag in self.generate_tags_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="dataflow",
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
        Emit the DataFlow entity to Datahub

        :param emitter: Datahub Emitter to emit the process event
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp():
            if type(emitter).__name__ == "DatahubKafkaEmitter":
                assert callback is not None
                kafka_emitter = cast("DatahubKafkaEmitter", emitter)
                kafka_emitter.emit(mcp, callback)
            else:
                rest_emitter = cast("DatahubRestEmitter", emitter)
                rest_emitter.emit(mcp)
