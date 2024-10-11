import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Set, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import ALL_ENV_TYPES
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataFlowInfoClass,
    DataFlowSnapshotClass,
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

logger = logging.getLogger(__name__)


@dataclass
class DataFlow:
    """The DataHub representation of data-flow.

    Args:
        urn (int): Unique identifier of the DataFlow in DataHub. For more detail refer https://datahubproject.io/docs/what/urn/.
        id (str): Identifier of DataFlow in orchestrator.
        orchestrator (str): orchestrator. for example airflow.
        cluster (Optional[str]): [deprecated] Please use env.
        name (str): Name of the DataFlow.
        description (str): Description about DataFlow
        properties (Optional[str]): Additional properties if any.
        url (Optional[str]): URL pointing to DataFlow.
        tags (Set[str]): tags that need to be apply on DataFlow.
        owners (Set[str]): owners that need to be apply on DataFlow.
        platform_instance (Optional[str]): The instance of the platform that all assets produced by this orchestrator belong to. For more detail refer https://datahubproject.io/docs/platform-instances/.
        env (Optional[str]): The environment that all assets produced by this orchestrator belong to. For more detail and possible values refer https://datahubproject.io/docs/graphql/enums/#fabrictype.
    """

    urn: DataFlowUrn = field(init=False)
    id: str
    orchestrator: str
    cluster: Optional[str] = None  # Deprecated in favor of env
    name: Optional[str] = None
    description: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    owners: Set[str] = field(default_factory=set)
    group_owners: Set[str] = field(default_factory=set)
    platform_instance: Optional[str] = None
    env: Optional[str] = None

    def __post_init__(self):
        if self.env is not None and self.cluster is not None:
            raise ValueError(
                "Cannot provide both env and cluster parameter. Cluster is deprecated in favor of env."
            )

        if self.env is None and self.cluster is None:
            raise ValueError("env is required")

        if self.cluster is not None:
            logger.warning(
                "The cluster argument is deprecated. Use env and possibly platform_instance instead."
            )
            self.env = self.cluster

        # self.env is optional, cast is required to pass lint
        self.urn = DataFlowUrn.create_from_ids(
            orchestrator=self.orchestrator,
            env=cast(str, self.env),
            flow_id=self.id,
            platform_instance=self.platform_instance,
        )

    def generate_ownership_aspect(self):
        owners = {builder.make_user_urn(owner) for owner in self.owners} | {
            builder.make_group_urn(owner) for owner in self.group_owners
        }
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

    def _get_env(self) -> Optional[str]:
        env: Optional[str] = None
        if self.env and self.env.upper() in ALL_ENV_TYPES:
            env = self.env.upper()
        else:
            logger.debug(
                f"{self.env} is not a valid environment type so Environment filter won't work."
            )
        return env

    def generate_mce(self) -> MetadataChangeEventClass:
        env = self._get_env()
        flow_mce = MetadataChangeEventClass(
            proposedSnapshot=DataFlowSnapshotClass(
                urn=str(self.urn),
                aspects=[
                    DataFlowInfoClass(
                        name=self.id,
                        description=self.description,
                        customProperties=self.properties,
                        externalUrl=self.url,
                        env=env,
                    ),
                    *self.generate_ownership_aspect(),
                    *self.generate_tags_aspect(),
                ],
            )
        )

        return flow_mce

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        env = self._get_env()
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=DataFlowInfoClass(
                name=self.name if self.name is not None else self.id,
                description=self.description,
                customProperties=self.properties,
                externalUrl=self.url,
                env=env,
            ),
        )
        yield mcp

        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=StatusClass(
                removed=False,
            ),
        )
        yield mcp

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
        Emit the DataFlow entity to Datahub

        :param emitter: Datahub Emitter to emit the process event
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """

        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)
