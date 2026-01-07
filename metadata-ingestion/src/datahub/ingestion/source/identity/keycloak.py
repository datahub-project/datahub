import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Iterable, List, Optional, Union

# Import Keycloak admin client
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from pydantic.fields import Field

from datahub.emitter.mce_builder import make_group_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    CorpGroupSnapshot,
    CorpUserSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
    OriginClass,
    OriginTypeClass,
    StatusClass,
)
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


class KeycloakConfig(StatefulIngestionConfigBase):
    # Required
    server_url: str = Field(
        description="The base URL of the Keycloak server, e.g. https://keycloak.example.com/auth or https://keycloak.example.com"
    )
    realm: str = Field(description="The Keycloak realm to ingest from")

    # Credentials (Service Account)
    client_id: str = Field(description="Client ID")
    client_secret: str = Field(description="Client Secret")

    # Optional flags
    verify_ssl: Union[bool, str] = Field(
        default=True,
        description="Whether to verify SSL certificates. If a string is provided, it is treated as a path to a CA bundle to use.",
    )

    ingest_users: bool = Field(
        default=True, description="Whether users should be ingested into DataHub."
    )
    ingest_groups: bool = Field(
        default=True, description="Whether groups should be ingested into DataHub."
    )
    ingest_group_membership: bool = Field(
        default=True,
        description="Whether group membership should be ingested into DataHub.",
    )

    # Masking
    mask_group_id: bool = Field(
        default=True,
        description="Whether workunit ID's for groups should be masked to avoid leaking sensitive information.",
    )
    mask_user_id: bool = Field(
        default=True,
        description="Whether workunit ID's for users should be masked to avoid leaking sensitive information.",
    )

    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Keycloak Stateful Ingestion Config."
    )


@dataclass
class KeycloakSourceReport(StaleEntityRemovalSourceReport):
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_filtered(self, name: str) -> None:
        self.filtered.append(name)


@platform_name("Keycloak")
@config_class(KeycloakConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
class KeycloakSource(StatefulIngestionSourceBase):
    """
    This plugin extracts Users, Groups, and Group Membership from Keycloak using python-keycloak.
    """

    config: KeycloakConfig
    report: KeycloakSourceReport
    keycloak_admin: KeycloakAdmin

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "KeycloakSource":
        config = KeycloakConfig.model_validate(config_dict)
        return cls(config, ctx)

    def __init__(self, config: KeycloakConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = KeycloakSourceReport()
        self.keycloak_admin = self._create_keycloak_admin()
        self.selected_groups: List[Dict] = []

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _create_keycloak_admin(self) -> KeycloakAdmin:
        try:
            keycloak_connection = KeycloakOpenIDConnection(
                server_url=self.config.server_url,
                realm_name=self.config.realm,
                client_id=self.config.client_id,
                client_secret_key=self.config.client_secret,
                verify=self.config.verify_ssl,
            )

            return KeycloakAdmin(connection=keycloak_connection)

        except Exception as e:
            error_str = f"Failed to initialize Keycloak client: {e}"
            self.report.report_failure("initialization", error_str)
            raise

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # 1. Ingest Groups
        if self.config.ingest_groups:
            for group_batch in self._get_keycloak_groups():
                datahub_corp_group_snapshots = list(
                    self._map_keycloak_groups(group_batch)
                )
                for group_count, datahub_corp_group_snapshot in enumerate(
                    datahub_corp_group_snapshots
                ):
                    mce = MetadataChangeEvent(
                        proposedSnapshot=datahub_corp_group_snapshot
                    )
                    wu_id = f"group-snapshot-{group_count + 1 if self.config.mask_group_id else datahub_corp_group_snapshot.urn}"
                    yield MetadataWorkUnit(id=wu_id, mce=mce)

                    # Yield Origin MCP
                    yield MetadataChangeProposalWrapper(
                        entityUrn=datahub_corp_group_snapshot.urn,
                        aspect=OriginClass(OriginTypeClass.EXTERNAL, "KEYCLOAK"),
                    ).as_workunit()

                    # Yield Status MCP
                    yield MetadataChangeProposalWrapper(
                        entityUrn=datahub_corp_group_snapshot.urn,
                        aspect=StatusClass(removed=False),
                    ).as_workunit()

        # 2. Populate Group Membership
        datahub_corp_user_urn_to_group_membership: Dict[str, GroupMembershipClass] = (
            defaultdict(lambda: GroupMembershipClass(groups=[]))
        )

        if self.config.ingest_group_membership and self.selected_groups:
            for group in self.selected_groups:
                group_urn = self._map_keycloak_group_to_urn(group)
                if group_urn:
                    self._populate_group_membership(
                        group, group_urn, datahub_corp_user_urn_to_group_membership
                    )

        # 3. Ingest Users
        if self.config.ingest_users:
            for user_batch in self._get_keycloak_users():
                datahub_corp_user_snapshots = self._map_keycloak_users(user_batch)
                yield from self.ingest_keycloak_users(
                    datahub_corp_user_snapshots,
                    datahub_corp_user_urn_to_group_membership,
                )

    def ingest_keycloak_users(
        self,
        datahub_corp_user_snapshots: Iterable[CorpUserSnapshot],
        datahub_corp_user_urn_to_group_membership: Dict[str, GroupMembershipClass],
    ) -> Generator[MetadataWorkUnit, Any, None]:
        for user_count, datahub_corp_user_snapshot in enumerate(
            datahub_corp_user_snapshots
        ):
            # Attach Group Membership
            if (
                datahub_corp_user_snapshot.urn
                in datahub_corp_user_urn_to_group_membership
            ):
                datahub_corp_user_snapshot.aspects.append(
                    datahub_corp_user_urn_to_group_membership[
                        datahub_corp_user_snapshot.urn
                    ]
                )

            mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_user_snapshot)
            wu_id = f"user-snapshot-{user_count + 1 if self.config.mask_user_id else datahub_corp_user_snapshot.urn}"
            yield MetadataWorkUnit(id=wu_id, mce=mce)

            yield MetadataChangeProposalWrapper(
                entityUrn=datahub_corp_user_snapshot.urn,
                aspect=OriginClass(OriginTypeClass.EXTERNAL, "KEYCLOAK"),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=datahub_corp_user_snapshot.urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

    def _get_keycloak_groups(self) -> Iterable[List[Dict]]:
        try:
            # Keycloak returns heirarchy, so we get all at once usually
            # But let's assume we might need to batch if API supported it,
            # for now python-keycloak get_groups fetches all top-level.
            groups = self.keycloak_admin.get_groups()
            flat_groups = self._flatten_groups(groups)
            # Yielding as a single batch since we flattened them
            yield flat_groups
        except Exception as e:
            error_str = f"Failed to fetch groups: {e}"
            self.report.report_failure("groups", error_str)

    def _flatten_groups(self, groups: List[Dict]) -> List[Dict]:
        """Recursively flatten groups"""
        flat = []
        for group in groups:
            flat.append(group)
            if "subGroups" in group:
                flat.extend(self._flatten_groups(group["subGroups"]))
        return flat

    def _map_keycloak_groups(self, groups: List[Dict]) -> Iterable[CorpGroupSnapshot]:
        for group in groups:
            group_urn = self._map_keycloak_group_to_urn(group)
            if not group_urn:
                continue

            # Store for membership processing
            self.selected_groups.append(group)

            corp_group_snapshot = CorpGroupSnapshot(
                urn=group_urn,
                aspects=[
                    CorpGroupInfoClass(
                        displayName=group.get("name"),
                        email=None,
                        admins=[],
                        members=[],
                        groups=[],
                        description=group.get("path"),
                    )
                ],
            )
            yield corp_group_snapshot

    def _map_keycloak_group_to_urn(self, group: Dict) -> Optional[str]:
        group_name = group.get("name")
        if not group_name:
            return None
        return make_group_urn(group_name)

    def _populate_group_membership(
        self,
        group: Dict,
        group_urn: str,
        user_urn_to_group_membership: Dict[str, GroupMembershipClass],
    ) -> None:
        try:
            group_id = group.get("id")
            if not group_id:
                return

            offset = 0
            limit = 100
            while True:
                members = self.keycloak_admin.get_group_members(
                    group_id=group_id, query={"first": offset, "max": limit}
                )
                if not members:
                    break

                for member in members:
                    user_urn = self._map_keycloak_user_to_urn(member)
                    if user_urn:
                        user_urn_to_group_membership[user_urn].groups.append(group_urn)

                if len(members) < limit:
                    break
                offset += limit
        except Exception as e:
            error_str = f"Failed to fetch members for group {group.get('name')}: {e}"
            self.report.report_warning("group_members", error_str)

    def _get_keycloak_users(self) -> Iterable[List[Dict]]:
        try:
            count = self.keycloak_admin.users_count()
            limit = 100
            for offset in range(0, count, limit):
                query = {"first": offset, "max": limit}
                users = self.keycloak_admin.get_users(query=query)
                yield users
        except Exception as e:
            error_str = f"Failed to fetch users: {e}"
            self.report.report_failure("users", error_str)

    def _map_keycloak_users(self, users: List[Dict]) -> Iterable[CorpUserSnapshot]:
        for user in users:
            user_urn = self._map_keycloak_user_to_urn(user)
            if not user_urn:
                continue

            corp_user_snapshot = CorpUserSnapshot(
                urn=user_urn,
                aspects=[self._map_keycloak_user_to_corp_user(user)],
            )
            yield corp_user_snapshot

    def _map_keycloak_user_to_urn(self, user: Dict) -> Optional[str]:
        username = user.get("username")
        email = user.get("email")

        # Prefer email for URN, fall back to username
        urn_id = email if email else username

        if not urn_id:
            logger.warning(
                f"User {user.get('id')} has neither username nor email. Skipping."
            )
            return None

        return make_user_urn(urn_id)

    def _map_keycloak_user_to_corp_user(self, user: Dict) -> CorpUserInfoClass:
        username = user.get("username")
        first_name = user.get("firstName")
        last_name = user.get("lastName")
        email = user.get("email")
        full_name = f"{first_name or ''} {last_name or ''}".strip() or username or email

        return CorpUserInfoClass(
            active=user.get("enabled", True),
            displayName=full_name,
            firstName=first_name,
            lastName=last_name,
            fullName=full_name,
            email=email,
        )

    def get_report(self):
        return self.report
