from collections import defaultdict
from typing import Any, Dict, Iterable, List, Set

from botocore.exceptions import ClientError

from datahub.api.entities.corpgroup.corpgroup import CorpGroup
from datahub.api.entities.corpuser.corpuser import (
    CorpUser,
    CorpUserGenerationConfig,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.quicksight.quicksight_urn import quicksight_user_id

# QuickSight identity APIs are Enterprise-edition only and need extra IAM
# actions; degrade gracefully on these codes rather than failing the run.
_GRACEFUL_DEGRADE_CODES = frozenset(
    {
        "UnsupportedUserEditionException",
        "AccessDeniedException",
        "ResourceNotFoundException",
    }
)


def _is_graceful(error: Exception) -> bool:
    return (
        isinstance(error, ClientError)
        and error.response.get("Error", {}).get("Code") in _GRACEFUL_DEGRADE_CODES
    )


class UsersGroupsProcessor:
    """Emits QuickSight users and groups as DataHub ``CorpUser`` / ``CorpGroup``
    entities, plus each user's group memberships.

    Opt-in via ``extract_users_and_groups`` (default off) because the identity
    APIs are Enterprise-edition only, often noisy, and require additional IAM
    permissions. Group membership is materialized on the user side (a
    ``GroupMembership`` aspect per user) since QuickSight only exposes the
    group -> members direction.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_users_and_groups:
            return
        for namespace in self._namespaces():
            yield from self._emit_namespace_identity(namespace)

    def _namespaces(self) -> List[str]:
        try:
            names = [ns["Name"] for ns in self.api.list_namespaces() if ns.get("Name")]
        except Exception as e:
            if not _is_graceful(e):
                raise
            names = []
        return names or ["default"]

    def _emit_namespace_identity(self, namespace: str) -> Iterable[MetadataWorkUnit]:
        # group_id -> group summary, and user_name -> set(group_ids), built first
        # so each user can carry its full membership in a single emission.
        groups: List[Dict[str, Any]] = []
        memberships: Dict[str, Set[str]] = defaultdict(set)
        try:
            for group in self.api.list_groups(namespace):
                group_name = group.get("GroupName")
                if not group_name:
                    continue
                groups.append(group)
                for member in self._group_members(group_name, namespace):
                    member_name = member.get("MemberName")
                    if member_name:
                        memberships[member_name].add(group_name)
        except Exception as e:
            if not _is_graceful(e):
                raise
            self.report.warning(
                title="Skipping groups",
                message="QuickSight groups could not be listed for this namespace.",
                context=namespace,
                exc=e,
            )

        for group in groups:
            yield from self._emit_group(group)

        try:
            users = list(self.api.list_users(namespace))
        except Exception as e:
            if not _is_graceful(e):
                raise
            self.report.warning(
                title="Skipping users",
                message="QuickSight users could not be listed for this namespace.",
                context=namespace,
                exc=e,
            )
            users = []
        for user in users:
            yield from self._emit_user(
                user, sorted(memberships.get(user.get("UserName") or "", set()))
            )

    def _group_members(
        self, group_name: str, namespace: str
    ) -> Iterable[Dict[str, Any]]:
        try:
            yield from self.api.list_group_memberships(group_name, namespace)
        except Exception as e:
            if not _is_graceful(e):
                raise
            self.report.warning(
                title="Could not list group members",
                message="Group memberships for this group will be omitted.",
                context=group_name,
                exc=e,
            )

    def _emit_group(self, group: Dict[str, Any]) -> Iterable[MetadataWorkUnit]:
        group_name = group["GroupName"]
        corp_group = CorpGroup(
            id=group_name,
            display_name=group_name,
            description=group.get("Description") or None,
        )
        self.report.corp_groups.processed(group_name)
        # is_primary_source=False keeps these global identities out of the
        # stale-entity-removal checkpoint, so toggling extract_users_and_groups
        # off (or a transient identity-API failure) can't soft-delete CorpGroups
        # that other sources also populate. Matches the PowerBI connector.
        for mcp in corp_group.generate_mcp():
            yield mcp.as_workunit(is_primary_source=False)

    def _emit_user(
        self, user: Dict[str, Any], group_names: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        user_name = user.get("UserName")
        if not user_name:
            return
        # Normalize to the same id the ownership extractor uses (trailing
        # session segment for IAM/SSO users), so this CorpUser entity matches the
        # owner references on assets. The email is taken from the user record
        # when QuickSight provides it.
        user_id = quicksight_user_id(user_name, self.config.strip_user_email_domain)
        # Display the clean identity (email for IAM/SSO users, else the username)
        # rather than the raw "AWSReservedSSO_<role>_<hash>/<session>" string, so
        # owner pills in the UI read cleanly.
        corp_user = CorpUser(
            id=user_id,
            display_name=user.get("Email") or user_id,
            email=user.get("Email") or None,
            groups=group_names or None,
        )
        self.report.corp_users.processed(user_id)
        # override_editable=False keeps us on the ingestion-owned CorpUserInfo
        # aspect rather than the UI-editable one. is_primary_source=False keeps
        # these global identities out of the stale-entity-removal checkpoint (see
        # _emit_group). Matches the PowerBI connector.
        for mcp in corp_user.generate_mcp(
            generation_config=CorpUserGenerationConfig(override_editable=False)
        ):
            yield mcp.as_workunit(is_primary_source=False)
