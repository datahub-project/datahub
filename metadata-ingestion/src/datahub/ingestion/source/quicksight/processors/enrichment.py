import logging
from typing import Callable, Dict, List, Optional

from datahub.emitter.mce_builder import make_group_urn, make_tag_urn, make_user_urn
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.quicksight.quicksight_urn import quicksight_user_id
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)


def _is_owner_principal(actions: List[str]) -> bool:
    """Whether a permission entry denotes an asset *owner*.

    QuickSight grants only the owner role an ``Update<Asset>Permissions`` action
    (the ability to manage the asset's share list); viewers get just
    ``Describe*`` / ``Query*``. Detecting that action is the most reliable
    owner signal across all asset types.
    """
    return any("Update" in action and "Permissions" in action for action in actions)


def _principal_to_owner_urn(
    principal_arn: str, strip_email_domain: bool = False
) -> Optional[str]:
    """Map a QuickSight permission principal ARN to a DataHub actor URN.

    Principal ARNs look like
    ``arn:aws:quicksight:<region>:<account>:user/<namespace>/<name>`` (or
    ``group/<namespace>/<name>``). For IAM/SSO-federated identities the name is
    ``<role>/<session>`` (e.g. ``AWSReservedSSO_Admin_<hash>/jane@acme.com``);
    the **session** (trailing segment) is the actual user identity — usually the
    email — so we use it rather than embedding the role prefix. This keeps the
    CorpUser id consistent with the plain email/username convention used by the
    PowerBI / Tableau / Sigma / Looker / Informatica connectors instead of a
    ``role/session`` composite.
    """
    resource = principal_arn.split(":")[-1]
    parts = resource.split("/")
    if len(parts) < 3:
        return None
    kind, name = parts[0], parts[-1]
    if not name:
        return None
    if kind == "user":
        return make_user_urn(quicksight_user_id(name, strip_email_domain))
    if kind == "group":
        return make_group_urn(name)
    return None


class AssetEnricher:
    """Resolves ownership (from ``Describe*Permissions``) and AWS resource tags
    (from ``ListTagsForResource``) for QuickSight assets.

    Shared by every asset processor so the owner/tag aspects ride along on the
    entity's single emission. Both lookups are opt-out via ``extract_ownership``
    / ``extract_tags`` and degrade to ``None`` (no aspect) on permission errors —
    these endpoints need extra IAM actions and per-asset resource permissions.
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
        # Asset kind -> the describe_*_permissions accessor for that kind.
        self._permission_fns: Dict[str, Callable[[str], List[dict]]] = {
            "dashboard": api.describe_dashboard_permissions,
            "analysis": api.describe_analysis_permissions,
            "data_set": api.describe_data_set_permissions,
            "data_source": api.describe_data_source_permissions,
            "folder": api.describe_folder_permissions,
        }

    def owners(self, kind: str, asset_id: str) -> Optional[List[OwnerClass]]:
        if not self.config.extract_ownership:
            return None
        try:
            permissions = self._permission_fns[kind](asset_id)
        except Exception as e:
            logger.info(f"Could not fetch {kind} permissions for {asset_id}: {e}")
            return None

        owners: List[OwnerClass] = []
        seen: set = set()
        for permission in permissions:
            if not _is_owner_principal(permission.get("Actions") or []):
                continue
            urn = _principal_to_owner_urn(
                permission.get("Principal") or "",
                self.config.strip_user_email_domain,
            )
            if urn and urn not in seen:
                seen.add(urn)
                owners.append(
                    OwnerClass(owner=urn, type=OwnershipTypeClass.TECHNICAL_OWNER)
                )
        if owners:
            self.report.num_assets_with_owners += 1
        return owners or None

    def tags(self, arn: str) -> Optional[List[TagAssociationClass]]:
        if not self.config.extract_tags or not arn:
            return None
        try:
            raw_tags = self.api.list_tags_for_resource(arn)
        except Exception as e:
            logger.info(f"Could not fetch tags for {arn}: {e}")
            return None

        tags: List[TagAssociationClass] = []
        seen: set = set()
        for tag in raw_tags:
            key = tag.get("Key")
            if not key:
                continue
            value = tag.get("Value") or ""
            # AWS tags are key=value; expose both so tag_pattern (documented as
            # matching `key=value`) can filter on either part.
            label = f"{key}={value}" if value else key
            if not self.config.tag_pattern.allowed(label):
                continue
            urn = make_tag_urn(label)
            if urn not in seen:
                seen.add(urn)
                tags.append(TagAssociationClass(tag=urn))
        if tags:
            self.report.num_assets_with_tags += 1
        return tags or None
