"""URN construction helpers shared across QuickSight processors.

Centralized so the Dataset URNs referenced by dashboards/analyses lineage stay
byte-identical to the URNs the dataset processor emits.
"""

from typing import Optional

import datahub.emitter.mce_builder as builder
from datahub.metadata.urns import DashboardUrn

PLATFORM = "quicksight"


def quicksight_user_id(user_name: str, strip_email_domain: bool = False) -> str:
    """Normalize a QuickSight user name to a DataHub CorpUser id.

    QuickSight identifies IAM/SSO-federated users as ``<role>/<session>`` (e.g.
    ``AWSReservedSSO_Admin_<hash>/jane@acme.com``); the session (trailing
    segment) is the real identity, usually the email. Taking it keeps CorpUser
    ids consistent with the plain email/username convention of the other BI
    connectors — and ensures the user *entity* emitted by the users/groups
    processor matches the owner *reference* derived from permission ARNs.
    """
    name = user_name.split("/")[-1]
    if strip_email_domain and "@" in name:
        name = name.split("@", 1)[0]
    return name


def id_from_arn(arn: str) -> str:
    """Return the trailing resource id of a QuickSight ARN.

    e.g. ``arn:aws:quicksight:us-east-1:123:dataset/abc-123`` -> ``abc-123``.
    """
    return arn.split("/")[-1]


def make_dataset_urn(
    account_id: str, dataset_id: str, platform_instance: Optional[str], env: str
) -> str:
    return builder.make_dataset_urn_with_platform_instance(
        platform=PLATFORM,
        name=f"{account_id}.{dataset_id}",
        platform_instance=platform_instance,
        env=env,
    )


def make_dashboard_urn(asset_id: str, platform_instance: Optional[str]) -> str:
    """Build a Dashboard URN for a QuickSight analysis or dashboard.

    Both analyses and dashboards map to DataHub Dashboard entities; the URN is
    ``(quicksight, {id})`` with the platform instance folded in when configured.
    """
    return DashboardUrn.create_from_ids(
        platform=PLATFORM,
        name=asset_id,
        platform_instance=platform_instance,
    ).urn()


def chart_id_from_visual(parent_id: str, visual_id: str) -> str:
    """Build the stable Chart name for a QuickSight visual.

    QuickSight auto-generates ``VisualId`` values that already embed the parent
    analysis/dashboard id (e.g. ``{dashboard_id}_sheet-1_visual_1``). We avoid
    doubling that prefix while still guaranteeing the chart id is globally unique
    for user-authored visuals whose ids are bare (e.g. ``visual_1``).
    """
    return visual_id if visual_id.startswith(parent_id) else f"{parent_id}_{visual_id}"
