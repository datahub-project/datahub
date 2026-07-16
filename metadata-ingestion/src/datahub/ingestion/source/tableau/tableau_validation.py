import logging
from typing import Dict, Union

from datahub.ingestion.api.source import CapabilityReport, SourceCapability
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau_server_wrapper import UserInfo

logger = logging.getLogger(__name__)


def check_user_role(
    logged_in_user: UserInfo,
) -> Dict[Union[SourceCapability, str], CapabilityReport]:
    capability_dict: Dict[Union[SourceCapability, str], CapabilityReport] = {
        c.SITE_PERMISSION: CapabilityReport(
            capable=True,
        )
    }

    failure_reason: str = (
        "The user does not have the `Site Administrator Explorer` role."
    )

    mitigation_message_prefix: str = (
        "Assign `Site Administrator Explorer` role to the user"
    )
    mitigation_message_suffix: str = "Refer to the setup guide: https://docs.datahub.com/docs/quick-ingestion-guides/tableau/setup"

    try:
        # TODO: Add check for `Enable Derived Permissions`
        if not logged_in_user.has_site_administrator_explorer_privileges():
            capability_dict[c.SITE_PERMISSION] = CapabilityReport(
                capable=False,
                failure_reason=f"{failure_reason} Their current role is {logged_in_user.site_role}.",
                mitigation_message=f"{mitigation_message_prefix} `{logged_in_user.user_name}`. {mitigation_message_suffix}",
            )

        return capability_dict

    except Exception as e:
        logger.warning(msg=e, exc_info=e)
        capability_dict[c.SITE_PERMISSION] = CapabilityReport(
            capable=False,
            failure_reason="Failed to verify user role.",
            mitigation_message=f"{mitigation_message_prefix}. {mitigation_message_suffix}",  # user is unknown
        )

        return capability_dict
