from typing import Any, Callable, Dict, Optional

from okta.models import User, UserStatus

from datahub.emitter.mce_builder import get_sys_time
from datahub.metadata.schema_classes import AuditStampClass, CorpUserStatusClass

CORP_USER_STATUS_ACTIVE = "ACTIVE"
CORP_USER_STATUS_SUSPENDED = "SUSPENDED"
INGESTION_ACTOR = "urn:li:corpuser:datahub"

AD_ACCOUNT_DISABLE_FLAG = 0x2  # USER_ACCOUNT_CONTROL_ACCOUNTDISABLE

OKTA_SUSPENDED_STATUSES = frozenset(
    {
        UserStatus.SUSPENDED,
        UserStatus.DEPROVISIONED,
        UserStatus.LOCKED_OUT,
    }
)

LDAP_DISABLED_INDICATORS = frozenset({"true", "1", "yes", "disabled"})


def corp_user_info_active_from_status(status: str) -> bool:
    return status == CORP_USER_STATUS_ACTIVE


def make_corp_user_status_aspect(status: str) -> CorpUserStatusClass:
    return CorpUserStatusClass(
        status=status,
        lastModified=AuditStampClass(time=get_sys_time(), actor=INGESTION_ACTOR),
    )


def derive_corp_user_status_from_ldap(
    attrs: Dict[str, Any],
    account_status_attr: str,
    get_attr: Optional[Callable[[Dict[str, Any], str], Optional[str]]] = None,
) -> str:
    attr_getter = get_attr or _default_get_attr
    raw = attr_getter(attrs, account_status_attr)
    if raw is None:
        return CORP_USER_STATUS_ACTIVE

    if account_status_attr.lower() == "useraccountcontrol":
        try:
            uac = int(raw)
            return (
                CORP_USER_STATUS_SUSPENDED
                if (uac & AD_ACCOUNT_DISABLE_FLAG) != 0
                else CORP_USER_STATUS_ACTIVE
            )
        except ValueError:
            return CORP_USER_STATUS_ACTIVE

    if raw.lower() in LDAP_DISABLED_INDICATORS:
        return CORP_USER_STATUS_SUSPENDED
    return CORP_USER_STATUS_ACTIVE


def _default_get_attr(attrs: Dict[str, Any], key: str) -> Optional[str]:
    if not attrs.get(key):
        return None
    value = attrs[key][0]
    return value.decode() if isinstance(value, bytes) else str(value)


def derive_corp_user_status_from_okta(okta_user: User) -> str:
    if okta_user.status in OKTA_SUSPENDED_STATUSES:
        return CORP_USER_STATUS_SUSPENDED
    return CORP_USER_STATUS_ACTIVE


def derive_corp_user_status_from_azure_ad(azure_ad_user: dict) -> str:
    account_enabled = azure_ad_user.get("accountEnabled")
    if account_enabled is False:
        return CORP_USER_STATUS_SUSPENDED
    return CORP_USER_STATUS_ACTIVE
