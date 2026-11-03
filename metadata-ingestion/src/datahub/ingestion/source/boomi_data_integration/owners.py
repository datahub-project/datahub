from typing import Dict, List, Optional, Set, Tuple

import requests
from pydantic import SecretStr

from datahub.ingestion.source.boomi_data_integration.client import (
    BoomiDataIntegrationApiError,
    BoomiDataIntegrationClient,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    ACCESS_DENIED_STATUS_CODES,
)
from datahub.ingestion.source.boomi_data_integration.models import RiverMetadata
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.metadata.schema_classes import OwnershipTypeClass
from datahub.metadata.urns import CorpUserUrn

Owners = List[Tuple[CorpUserUrn, str]]


def _name_key(name: str) -> str:
    return name.strip().casefold()


class OwnerResolver:
    # Rivers name their creator and last editor by display name only, so the
    # names are matched to emails through the account's users list.
    def __init__(
        self,
        client: BoomiDataIntegrationClient,
        report: BoomiDataIntegrationSourceReport,
    ) -> None:
        self.client = client
        self.report = report
        # Display name -> email, or None when several users share the name.
        self._emails: Optional[Dict[str, Optional[str]]] = None
        self._tried_environments: Set[str] = set()
        self._warned = False

    def owners(
        self, metadata: RiverMetadata, environment_id: str, token: SecretStr
    ) -> Optional[Owners]:
        emails = self._load(environment_id, token)
        if emails is None:
            return None
        owners: Owners = []
        creator = self._email(emails, metadata.created_by)
        if creator:
            owners.append((CorpUserUrn(creator), OwnershipTypeClass.DATAOWNER))
        editor = self._email(emails, metadata.last_updated_by)
        if editor and editor != creator:
            owners.append((CorpUserUrn(editor), OwnershipTypeClass.TECHNICAL_OWNER))
        self.report.owners_emitted += len(owners)
        return owners or None

    def _email(
        self, emails: Dict[str, Optional[str]], name: Optional[str]
    ) -> Optional[str]:
        if not name or not name.strip():
            return None
        key = _name_key(name)
        if key not in emails:
            self.report.owners_unknown_names += 1
            return None
        email = emails[key]
        if email is None:
            self.report.owners_ambiguous_names += 1
        return email

    def _load(
        self, environment_id: str, token: SecretStr
    ) -> Optional[Dict[str, Optional[str]]]:
        # The users list is account-wide, but tokens are scoped per
        # environment and may differ in scopes, so each environment's token is
        # tried once until one succeeds.
        if self._emails is not None or environment_id in self._tried_environments:
            return self._emails
        self._tried_environments.add(environment_id)
        try:
            users = list(self.client.list_users(token))
        except (
            BoomiDataIntegrationApiError,
            requests.RequestException,
            ValueError,
        ) as e:
            self.report.users_unavailable = True
            if not self._warned:
                self._warned = True
                denied = (
                    isinstance(e, BoomiDataIntegrationApiError)
                    and e.status_code in ACCESS_DENIED_STATUS_CODES
                )
                self.report.warning(
                    title="Users list not readable",
                    message=(
                        "The API token lacks the user:list scope, so river owners "
                        "are skipped."
                        if denied
                        else "The users list could not be read, so river owners are skipped."
                    ),
                    context=f"environment_id={environment_id}",
                    exc=e,
                )
            return None
        emails: Dict[str, Optional[str]] = {}
        for user in users:
            if not user.user_name or not user.user_email:
                continue
            key = _name_key(user.user_name)
            email = user.user_email.strip()
            if key in emails and emails[key] != email:
                emails[key] = None
            else:
                emails[key] = email
        self.report.users_listed = len(users)
        self.report.users_unavailable = False
        self._emails = emails
        return emails
