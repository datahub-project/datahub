from typing import Dict, List, Optional, Tuple

import requests_mock
from pydantic import SecretStr

from datahub.ingestion.source.boomi_data_integration.client import (
    BoomiDataIntegrationClient,
)
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.models import RiverMetadata
from datahub.ingestion.source.boomi_data_integration.owners import OwnerResolver
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)

USERS_URL = "https://api.rivery.io/v1/accounts/acct/users"
TOKEN = SecretStr("t")


def _resolver() -> OwnerResolver:
    config = BoomiDataIntegrationSourceConfig.model_validate(
        {"account_id": "acct", "api_token": "t", "max_requests_per_second": 50}
    )
    report = BoomiDataIntegrationSourceReport()
    return OwnerResolver(BoomiDataIntegrationClient(config, report), report)


def _users(*pairs: Tuple[str, str]) -> Dict[str, object]:
    return {
        "items": [
            {"user_id": f"u{i}", "user_name": name, "user_email": email}
            for i, (name, email) in enumerate(pairs)
        ],
        "next_page": "",
    }


def _owners(
    resolver: OwnerResolver,
    created_by: Optional[str],
    last_updated_by: Optional[str],
    environment_id: str = "env-1",
) -> Optional[List[Tuple[str, str]]]:
    owners = resolver.owners(
        RiverMetadata(created_by=created_by, last_updated_by=last_updated_by),
        environment_id,
        TOKEN,
    )
    return [(str(urn), kind) for urn, kind in owners] if owners else None


def test_creator_and_editor_resolve_to_emails() -> None:
    resolver = _resolver()
    with requests_mock.Mocker() as mock:
        mock.get(
            USERS_URL,
            json=_users(
                ("Example User", "svc@example.com"),
                ("Other User", "other@example.com"),
            ),
        )
        both = _owners(resolver, "example user ", "Other User")
        same = _owners(resolver, "Example User", "Example User")
        assert mock.call_count == 1

    assert both == [
        ("urn:li:corpuser:svc@example.com", "DATAOWNER"),
        ("urn:li:corpuser:other@example.com", "TECHNICAL_OWNER"),
    ]
    assert same == [("urn:li:corpuser:svc@example.com", "DATAOWNER")]
    assert resolver.report.owners_emitted == 3


def test_ambiguous_and_unknown_names_are_skipped_and_counted() -> None:
    resolver = _resolver()
    with requests_mock.Mocker() as mock:
        mock.get(
            USERS_URL,
            json=_users(
                ("Shared Name", "a@example.com"),
                ("Shared Name", "b@example.com"),
                ("Example User", "svc@example.com"),
                ("Example User", "svc@example.com"),
            ),
        )
        assert _owners(resolver, "Shared Name", "Nobody") is None
        assert _owners(resolver, None, "Example User") == [
            ("urn:li:corpuser:svc@example.com", "TECHNICAL_OWNER")
        ]

    assert resolver.report.owners_ambiguous_names == 1
    assert resolver.report.owners_unknown_names == 1


def test_forbidden_users_list_warns_once_and_retries_per_environment() -> None:
    resolver = _resolver()
    with requests_mock.Mocker() as mock:
        mock.get(USERS_URL, status_code=403, json={"detail": "Insufficient scopes"})
        assert _owners(resolver, "Example User", None) is None
        assert _owners(resolver, "Example User", None) is None
        assert mock.call_count == 1

        mock.get(USERS_URL, json=_users(("Example User", "svc@example.com")))
        assert _owners(resolver, "Example User", None, "env-2") == [
            ("urn:li:corpuser:svc@example.com", "DATAOWNER")
        ]

    assert [w.title for w in resolver.report.warnings] == ["Users list not readable"]
    assert resolver.report.users_unavailable is False


def test_users_list_server_error_skips_owners() -> None:
    resolver = _resolver()
    with requests_mock.Mocker() as mock:
        mock.get(USERS_URL, status_code=404, text="not found")
        assert _owners(resolver, "Example User", None) is None

    assert resolver.report.users_unavailable is True
    assert len(resolver.report.warnings) == 1
