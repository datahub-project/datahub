from typing import Dict, Set, Union
from unittest.mock import MagicMock

import pytest
from datahub.ingestion.graph.client import AspectBag, DataHubGraph
from datahub.metadata.schema_classes import (
    CorpGroupEditableInfoClass,
    CorpGroupInfoClass,
    CorpUserAppearanceSettingsClass,
    CorpUserEditableInfoClass,
    CorpUserInfoClass,
    CorpUserSettingsClass,
    NotificationSettingsClass,
    SlackNotificationSettingsClass,
)

from datahub_integrations.identity.identity_provider import (
    Group,
    IdentityProvider,
    User,
)


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def identity_provider(mock_graph: MagicMock) -> IdentityProvider:
    return IdentityProvider(mock_graph)


def test_batch_get_actors_success(
    identity_provider: IdentityProvider, mock_graph: MagicMock
) -> None:
    mock_graph.get_entity_semityped.side_effect = [
        AspectBag(
            {
                "corpUserInfo": CorpUserInfoClass(
                    displayName="John Doe",
                    email="john@example.com",
                    firstName="John",
                    lastName="Joyce",
                    active=True,
                ),
                "corpUserEditableInfo": CorpUserEditableInfoClass(
                    title="Software Engineer",
                    phone="412-760-1315",
                    slack="#test-overridden-handle",
                ),
                "corpUserSettings": CorpUserSettingsClass(
                    appearance=CorpUserAppearanceSettingsClass(),
                    notificationSettings=NotificationSettingsClass(
                        sinkTypes=[],
                        slackSettings=SlackNotificationSettingsClass(
                            userHandle="#test-handle"
                        ),
                    ),
                ),
                "corpGroupInfo": CorpGroupInfoClass(
                    displayName="Engineering Team",
                    slack="#eng-team-overridden",
                    email="eng-overriden@example.com",
                    admins=[],
                    members=[],
                    groups=[],
                ),
                "corpGroupEditableInfo": CorpGroupEditableInfoClass(
                    email="eng@example.com", slack="#eng-team"
                ),
            }
        ),
        AspectBag(
            {
                "corpUserInfo": CorpUserInfoClass(
                    displayName="John Doe",
                    email="john@example.com",
                    firstName="John",
                    lastName="Joyce",
                    active=True,
                ),
                "corpUserEditableInfo": CorpUserEditableInfoClass(
                    title="Software Engineer",
                    phone="412-760-1315",
                    slack="#test-overridden-handle",
                ),
                "corpUserSettings": CorpUserSettingsClass(
                    appearance=CorpUserAppearanceSettingsClass(),
                    notificationSettings=NotificationSettingsClass(
                        sinkTypes=[],
                        slackSettings=SlackNotificationSettingsClass(
                            userHandle="#test-handle"
                        ),
                    ),
                ),
                "corpGroupInfo": CorpGroupInfoClass(
                    displayName="Engineering Team",
                    slack="#eng-team-overridden",
                    email="eng-overriden@example.com",
                    admins=[],
                    members=[],
                    groups=[],
                ),
                "corpGroupEditableInfo": CorpGroupEditableInfoClass(
                    email="eng@example.com", slack="#eng-team"
                ),
            }
        ),
    ]
    actor_urns: Set[str] = {"urn:li:corpuser:1", "urn:li:corpGroup:2"}
    expected_results: Dict[str, Union[User, Group]] = {
        "urn:li:corpuser:1": User(
            "urn:li:corpuser:1",
            displayName="John Doe",
            firstName="John",
            lastName="Joyce",
            isActive=False,
            phone="412-760-1315",
            email="john@example.com",
            title="Software Engineer",
            slack="#test-handle",
        ),
        "urn:li:corpGroup:2": Group(
            "urn:li:corpGroup:2",
            displayName="Engineering Team",
            slack="#eng-team",
            email="eng@example.com",
        ),
    }
    result: Dict[str, Union[User, Group]] = identity_provider.batch_get_actors(
        actor_urns
    )

    assert result == expected_results


def test_batch_get_user_info_only(
    identity_provider: IdentityProvider, mock_graph: MagicMock
) -> None:
    mock_graph.get_entity_semityped.side_effect = [
        AspectBag(
            {
                "corpUserInfo": CorpUserInfoClass(
                    displayName="John Doe",
                    email="john@example.com",
                    firstName="John",
                    lastName="Joyce",
                    active=True,
                ),
            }
        ),
    ]
    actor_urns: Set[str] = {"urn:li:corpuser:1"}
    expected_results: Dict[str, Union[User, Group]] = {
        "urn:li:corpuser:1": User(
            "urn:li:corpuser:1",
            displayName="John Doe",
            firstName="John",
            lastName="Joyce",
            isActive=False,
            phone=None,
            email="john@example.com",
        ),
    }
    result: Dict[str, Union[User, Group]] = identity_provider.batch_get_actors(
        actor_urns
    )

    assert result == expected_results


def test_batch_get_actors_user_infos(
    identity_provider: IdentityProvider, mock_graph: MagicMock
) -> None:
    mock_graph.get_entity_semityped.side_effect = [
        AspectBag(
            {
                "corpUserInfo": CorpUserInfoClass(
                    displayName="John Doe",
                    email="john@example.com",
                    firstName="John",
                    lastName="Joyce",
                    active=True,
                ),
                "corpUserEditableInfo": CorpUserEditableInfoClass(
                    title="Software Engineer",
                    phone="412-760-1315",
                    slack="#test-handle",
                ),
            }
        )
    ]
    actor_urns: Set[str] = {"urn:li:corpuser:1"}
    expected_results: Dict[str, Union[User, Group]] = {
        "urn:li:corpuser:1": User(
            "urn:li:corpuser:1",
            displayName="John Doe",
            firstName="John",
            lastName="Joyce",
            isActive=False,
            phone="412-760-1315",
            email="john@example.com",
            title="Software Engineer",
            slack="#test-handle",
        ),
    }
    result: Dict[str, Union[User, Group]] = identity_provider.batch_get_actors(
        actor_urns
    )

    assert result == expected_results


def test_batch_get_actors_group_info_only(
    identity_provider: IdentityProvider, mock_graph: MagicMock
) -> None:
    mock_graph.get_entity_semityped.side_effect = [
        AspectBag(
            {
                "corpGroupInfo": CorpGroupInfoClass(
                    displayName="Engineering Team",
                    slack="#eng-team",
                    email="eng@example.com",
                    admins=[],
                    members=[],
                    groups=[],
                ),
            }
        )
    ]
    actor_urns: Set[str] = {"urn:li:corpGroup:2"}
    expected_results: Dict[str, Union[User, Group]] = {
        "urn:li:corpGroup:2": Group(
            "urn:li:corpGroup:2",
            displayName="Engineering Team",
            slack="#eng-team",
            email="eng@example.com",
        )
    }
    result: Dict[str, Union[User, Group]] = identity_provider.batch_get_actors(
        actor_urns
    )

    assert result == expected_results
