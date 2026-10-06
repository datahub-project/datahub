"""Tests for merging incoming ownership with ownership stored on the server."""

from unittest import mock

from datahub.ingestion.transformer.add_ownership import AddOwnership
from datahub.metadata.schema_classes import OwnerClass, OwnershipClass

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example.dataset,PROD)"

OWNER_URN = "urn:li:corpuser:alice"

TECHNICAL_OWNER_TYPE_URN = "urn:li:ownershipType:__system__technical_owner"

BUSINESS_OWNER_TYPE_URN = "urn:li:ownershipType:__system__business_owner"


def test_merge_deduplicates_same_owner_and_type_urn() -> None:
    """
    The same owner and typeUrn must not be duplicated merely because the
    legacy `type` field differs.

    This covers the case where an owner added through the UI has type NONE,
    while an ingestion transformer emits the same owner with type CUSTOM.
    """
    graph = mock.MagicMock()

    graph.get_ownership.return_value = OwnershipClass(
        owners=[
            OwnerClass(
                owner=OWNER_URN,
                type="NONE",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            )
        ]
    )

    incoming_ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=OWNER_URN,
                type="CUSTOM",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            )
        ]
    )

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=incoming_ownership,
    )

    assert result is not None
    assert len(result.owners) == 1

    # Incoming ownership is inserted after server ownership and therefore wins.
    assert result.owners[0].owner == OWNER_URN
    assert result.owners[0].type == "CUSTOM"
    assert result.owners[0].typeUrn == TECHNICAL_OWNER_TYPE_URN

    graph.get_ownership.assert_called_once_with(
        entity_urn=DATASET_URN,
    )


def test_merge_keeps_same_owner_with_different_type_urns() -> None:
    """
    One person may have multiple ownership roles.

    Owners with the same owner URN but different typeUrn values must remain
    separate.
    """
    graph = mock.MagicMock()

    graph.get_ownership.return_value = OwnershipClass(
        owners=[
            OwnerClass(
                owner=OWNER_URN,
                type="CUSTOM",
                typeUrn=BUSINESS_OWNER_TYPE_URN,
            )
        ]
    )

    incoming_ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=OWNER_URN,
                type="CUSTOM",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            )
        ]
    )

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=incoming_ownership,
    )

    assert result is not None
    assert len(result.owners) == 2

    assert {(owner.owner, owner.typeUrn) for owner in result.owners} == {
        (OWNER_URN, BUSINESS_OWNER_TYPE_URN),
        (OWNER_URN, TECHNICAL_OWNER_TYPE_URN),
    }

    graph.get_ownership.assert_called_once_with(
        entity_urn=DATASET_URN,
    )


def test_merge_preserves_different_owners_with_same_type_urn() -> None:
    """Different people with the same ownership type must remain separate."""
    second_owner_urn = "urn:li:corpuser:bob"

    graph = mock.MagicMock()

    graph.get_ownership.return_value = OwnershipClass(
        owners=[
            OwnerClass(
                owner=OWNER_URN,
                type="CUSTOM",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            )
        ]
    )

    incoming_ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=second_owner_urn,
                type="CUSTOM",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            )
        ]
    )

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=incoming_ownership,
    )

    assert result is not None
    assert len(result.owners) == 2

    assert {(owner.owner, owner.typeUrn) for owner in result.owners} == {
        (OWNER_URN, TECHNICAL_OWNER_TYPE_URN),
        (second_owner_urn, TECHNICAL_OWNER_TYPE_URN),
    }


def test_merge_returns_incoming_ownership_when_server_has_no_ownership() -> None:
    """Incoming ownership must be returned unchanged when the server has none."""
    graph = mock.MagicMock()
    graph.get_ownership.return_value = None

    incoming_owner = OwnerClass(
        owner=OWNER_URN,
        type="CUSTOM",
        typeUrn=TECHNICAL_OWNER_TYPE_URN,
    )
    incoming_ownership = OwnershipClass(
        owners=[incoming_owner],
    )

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=incoming_ownership,
    )

    assert result is incoming_ownership
    assert result.owners == [incoming_owner]

    graph.get_ownership.assert_called_once_with(
        entity_urn=DATASET_URN,
    )


def test_merge_returns_none_when_incoming_ownership_is_none() -> None:
    """The server must not be queried when no incoming ownership exists."""
    graph = mock.MagicMock()

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=None,
    )

    assert result is None
    graph.get_ownership.assert_not_called()


def test_merge_returns_none_when_incoming_owners_are_empty() -> None:
    """The server must not be queried when the incoming owners list is empty."""
    graph = mock.MagicMock()

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=OwnershipClass(owners=[]),
    )

    assert result is None
    graph.get_ownership.assert_not_called()


def test_merge_deduplicates_multiple_incoming_entries() -> None:
    """
    Duplicate entries within the incoming ownership aspect must also collapse
    to one owner when owner and typeUrn are identical.
    """
    graph = mock.MagicMock()

    graph.get_ownership.return_value = OwnershipClass(owners=[])

    incoming_ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=OWNER_URN,
                type="NONE",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            ),
            OwnerClass(
                owner=OWNER_URN,
                type="CUSTOM",
                typeUrn=TECHNICAL_OWNER_TYPE_URN,
            ),
        ]
    )

    result = AddOwnership._merge_with_server_ownership(
        graph=graph,
        urn=DATASET_URN,
        mce_ownership=incoming_ownership,
    )

    assert result is not None
    assert len(result.owners) == 1
    assert result.owners[0].type == "CUSTOM"
