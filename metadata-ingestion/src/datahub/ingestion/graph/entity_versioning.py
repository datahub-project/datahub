import uuid
from typing import Dict, Optional, Protocol, Type

from datahub.emitter.mce_builder import Aspect
from datahub.metadata.schema_classes import (
    VersionPropertiesClass,
    VersionSetPropertiesClass,
)
from datahub.metadata.urns import VersionSetUrn
from datahub.utilities.urns.urn import guess_entity_type


class DataHubGraphProtocol(Protocol):
    def execute_graphql(
        self,
        query: str,
        variables: Optional[Dict],
        operation_name: Optional[str] = None,
        format_exception: bool = True,
    ) -> Dict: ...

    def get_aspect(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        version: int = 0,
    ) -> Optional[Aspect]: ...


class EntityVersioningAPI(DataHubGraphProtocol):
    LINK_VERSION_MUTATION = """
        mutation($input: LinkVersionInput!) {
            linkAssetVersion(input: $input) {
                urn
            }
        }
    """

    UNLINK_VERSION_MUTATION = """
        mutation($input: UnlinkVersionInput!) {
            unlinkAssetVersion(input: $input) {
                urn
            }
        }
    """

    def link_asset_to_version_set(
        self,
        asset_urn: str,
        version_set_urn: Optional[str],
        label: str,
        *,
        comment: Optional[str] = None,
    ) -> Optional[str]:
        """Sets an entity as the latest version of a version set.
        Can also be used to create a new version set, with `asset_urn` as the first version.

        Args:
            asset_urn: URN of the entity.
            version_set_urn: URN of the version set, or None to generate a new version set urn
            label: Label of the version.
            comment: Comment about the version.

        Returns:
            URN of the version set to which `asset_urn` was linked,
            or None if the `asset_urn` was already linked to `version_set_urn`.
        """

        entity_type = guess_entity_type(asset_urn)
        if version_set_urn is None:
            version_set_urn = VersionSetUrn(str(uuid.uuid4()), entity_type).urn()
        elif guess_entity_type(version_set_urn) != "versionSet":
            raise ValueError(f"Expected version set URN, got {version_set_urn}")

        entity_version = self.get_aspect(asset_urn, VersionPropertiesClass)
        if entity_version and entity_version.versionSet:
            if entity_version.versionSet == version_set_urn:
                return None
            else:
                raise ValueError(
                    f"Asset {asset_urn} is already a version of {entity_version.versionSet}"
                )

        variables = {
            "input": {
                "versionSet": version_set_urn,
                "linkedEntity": asset_urn,
                "version": label,
                "comment": comment,
            }
        }
        response = self.execute_graphql(self.LINK_VERSION_MUTATION, variables)
        try:
            return response["linkAssetVersion"]["urn"]
        except KeyError:
            raise ValueError(f"Unexpected response: {response}") from None

    def link_asset_to_versioned_asset(
        self,
        new_asset_urn: str,
        old_asset_urn: str,
        label: str,
        *,
        comment: Optional[str] = None,
    ) -> Optional[str]:
        """Sets an entity as the latest version of an existing versioned entity.

        Args:
            new_asset_urn: URN of the new latest entity.
            old_asset_urn: URN of an existing versioned entity to link onto.
            label: Label of the version.
            comment: Comment about the version.

        Returns:
            URN of the version set to which `new_asset_urn` was linked,
            or None if the `new_asset_urn` was already linked to `old_asset_urn`.
        """

        new_entity_type = guess_entity_type(new_asset_urn)
        old_entity_type = guess_entity_type(old_asset_urn)
        if new_entity_type != old_entity_type:
            raise ValueError(
                f"Expected URNs of the same type, got {new_entity_type} and {old_entity_type}"
            )

        old_entity_version = self.get_aspect(old_asset_urn, VersionPropertiesClass)
        if not old_entity_version:
            raise ValueError(f"Asset {old_asset_urn} is not versioned")

        new_entity_version = self.get_aspect(new_asset_urn, VersionPropertiesClass)
        if new_entity_version:
            if new_entity_version.versionSet == old_entity_version.versionSet:
                return None
            else:
                raise ValueError(
                    f"Asset {new_asset_urn} is already a version of {new_entity_version.versionSet}"
                )

        return self.link_asset_to_version_set(
            new_asset_urn, old_entity_version.versionSet, label, comment=comment
        )

    def unlink_asset_from_version_set(self, asset_urn: str) -> Optional[str]:
        """Unlinks an entity from its version set.

        Args:
            asset_urn: URN of the entity to unlink from its version set.

        Returns:
            If successful, the URN of the version set from which `asset_urn` was unlinked,
            or None if `asset_urn` was not linked to any version set.
        """

        entity_version = self.get_aspect(asset_urn, VersionPropertiesClass)
        if not entity_version:
            return None

        variables = {
            "input": {
                "versionSet": entity_version.versionSet,
                "unlinkedEntity": asset_urn,
            }
        }
        response = self.execute_graphql(self.UNLINK_VERSION_MUTATION, variables)
        try:
            return response["unlinkAssetVersion"]["urn"]
        except KeyError:
            raise ValueError(f"Unexpected response: {response}") from None

    def unlink_latest_asset_from_version_set(
        self, version_set_urn: str
    ) -> Optional[str]:
        """Unlinks the latest version of a version set.

        Args:
            version_set_urn: URN of the version set.

        Returns:
            If successful, the URN of the entity that was unlinked from `version_set_urn`,
            or None if no entity was unlinked.
        """

        version_set_properties = self.get_aspect(
            version_set_urn, VersionSetPropertiesClass
        )
        if not version_set_properties:
            raise ValueError(
                f"Version set {version_set_urn} does not exist or has no versions"
            )

        variables = {
            "input": {
                "versionSet": version_set_urn,
                "unlinkedEntity": version_set_properties.latest,
            }
        }
        response = self.execute_graphql(self.UNLINK_VERSION_MUTATION, variables)
        try:
            return response["unlinkAssetVersion"]["urn"]
        except KeyError:
            raise ValueError(f"Unexpected response: {response}") from None
