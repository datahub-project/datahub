import uuid
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    VersionPropertiesClass,
    VersionSetPropertiesClass,
)
from datahub.metadata.urns import VersionSetUrn
from datahub.utilities.urns.urn import guess_entity_type


class EntityVersioningAPI(DataHubGraph):
    LINK_VERSION_MUTATION = """
        mutation($input: LinkVersionInput!) {
            linkAssetVersion(input: $input)
        }
    """

    UNLINK_VERSION_MUTATION = """
        mutation($input: UnlinkVersionInput!) {
            unlinkAssetVersion(input: $input)
        }
    """

    def link_asset_to_version_set(
        self,
        asset_urn: str,
        version_set_urn: Optional[str],
        label: str,
        *,
        comment: Optional[str] = None,
    ) -> str:
        """Sets an entity as the latest version of a version set.

        Can also be used to create a new version set, with `asset_urn` as the first version.

        Args:
            asset_urn: URN of the entity.
            version_set_urn: URN of the version set, or None to generate a new version set urn
            label: Label of the version.
            comment: Comment about the version.

        Returns:
            URN of the version set to which `asset_urn` was linked.
        """

        entity_type = guess_entity_type(asset_urn)
        if version_set_urn is None:
            version_set_urn = VersionSetUrn(str(uuid.uuid4()), entity_type).urn()
        elif guess_entity_type(version_set_urn) != "versionSet":
            raise ValueError(f"Expected version set URN, got {version_set_urn}")

        entity_version = self.get_aspect(asset_urn, VersionPropertiesClass)
        if entity_version:
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
        self.execute_graphql(self.LINK_VERSION_MUTATION, variables)
        return version_set_urn

    def link_asset_to_versioned_asset(
        self,
        new_asset_urn: str,
        old_asset_urn: str,
        label: str,
        *,
        comment: Optional[str] = None,
    ) -> str:
        """Sets an entity as the latest version of an existing versioned entity.

        Args:
            new_asset_urn: URN of the new latest entity.
            old_asset_urn: URN of an existing versioned entity to link onto.
            label: Label of the version.
            comment: Comment about the version.

        Returns:
            URN of the version set to which `new_asset_urn` was linked.
        """

        new_entity_type = guess_entity_type(new_asset_urn)
        old_entity_type = guess_entity_type(old_asset_urn)
        if new_entity_type != old_entity_type:
            raise ValueError(
                f"Expected URNs of the same type, got {new_entity_type} and {old_entity_type}"
            )

        new_entity_version = self.get_aspect(new_asset_urn, VersionPropertiesClass)
        if new_entity_version:
            raise ValueError(
                f"Asset {new_asset_urn} is already a version of {new_entity_version.versionSet}"
            )
        old_entity_version = self.get_aspect(old_asset_urn, VersionPropertiesClass)
        if not old_entity_version:
            raise ValueError(f"Asset {old_asset_urn} is not versioned")

        version_set_urn = old_entity_version.versionSet
        self.link_asset_to_version_set(
            new_asset_urn, version_set_urn, label, comment=comment
        )
        return version_set_urn

    def unlink_asset_from_version_set(self, asset_urn: str) -> Optional[str]:
        """Unlinks an entity from its version set.

        Args:
            asset_urn: URN of the entity to unlink from its version set.

        Returns:
            If successful, the URN of the version set from which `asset_urn` was unlinked.
        """

        entity_version = self.get_aspect(asset_urn, VersionPropertiesClass)
        if not entity_version:
            raise ValueError(f"Asset {asset_urn} is not versioned")

        variables = {
            "input": {
                "versionSet": entity_version.versionSet,
                "unlinkedEntity": asset_urn,
            }
        }
        if self.execute_graphql(self.UNLINK_VERSION_MUTATION, variables):
            return entity_version.versionSet
        else:
            return None

    def unlink_latest_asset_from_version_set(
        self, version_set_urn: str
    ) -> Optional[str]:
        """Unlinks the latest version of a version set.

        Args:
            version_set_urn: URN of the version set.

        Returns:
            If successful, the URN of the entity that was unlinked from `version_set_urn`.
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
        if self.execute_graphql(self.UNLINK_VERSION_MUTATION, variables):
            return version_set_properties.latest
        else:
            return None
