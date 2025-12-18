"""
Ownership Builder for Snowplow connector.

Handles construction of ownership metadata from deployment information.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataStructureDeployment,
    User,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

logger = logging.getLogger(__name__)


class OwnershipBuilder:
    """
    Builder for constructing ownership metadata from Snowplow deployments.

    Extracts creator and modifier information from deployment history
    and builds DataHub ownership aspects.
    """

    def __init__(
        self,
        config: SnowplowSourceConfig,
        user_cache: Dict[str, User],
        user_name_cache: Dict[str, List[User]],
    ):
        """
        Initialize ownership builder.

        Args:
            config: Source configuration for BDP connection
            user_cache: Cache of users by ID
            user_name_cache: Cache of users by name
        """
        self.config = config
        self.user_cache = user_cache
        self.user_name_cache = user_name_cache

    def extract_ownership_from_deployments(
        self, deployments: List[DataStructureDeployment]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract createdBy and modifiedBy from deployments array.

        The oldest deployment represents the creator (DATAOWNER).
        The newest deployment represents the last modifier (PRODUCER).

        Args:
            deployments: List of deployments for a data structure

        Returns:
            Tuple of (createdBy_email, modifiedBy_email)
        """
        if not deployments:
            return None, None

        # Sort by timestamp (oldest first)
        sorted_deployments = sorted(
            deployments, key=lambda d: d.ts or "", reverse=False
        )

        # Oldest deployment = creator
        oldest = sorted_deployments[0]
        created_by = self._resolve_user_email(oldest.initiator_id, oldest.initiator)

        # Newest deployment = modifier
        newest = sorted_deployments[-1]
        modified_by = self._resolve_user_email(newest.initiator_id, newest.initiator)

        return created_by, modified_by

    def build_ownership_list(
        self,
        created_by: Optional[str],
        modified_by: Optional[str],
        schema_identifier: str,
    ) -> Optional[List[OwnerClass]]:
        """
        Build ownership list from creator and modifier emails.

        Args:
            created_by: Email of creator (from oldest deployment)
            modified_by: Email of last modifier (from newest deployment)
            schema_identifier: Schema identifier for logging

        Returns:
            List of OwnerClass objects or None if no owners found
        """
        if not created_by and not modified_by:
            return None

        owners = []

        # Primary owner (creator) - DATAOWNER
        if created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(created_by),
                    type=OwnershipTypeClass.DATAOWNER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        # Producer (last modifier) - PRODUCER (only if different from creator)
        if modified_by and modified_by != created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(modified_by),
                    type=OwnershipTypeClass.PRODUCER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        return owners if owners else None

    def emit_ownership(
        self,
        dataset_urn: str,
        created_by: Optional[str],
        modified_by: Optional[str],
        schema_identifier: str,
    ) -> Optional[MetadataWorkUnit]:
        """
        Emit ownership aspect for a dataset.

        Args:
            dataset_urn: URN of the dataset
            created_by: Email or name of creator
            modified_by: Email or name of last modifier
            schema_identifier: Schema identifier for logging

        Returns:
            MetadataWorkUnit: Ownership metadata work unit, or None if no ownership data
        """
        if not created_by and not modified_by:
            return None

        owners = []

        # Primary owner (creator) - DATAOWNER
        if created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(created_by),
                    type=OwnershipTypeClass.DATAOWNER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        # Producer (last modifier) - PRODUCER (only if different from creator)
        if modified_by and modified_by != created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(modified_by),
                    type=OwnershipTypeClass.PRODUCER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        if not owners:
            return None

        ownership = OwnershipClass(
            owners=owners,
            lastModified=AuditStampClass(
                time=int(time.time() * 1000), actor=make_user_urn("datahub")
            ),
        )

        logger.debug(
            f"Emitting ownership for {schema_identifier}: "
            f"created_by={created_by}, modified_by={modified_by}"
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=ownership
        ).as_workunit()

    def _resolve_user_email(
        self, initiator_id: Optional[str], initiator_name: Optional[str]
    ) -> Optional[str]:
        """
        Resolve initiator to email address or identifier.

        Priority:
        1. Use initiatorId → lookup in user cache → return email (RELIABLE)
        2. If initiatorId missing: Try to match by name (UNRELIABLE - multiple matches possible)
        3. Return initiator_name directly if no resolution possible

        Args:
            initiator_id: User ID from deployment (preferred)
            initiator_name: User full name from deployment (fallback)

        Returns:
            Email address, name, or None if unresolvable
        """
        # Best case: Use initiatorId (unique identifier)
        if initiator_id and initiator_id in self.user_cache:
            user = self.user_cache[initiator_id]
            return user.email or user.name or initiator_name

        # Fallback: Try to match by name (PROBLEMATIC - names not unique!)
        if initiator_name and initiator_name in self.user_name_cache:
            matching_users = self.user_name_cache[initiator_name]

            if len(matching_users) == 1:
                # Single match - use it
                user = matching_users[0]
                logger.debug(
                    f"Resolved '{initiator_name}' to {user.email or user.name} by name match"
                )
                return user.email or user.name or initiator_name
            elif len(matching_users) > 1:
                # Multiple matches - ambiguous!
                logger.warning(
                    f"Ambiguous ownership: Found {len(matching_users)} users with name '{initiator_name}'. "
                    f"Using initiator name directly."
                )
                return initiator_name

        # Last resort: return name directly (better than nothing)
        if initiator_name:
            return initiator_name

        return None

    def _get_schema_url_for_ownership(self, schema_identifier: str) -> Optional[str]:
        """Get BDP Console URL for ownership source."""
        if self.config.bdp_connection:
            org_id = self.config.bdp_connection.organization_id
            # Basic URL - actual implementation would need proper schema hash
            return f"https://console.snowplowanalytics.com/organizations/{org_id}/data-structures"
        return None
