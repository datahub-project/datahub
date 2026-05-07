"""
User resolution service for Snowplow BDP ownership tracking.

Maps deployment initiators (IDs/names) to user email addresses for DataHub ownership.
"""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

import requests

from datahub.ingestion.source.snowplow.models.snowplow_models import User

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
    from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport

logger = logging.getLogger(__name__)


class UserResolver:
    """
    Resolves Snowplow BDP deployment initiators to user email addresses.

    Maintains an in-memory cache of users fetched from BDP API to enable
    efficient lookup during ownership resolution.
    """

    def __init__(
        self,
        bdp_client: Optional["SnowplowBDPClient"],
        report: Optional["SnowplowSourceReport"] = None,
    ) -> None:
        """
        Initialize user resolver with BDP client.

        Args:
            bdp_client: SnowplowBDPClient instance or None
            report: Optional report for tracking warnings
        """
        self.bdp_client = bdp_client
        self.report = report
        self._user_cache: Dict[str, User] = {}
        self._user_name_cache: Dict[str, List[User]] = {}

    def load_users(self) -> None:
        """
        Load all users from BDP API and cache them for ownership resolution.

        This is called once during initialization to enable efficient
        initiator name/ID → email resolution for ownership tracking.
        """
        if not self.bdp_client:
            return

        try:
            users = self.bdp_client.get_users()
            for user in users:
                # Cache by ID (for initiatorId lookups)
                if user.id:
                    self._user_cache[user.id] = user

                # Cache by name (for initiator name lookups)
                if user.name:
                    if user.name not in self._user_name_cache:
                        self._user_name_cache[user.name] = []
                    self._user_name_cache[user.name].append(user)

                # Also cache by display_name
                if user.display_name:
                    if user.display_name not in self._user_name_cache:
                        self._user_name_cache[user.display_name] = []
                    self._user_name_cache[user.display_name].append(user)

            logger.info(
                f"Cached {len(self._user_cache)} users for ownership resolution"
            )
        except (requests.RequestException, ValueError, KeyError) as e:
            # Expected errors: API failures, parsing errors, missing fields
            logger.warning(
                f"Failed to load users for ownership resolution: {e}. "
                f"Ownership tracking will use initiator names directly."
            )
            if self.report:
                self.report.report_warning(
                    "user_loading",
                    f"Could not load users from BDP API: {e}. "
                    "Ownership will use initiator names instead of emails.",
                )
        except Exception as e:
            # Unexpected error - indicates a bug
            logger.error(f"Unexpected error loading users: {e}", exc_info=True)
            if self.report:
                self.report.report_failure(
                    "user_loading",
                    f"Unexpected error loading users: {e}. This may indicate a bug.",
                )

    def resolve_user_email(
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
        if initiator_id and initiator_id in self._user_cache:
            user = self._user_cache[initiator_id]
            return user.email or user.name or initiator_name

        # Fallback: Try to match by name (PROBLEMATIC - names not unique!)
        if initiator_name and initiator_name in self._user_name_cache:
            matching_users = self._user_name_cache[initiator_name]

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

    def get_user_cache(self) -> Dict[str, User]:
        """Get user cache dictionary (for OwnershipBuilder)."""
        return self._user_cache

    def get_user_name_cache(self) -> Dict[str, List[User]]:
        """Get user name cache dictionary (for OwnershipBuilder)."""
        return self._user_name_cache
