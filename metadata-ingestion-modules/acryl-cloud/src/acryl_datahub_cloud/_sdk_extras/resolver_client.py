from typing import List, Optional

from datahub.metadata.urns import (
    SubscriptionUrn,
)
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.resolver_client import ResolverClient as OSSResolverClient
from datahub.sdk.search_filters import Filter, FilterDsl as F


class ResolverClient(OSSResolverClient):
    def __init__(self, client: DataHubClient):
        super().__init__(client=client)

    def subscription(
        self,
        *,
        entity_urn: Optional[str] = None,
        actor_urn: Optional[str] = None,
    ) -> List[SubscriptionUrn]:
        """Retrieve subscriptions for a given entity or actor, or both if both are given.
        Args:
            entity_urn: Optional URN of the entity to filter subscriptions by.
            actor_urn: Optional URN of the actor to filter subscriptions by.
        Returns:
            List[SubscriptionUrn]: List of SubscriptionUrns matching the filters,
            empty list if none.
        """
        filters: List[Filter] = [F.entity_type(SubscriptionUrn.ENTITY_TYPE)]

        if entity_urn is not None:
            filters.append(F.custom_filter("entityUrn", "EQUAL", [entity_urn]))

        if actor_urn is not None:
            filters.append(F.custom_filter("actorUrn", "EQUAL", [actor_urn]))

        filter = F.and_(*filters)
        subscriptions = list(self._client.search.get_urns(filter=filter))
        return [SubscriptionUrn.from_string(urn) for urn in subscriptions]
