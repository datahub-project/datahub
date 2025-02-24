from __future__ import annotations

from typing import TYPE_CHECKING, Optional, overload

from datahub.errors import ItemNotFoundError, MultipleItemsFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import (
    CorpUserUrn,
    DomainUrn,
    GlossaryTermUrn,
)

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class ResolverClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    # TODO: add caching to this method

    @property
    def _graph(self) -> DataHubGraph:
        return self._client._graph

    def domain(self, *, name: str) -> DomainUrn:
        urn_str = self._graph.get_domain_urn_by_name(name)
        if urn_str is None:
            raise ItemNotFoundError(f"Domain with name {name} not found")
        return DomainUrn.from_string(urn_str)

    @overload
    def user(self, *, name: str) -> CorpUserUrn: ...
    @overload
    def user(self, *, email: str) -> CorpUserUrn: ...
    def user(
        self, *, name: Optional[str] = None, email: Optional[str] = None
    ) -> CorpUserUrn:
        filter_explanation: str
        filters = []
        if name is not None:
            if email is not None:
                raise SdkUsageError("Cannot specify both name and email for auto_user")
            # TODO: do we filter on displayName or fullName?
            filter_explanation = f"with name {name}"
            filters.append(
                {
                    "field": "fullName",
                    "values": [name],
                    "condition": "EQUAL",
                }
            )
        elif email is not None:
            filter_explanation = f"with email {email}"
            filters.append(
                {
                    "field": "email",
                    "values": [email],
                    "condition": "EQUAL",
                }
            )
        else:
            raise SdkUsageError("Must specify either name or email for auto_user")

        users = list(
            self._graph.get_urns_by_filter(
                entity_types=[CorpUserUrn.ENTITY_TYPE],
                extraFilters=filters,
            )
        )
        if len(users) == 0:
            # TODO: In auto methods, should we just create the user/domain/etc if it doesn't exist?
            raise ItemNotFoundError(f"User {filter_explanation} not found")
        elif len(users) > 1:
            raise MultipleItemsFoundError(
                f"Multiple users found {filter_explanation}: {users}"
            )
        else:
            return CorpUserUrn.from_string(users[0])

    def term(self, *, name: str) -> GlossaryTermUrn:
        # TODO: Add some limits on the graph fetch
        terms = list(
            self._graph.get_urns_by_filter(
                entity_types=[GlossaryTermUrn.ENTITY_TYPE],
                extraFilters=[
                    {
                        "field": "id",
                        "values": [name],
                        "condition": "EQUAL",
                    }
                ],
            )
        )
        if len(terms) == 0:
            raise ItemNotFoundError(f"Term with name {name} not found")
        elif len(terms) > 1:
            raise SdkUsageError(f"Multiple terms found with name {name}: {terms}")
        else:
            return GlossaryTermUrn.from_string(terms[0])
