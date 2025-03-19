from __future__ import annotations

from typing import TYPE_CHECKING, Optional, overload

from datahub.errors import ItemNotFoundError, MultipleItemsFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import (
    CorpUserUrn,
    DomainUrn,
    GlossaryTermUrn,
)
from datahub.sdk.search_filters import Filter, FilterDsl as F

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
        filter: Filter
        if name is not None:
            if email is not None:
                raise SdkUsageError("Cannot specify both name and email for auto_user")
            # We're filtering on both fullName and displayName. It's not clear
            # what the right behavior is here.
            filter_explanation = f"with name {name}"
            filter = F.or_(
                F.custom_filter("fullName", "EQUAL", [name]),
                F.custom_filter("displayName", "EQUAL", [name]),
            )
        elif email is not None:
            filter_explanation = f"with email {email}"
            filter = F.custom_filter("email", "EQUAL", [email])
        else:
            raise SdkUsageError("Must specify either name or email for auto_user")

        filter = F.and_(
            F.entity_type(CorpUserUrn.ENTITY_TYPE),
            filter,
        )
        users = list(self._client.search.get_urns(filter=filter))
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
            self._client.search.get_urns(
                filter=F.and_(
                    F.entity_type(GlossaryTermUrn.ENTITY_TYPE),
                    F.custom_filter("name", "EQUAL", [name]),
                ),
            )
        )
        if len(terms) == 0:
            raise ItemNotFoundError(f"Term with name {name} not found")
        elif len(terms) > 1:
            raise SdkUsageError(f"Multiple terms found with name {name}: {terms}")
        else:
            return GlossaryTermUrn.from_string(terms[0])
