from typing import (
    Type,
    Union,
)

from typing_extensions import (
    Self,
    assert_never,
)

from datahub.emitter.mcp_builder import DatahubKey
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    SubscriptionUrn,
    Urn,
)
from datahub.sdk.entity import Entity


class SubscriptionKey(DatahubKey):
    """
    Key class for generating stable subscription identifiers.

    The main goal is to have stable IDs that are deterministic based on the
    entity and actor URNs, which helps prevent duplicate subscriptions during
    eventual consistency scenarios when multiple subscription requests happen
    in quick succession.

    This implementation matches the behavior expected in the backend when a
    subscription is created, ensuring consistent ID generation between the
    Python SDK and Java backend services.
    """

    entity_urn: str
    actor_urn: str


class Subscription(Entity):
    """
    Subscription entity class.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[SubscriptionUrn]:
        """Get the URN type for subscriptions.

        Returns:
            The SubscriptionUrn class.
        """
        return SubscriptionUrn

    def __init__(
        self,
        # SubscriptionInfo
        info: models.SubscriptionInfoClass,
        # Identity
        id: Union[str, SubscriptionUrn],
    ):
        """
        Initialize the Subscription entity.
        """
        super().__init__(urn=Subscription._ensure_id(id))

        self._set_info(info)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, SubscriptionUrn)
        assert "subscriptionInfo" in current_aspects, "SubscriptionInfo is required"
        subscription_info = current_aspects["subscriptionInfo"]
        entity = cls(id=urn, info=subscription_info)
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> SubscriptionUrn:
        assert isinstance(self._urn, SubscriptionUrn)
        return self._urn

    @classmethod
    def _ensure_id(cls, id: Union[str, SubscriptionUrn]) -> SubscriptionUrn:
        if isinstance(id, str):
            return SubscriptionUrn.from_string(id)
        elif isinstance(id, SubscriptionUrn):
            return id
        else:
            assert_never(id)

    def _set_info(self, info: models.SubscriptionInfoClass) -> None:
        self._set_aspect(info)

    @property
    def info(self) -> models.SubscriptionInfoClass:
        """
        Get the SubscriptionInfo aspect of the subscription.
        """
        subscription_info = self._get_aspect(models.SubscriptionInfoClass)
        assert subscription_info is not None
        return subscription_info
