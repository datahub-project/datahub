import uuid
from typing import (
    Type,
    Union,
)

from typing_extensions import (
    Self,
    assert_never,
)

from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    SubscriptionUrn,
    Urn,
)
from datahub.sdk.entity import Entity


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
        # Identity; it is automatically generated if not provided
        id: Union[str, SubscriptionUrn, None] = None,
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
    def _ensure_id(cls, id: Union[str, SubscriptionUrn, None]) -> SubscriptionUrn:
        if isinstance(id, str):
            return SubscriptionUrn.from_string(id)
        elif isinstance(id, SubscriptionUrn):
            return id
        elif id is None:
            return SubscriptionUrn.from_string(f"urn:li:subscription:{uuid.uuid4()}")
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
