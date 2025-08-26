import uuid
from datetime import datetime
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeAlias,
    Union,
)

from typing_extensions import (
    Self,
    assert_never,
)

from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.emitter.enum_helpers import get_enum_options
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, DatasetUrn
from datahub.sdk._shared import (
    HasPlatformInstance,
    HasTags,
    TagsInputType,
)
from datahub.sdk.entity import Entity
from datahub.utilities.urns._urn_base import Urn

AssertionInfoInputType: TypeAlias = Union[
    models.DatasetAssertionInfoClass,
    models.FreshnessAssertionInfoClass,
    models.VolumeAssertionInfoClass,
    models.SqlAssertionInfoClass,
    models.FieldAssertionInfoClass,
    # TODO: models.SchemaAssertionInfoClass,
    # TODO: models.CustomAssertionInfoClass,
]
AssertionSourceInputType: TypeAlias = Union[
    str,  # assertion source type
    models.AssertionSourceTypeClass,
    models.AssertionSourceClass,
]
LastUpdatedInputType: TypeAlias = Union[
    Tuple[int, str],  # (timestamp, actor)
    Tuple[datetime, str],  # (datetime, actor)
    models.AuditStampClass,
]
AssertionActionsInputType: TypeAlias = List[
    Union[
        str,  # assertion action type: RAISE_INCIDENT, RESOLVE_INCIDENT
        models.AssertionActionTypeClass,
        models.AssertionActionClass,
    ]
]


class Assertion(HasPlatformInstance, HasTags, Entity):
    """Represents an assertion in DataHub.

    TODO: This is a placeholder for better documentation.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[AssertionUrn]:
        """Get the URN type for assertions.

        Returns:
            The AssertionUrn class.
        """
        return AssertionUrn

    def __init__(
        self,
        *,
        # Identity; it is automatically generated if not provided
        id: Union[str, AssertionUrn, None] = None,
        # Assertion info
        info: AssertionInfoInputType,
        description: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        source: Optional[AssertionSourceInputType] = None,
        last_updated: Optional[LastUpdatedInputType] = None,
        # For assertions, platform+instance is not coupled to identity
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
        # Standard aspects
        tags: Optional[TagsInputType] = None,
        # Assertion actions
        on_success: Optional[AssertionActionsInputType] = None,
        on_failure: Optional[AssertionActionsInputType] = None,
    ):
        super().__init__(urn=Assertion._ensure_id(id))

        self._set_info(info)
        if description is not None:
            self.set_description(description)
        if external_url is not None:
            self.set_external_url(external_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if source is not None:
            self._set_source(source)
        if last_updated is not None:
            self._set_last_updated(last_updated)

        if platform is not None:
            self._set_platform_instance(platform, platform_instance)

        if tags is not None:
            self.set_tags(tags)

        if on_success is not None or on_failure is not None:
            self._set_actions(on_success or [], on_failure or [])

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, AssertionUrn)
        assert "assertionInfo" in current_aspects, "AssertionInfo is required"
        assertion_info = current_aspects["assertionInfo"]
        info = cls._switch_assertion_info(assertion_info)

        entity = cls(id=urn, info=info)
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> AssertionUrn:
        assert isinstance(self._urn, AssertionUrn)
        return self._urn

    @classmethod
    def _ensure_id(cls, id: Union[str, AssertionUrn, None]) -> AssertionUrn:
        if isinstance(id, str):
            return AssertionUrn.from_string(id)
        elif isinstance(id, AssertionUrn):
            return id
        elif id is None:
            return AssertionUrn.from_string(f"urn:li:assertion:{uuid.uuid4()}")
        else:
            assert_never(id)

    def _ensure_info(self) -> models.AssertionInfoClass:
        # Compared with datasets, we don't have editable non-editable duality here
        return self._setdefault_aspect(
            models.AssertionInfoClass(
                # TBC: just because we need to set one!
                type=models.AssertionTypeClass.DATASET
            )
        )

    def _ensure_actions(self) -> models.AssertionActionsClass:
        return self._setdefault_aspect(
            models.AssertionActionsClass(onSuccess=[], onFailure=[])
        )

    @property
    def description(self) -> Optional[str]:
        """Get the description of the assertion.

        Returns:
            The descripton if set, None otherwise.
        """
        return self._ensure_info().description

    def set_description(self, description: str) -> None:
        """Set the description of the assertion.

        Args:
            description: The description to set.
        """
        self._ensure_info().description = description

    def _set_info(self, assertion: AssertionInfoInputType) -> None:
        info = self._ensure_info()
        if isinstance(assertion, models.DatasetAssertionInfoClass):
            info.datasetAssertion = assertion
            info.type = models.AssertionTypeClass.DATASET
        elif isinstance(assertion, models.FreshnessAssertionInfoClass):
            info.freshnessAssertion = assertion
            info.type = models.AssertionTypeClass.FRESHNESS
        elif isinstance(assertion, models.VolumeAssertionInfoClass):
            info.volumeAssertion = assertion
            info.type = models.AssertionTypeClass.VOLUME
        elif isinstance(assertion, models.SqlAssertionInfoClass):
            info.sqlAssertion = assertion
            info.type = models.AssertionTypeClass.SQL
        elif isinstance(assertion, models.FieldAssertionInfoClass):
            info.fieldAssertion = assertion
            info.type = models.AssertionTypeClass.FIELD
        else:
            assert_never(assertion)

    @property
    def info(self) -> AssertionInfoInputType:
        """Get the assertion info.

        Returns:
            The assertion info.
        """
        info = self._ensure_info()
        return Assertion._switch_assertion_info(info)

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the assertion.

        Returns:
            The external URL if set, None otherwise.
        """
        return self._ensure_info().externalUrl

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the assertion.

        Args:
            external_url: The external URL to set.
        """
        self._ensure_info().externalUrl = external_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the assertion.

        Returns:
            Dictionary of custom properties.
        """
        return self._ensure_info().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the assertion.

        Args:
            custom_properties: Dictionary of custom properties to set.
        """
        self._ensure_info().customProperties = custom_properties

    @property
    def source(self) -> Optional[models.AssertionSourceClass]:
        """Get the source of the assertion.

        Returns:
            The source if set, None otherwise.
        """
        return self._ensure_info().source

    def _set_source(self, source: AssertionSourceInputType) -> None:
        """Set the source of the assertion.

        Args:
            source: The source to set.
        """
        info = self._ensure_info()
        if isinstance(source, str):
            # TBC: The model allows arbitrary strings, should we restrict this to AssertionSourceTypeClass enum values?
            info.source = models.AssertionSourceClass(
                type=source,
            )
        elif isinstance(source, models.AssertionSourceTypeClass):
            info.source = models.AssertionSourceClass(
                type=source,
            )
        elif isinstance(source, models.AssertionSourceClass):
            info.source = source
        else:
            assert_never(source)

    @property
    def last_updated(self) -> Optional[models.AuditStampClass]:
        """Get the last updated audit event of the assertion.

        Audit event includes the timestamp and the actor who made the change.

        Returns:
            The last updated audit event of the assertion, or None if not set.
        """
        return self._ensure_info().lastUpdated

    def _set_last_updated(self, last_updated: LastUpdatedInputType) -> None:
        """Set the last updated audit event, including the timestamp and the actor who made the change.

        Args:
            last_updated: A tuple containing the last updated timestamp and actor.
        """
        info = self._ensure_info()

        if isinstance(last_updated, tuple):
            if len(last_updated) != 2:
                raise SdkUsageError("Invalid length for last updated tuple")

            tm, actor = last_updated

            if not self._is_actor_urn(actor):
                raise SdkUsageError(
                    "Invalid actor for last updated tuple, expected 'urn:li:corpuser:*' or 'urn:li:corpGroup:*'"
                )

            if isinstance(tm, datetime):
                info.lastUpdated = models.AuditStampClass(make_ts_millis(tm), actor)
            elif isinstance(tm, int):
                info.lastUpdated = models.AuditStampClass(tm, actor)
            else:
                raise SdkUsageError(
                    "Invalid type for last updated tuple timestamp, expected int or datetime"
                )
        elif isinstance(last_updated, models.AuditStampClass):
            if not self._is_actor_urn(last_updated.actor):
                raise SdkUsageError(
                    "Invalid actor for last updated tuple, expected 'urn:li:corpuser:*' or 'urn:li:corpGroup:*'"
                )
            info.lastUpdated = last_updated
        else:
            assert_never(last_updated)

    # TODO: is this defined somewhere else?
    def _is_actor_urn(self, actor: str) -> bool:
        """Check if the actor is a valid URN.

        Args:
            actor: The actor to check.

        Returns:
            True if the actor is a valid URN, False otherwise.
        """
        return actor.startswith("urn:li:corpuser:") or actor.startswith(
            "urn:li:corpGroup:"
        )

    @property
    def on_success(self) -> List[models.AssertionActionClass]:
        """Get the actions to perform on success.

        Returns:
            The actions to perform on success, it may be empty if not set.
        """
        return self._ensure_actions().onSuccess

    @property
    def on_failure(self) -> List[models.AssertionActionClass]:
        """Get the actions to perform on failure.

        Returns:
            The actions to perform on failure, it may be empty if not set.
        """
        return self._ensure_actions().onFailure

    def _set_actions(
        self,
        on_success: AssertionActionsInputType,
        on_failure: AssertionActionsInputType,
    ) -> None:
        """Set the actions to perform on success or failure.

        Args:
            on_success: The actions to perform on success.
            on_failure: The actions to perform on failure.
        """
        for action in on_success + on_failure:
            if isinstance(action, str) and action not in [
                models.AssertionActionTypeClass.RAISE_INCIDENT,
                models.AssertionActionTypeClass.RESOLVE_INCIDENT,
            ]:
                raise SdkUsageError(
                    f"Invalid action type {str} for on_success or on_failure actions, expected valid str or AssertionActionTypeClass"
                )

        actions = self._ensure_actions()
        actions.onSuccess = [
            action
            if isinstance(action, models.AssertionActionClass)
            else models.AssertionActionClass(type=action)
            for action in on_success
        ]
        actions.onFailure = [
            action
            if isinstance(action, models.AssertionActionClass)
            else models.AssertionActionClass(type=action)
            for action in on_failure
        ]

    @property
    def dataset(self) -> DatasetUrn:
        """Get the dataset URN associated with the assertion.

        Returns:
            The dataset URN.
        """
        info = self._ensure_info()
        assertion_info = Assertion._switch_assertion_info(info)

        dataset_str = (
            assertion_info.dataset
            if isinstance(assertion_info, models.DatasetAssertionInfoClass)
            else assertion_info.entity
        )
        return DatasetUrn.from_string(dataset_str)

    @classmethod
    def _switch_assertion_info(
        cls,
        assertion_info: models.AssertionInfoClass,
    ) -> Union[
        models.DatasetAssertionInfoClass,
        models.FreshnessAssertionInfoClass,
        models.VolumeAssertionInfoClass,
        models.SqlAssertionInfoClass,
        models.FieldAssertionInfoClass,
    ]:
        if assertion_info.type not in get_enum_options(models.AssertionTypeClass):
            raise SDKNotYetSupportedError(f"Assertion type: {assertion_info.type}")

        if assertion_info.type == models.AssertionTypeClass.DATASET:
            assert assertion_info.datasetAssertion is not None
            return assertion_info.datasetAssertion
        elif assertion_info.type == models.AssertionTypeClass.FRESHNESS:
            assert assertion_info.freshnessAssertion is not None
            return assertion_info.freshnessAssertion
        elif assertion_info.type == models.AssertionTypeClass.VOLUME:
            assert assertion_info.volumeAssertion is not None
            return assertion_info.volumeAssertion
        elif assertion_info.type == models.AssertionTypeClass.SQL:
            assert assertion_info.sqlAssertion is not None
            return assertion_info.sqlAssertion
        elif assertion_info.type == models.AssertionTypeClass.FIELD:
            assert assertion_info.fieldAssertion is not None
            return assertion_info.fieldAssertion
        else:
            raise AssertionError("Unreachable code, all cases should be handled above")
