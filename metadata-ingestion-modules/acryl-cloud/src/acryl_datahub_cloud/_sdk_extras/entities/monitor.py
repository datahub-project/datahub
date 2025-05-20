from typing import (
    Dict,
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

from datahub.emitter.enum_helpers import get_enum_options
from datahub.errors import SdkUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    DatasetUrn,
    MonitorUrn,
    Urn,
)
from datahub.sdk.entity import Entity

MonitorIdentityInputType: TypeAlias = Union[
    Tuple[DatasetUrn, str],  # (dataset urn, monitor id)
    models.MonitorKeyClass,
    MonitorUrn,
    # TBC: other options that may be considered
    # monitor_key.entity from dataset and monitor_key.id would be auto-generated
    # DatasetUrn,
    # we could fetch assertion info from datahub and then set monitor_key.entity from assertion and monitor_key.id from assertion id
    # this would require to fetch assertion info from datahub
    # AssertionUrn,
]

MonitorInfoInputType: TypeAlias = Union[
    Tuple[  # (monitor type [ASSERTION|FRESHNESS], monitor status mode [ACTIVE|INACTIVE|PASSIVE])
        str, str
    ],
    Tuple[  # (monitor type enum, monitor status mode enum)
        models.MonitorTypeClass, models.MonitorModeClass
    ],
    models.MonitorInfoClass,
]


class Monitor(Entity):
    """
    Monitor entity class.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[MonitorUrn]:
        """Get the URN type for monitors.

        Returns:
            The MonitorUrn class.
        """
        return MonitorUrn

    def __init__(
        self,
        id: MonitorIdentityInputType,
        # MonitorInfo
        info: MonitorInfoInputType,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(urn=Monitor._ensure_id(id=id))

        self._set_info(info)

        if external_url is not None:
            # this may overwrite the value set from info#externalUrl value
            self.set_external_url(external_url)
        if custom_properties is not None:
            # this may overwrite the value set from info#customPropperties value
            self.set_custom_properties(custom_properties)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, MonitorUrn)
        assert "monitorInfo" in current_aspects, "MonitorInfo is required"

        entity = cls(id=urn, info=current_aspects["monitorInfo"])
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> MonitorUrn:
        assert isinstance(self._urn, MonitorUrn)
        return self._urn

    @classmethod
    def _ensure_id(
        cls,
        id: MonitorIdentityInputType,
    ) -> MonitorUrn:
        if isinstance(id, tuple):
            if len(id) != 2:
                raise SdkUsageError(
                    f"Invalid monitor identity input tuple: {id}. Expected a tuple of (dataset/entity urn, monitor id)."
                )
            return MonitorUrn(entity=id[0], id=id[1])
        elif isinstance(id, models.MonitorKeyClass):
            # This validation may look redundant but it is not.
            # While MonitorKey PDL model ensures entity is a DatasetUrn,
            # generated python code for entity's type in MonitorKeyClass is not so strict and allows for any string.
            entity: DatasetUrn
            if isinstance(id.entity, str):
                try:
                    entity = DatasetUrn.from_string(id.entity)
                except Exception as e:
                    raise SdkUsageError(
                        f"Invalid monitor identity input key: {id}. Invalid dataset urn for entity."
                    ) from e
            else:
                if not isinstance(id.entity, DatasetUrn):
                    raise SdkUsageError(
                        f"Invalid monitor identity input key: {id}. Expected a DatasetUrn for entity."
                    )
                entity = id.entity
            return MonitorUrn(entity=entity, id=id.id)
        elif isinstance(id, MonitorUrn):
            return id
        else:
            assert_never(id)

    def _ensure_info(self) -> models.MonitorInfoClass:
        return self._setdefault_aspect(
            models.MonitorInfoClass(
                # TBC: just because we need to set them!
                type=models.MonitorTypeClass.ASSERTION,
                status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
            )
        )

    @property
    def info(self) -> models.MonitorInfoClass:
        """Get the monitor info.

        Returns:
            The monitor info.
        """
        return self._ensure_info()

    def _set_info(self, info: MonitorInfoInputType) -> None:
        """Set the monitor info.

        Args:
            info: The monitor info.
        """
        if isinstance(info, tuple):
            if len(info) != 2:
                raise SdkUsageError(
                    f"Invalid monitor info input tuple: {info}. Expected a tuple of (monitor type, monitor status)."
                )
            type, mode_status = info
            if isinstance(type, str) and type not in get_enum_options(
                models.MonitorTypeClass
            ):
                raise SdkUsageError(
                    f"Invalid monitor type: {type}. Expected {get_enum_options(models.MonitorTypeClass)}."
                )
            if isinstance(mode_status, str) and mode_status not in get_enum_options(
                models.MonitorModeClass
            ):
                raise SdkUsageError(
                    f"Invalid monitor status: {mode_status}. Expected {get_enum_options(models.MonitorModeClass)}."
                )
            self._ensure_info().type = type
            self._ensure_info().status = models.MonitorStatusClass(mode=mode_status)
        elif isinstance(info, models.MonitorInfoClass):
            self._set_aspect(info)
        else:
            assert_never(info)

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the monitor.

        Returns:
            The external URL if set, None otherwise.
        """
        return self._ensure_info().externalUrl

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the monitor.

        Args:
            external_url: The external URL to set.
        """
        self._ensure_info().externalUrl = external_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the monitor.

        Returns:
            Dictionary of custom properties.
        """
        return self._ensure_info().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the monitor.

        Args:
            custom_properties: Dictionary of custom properties to set.
        """
        self._ensure_info().customProperties = custom_properties
