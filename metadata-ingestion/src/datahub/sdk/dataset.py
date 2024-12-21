from datetime import datetime
from typing import Dict, Optional, Type

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import DEFAULT_ENV, make_ts_millis, parse_ts_millis
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, Urn
from datahub.sdk._shared import (
    Entity,
    HasOwnership,
    HasSubtype,
    HasUrn,
    OwnersInputType,
    UrnOrStr,
)
from datahub.specific.dataset import DatasetPatchBuilder


class DatasetUpdater:
    # basically a clone of the DatasetPatchBuilder, but with a few more methods

    DatasetPatchBuilder
    pass


def _make_time_stamp(ts: Optional[datetime]) -> Optional[models.TimeStampClass]:
    if ts is None:
        return None
    return models.TimeStampClass(time=make_ts_millis(ts))


def _parse_time_stamp(ts: Optional[models.TimeStampClass]) -> Optional[datetime]:
    if ts is None:
        return None
    return parse_ts_millis(ts.time)


class Dataset(HasSubtype, HasOwnership, Entity):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[Urn]:
        return DatasetUrn

    def __init__(
        self,
        *,
        # Identity.
        platform: str,
        name: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        # TODO have an urn-based variant? probably not, since we need to know the raw platform instance
        # Dataset properties.
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        qualified_name: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        # Standard aspects.
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        # TODO tags
        # TODO terms
        # structured_properties
        # TODO container / browse path generation?
        # Dataset-specific aspects.
        # TODO: schema -> how do we make this feel nice
        # TODO: lineage?
    ):
        urn = DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=name,
            platform_instance=platform_instance,
            env=env,
        )
        super().__init__(urn)

        if platform_instance is not None:
            # TODO: force create dataPlatformInstance aspect
            pass

        self._ensure_dataset_props()
        if description is not None:
            self.set_description(description)
        if display_name is not None:
            self.set_display_name(display_name)
        if qualified_name is not None:
            self.set_qualified_name(qualified_name)
        if external_url is not None:
            self.set_external_url(external_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)

        if subtype is not None:
            self.set_subtype(subtype)
        if owners is not None:
            self.set_owners(owners)

    @property
    def urn(self) -> DatasetUrn:
        return self._urn  # type: ignore

    @property
    def platform_instance(self) -> Optional[str]:
        dataPlatformInstance = self._get_aspect(models.DataPlatformInstanceClass)
        if dataPlatformInstance and dataPlatformInstance.instance:
            return dataPlatformInstance.instance
        return None

    def _ensure_dataset_props(self) -> models.DatasetPropertiesClass:
        dataset_props = self._get_aspect(models.DatasetPropertiesClass)
        if dataset_props is None:
            dataset_props = models.DatasetPropertiesClass()
            self._set_aspect(dataset_props)
        return dataset_props

    @property
    def description(self) -> Optional[str]:
        return self._ensure_dataset_props().description

    def set_description(self, description: str) -> None:
        self._ensure_dataset_props().description = description

    @property
    def display_name(self) -> Optional[str]:
        return self._ensure_dataset_props().name

    def set_display_name(self, display_name: str) -> None:
        self._ensure_dataset_props().name = display_name

    @property
    def qualified_name(self) -> Optional[str]:
        return self._ensure_dataset_props().qualifiedName

    def set_qualified_name(self, qualified_name: str) -> None:
        self._ensure_dataset_props().qualifiedName = qualified_name

    @property
    def external_url(self) -> Optional[str]:
        return self._ensure_dataset_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        self._ensure_dataset_props().externalUrl = external_url

    @property
    def custom_properties(self) -> Optional[Dict[str, str]]:
        return self._ensure_dataset_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        self._ensure_dataset_props().customProperties = custom_properties

    @property
    def created(self) -> Optional[datetime]:
        return _parse_time_stamp(self._ensure_dataset_props().created)

    def set_created(self, created: datetime) -> None:
        self._ensure_dataset_props().created = _make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        return _parse_time_stamp(self._ensure_dataset_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_dataset_props().lastModified = _make_time_stamp(last_modified)


def graph_get_dataset(graph: DataHubGraph, urn: UrnOrStr) -> Dataset:
    if not isinstance(urn, Urn):
        urn = Urn.from_string(urn)

    assert isinstance(urn, DatasetUrn)

    aspects = graph.get_entity_semityped(str(urn))

    # TODO get the right entity type subclass
    return Dataset._new_from_graph(urn, aspects)


if __name__ == "__main__":
    d = Dataset(
        platform="bigquery",
        name="test",
        subtype="Table",
    )
    assert isinstance(d, HasUrn)
    print(d.urn)

    assert d.subtype == "Table"

    with pytest.raises(AttributeError):
        # TODO: make this throw a nicer error
        d.owners = []  # type: ignore

    d.set_owners(["my_user", "other_user", ("third_user", "BUSINESS_OWNER")])
    print(d.owners)

    with pytest.raises(AttributeError):
        # ensure that all the slots are set, and so no extra attributes are allowed
        d.extra = {}  # type: ignore
