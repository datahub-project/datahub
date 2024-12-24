from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, Type, Union

import pytest
from typing_extensions import TypeAlias, assert_never

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import first_non_null
from datahub.emitter.mce_builder import DEFAULT_ENV, make_ts_millis, parse_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.metadata.urns import DatasetUrn, Urn
from datahub.sdk._shared import (
    Entity,
    HasOwnership,
    HasSubtype,
    HasUrn,
    OwnersInputType,
    UrnOrStr,
)
from datahub.sdk.errors import SdkUsageError
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


class DatasetEditMode(Enum):
    UI = "UI"
    INGESTION = "INGESTION"


_DEFAULT_EDIT_MODE = DatasetEditMode.UI
# TODO: Add default edit attribution for basic props e.g. tags/terms/owners/etc?

SchemaFieldInputType: TypeAlias = Union[
    str,
    Tuple[str, str],  # (name, type)
    Tuple[str, str, str],  # (name, type, description)
    models.SchemaFieldClass,
]
SchemaFieldsInputType: TypeAlias = (
    List[SchemaFieldInputType] | models.SchemaMetadataClass
)


class Dataset(HasSubtype, HasOwnership, Entity):
    __slots__ = ("_edit_mode",)

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
        edit_mode: Optional[DatasetEditMode] = None,
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
        schema: Optional[SchemaFieldsInputType] = None,
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

        if edit_mode is None:
            edit_mode = _DEFAULT_EDIT_MODE
        self._edit_mode = edit_mode

        if schema is not None:
            self.set_schema(schema)

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
        return self._setdefault_aspect(models.DatasetPropertiesClass())

    def _ensure_editable_props(self) -> models.EditableDatasetPropertiesClass:
        # Note that most of the fields in this aspect are not used.
        # The only one that's relevant for us is the description.
        return self._setdefault_aspect(models.EditableDatasetPropertiesClass())

    @property
    def description(self) -> Optional[str]:
        return first_non_null(
            [
                self._ensure_editable_props().description,
                self._ensure_dataset_props().description,
            ]
        )

    def set_description(self, description: str) -> None:
        if self._edit_mode == DatasetEditMode.INGESTION:
            editable_props = self._get_aspect(models.EditableDatasetPropertiesClass)
            if editable_props is not None and editable_props.description is not None:
                # TODO does this make sense?
                raise SdkUsageError(
                    "In ingestion mode, setting the description will be hidden by UI-based edits."
                )

            self._ensure_dataset_props().description = description
        else:
            self._ensure_editable_props().description = description

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

    @property
    def schema(self) -> Dict[str, models.SchemaFieldClass]:
        schema_metadata = self._get_aspect(models.SchemaMetadataClass)
        if schema_metadata is None:
            # TODO throw instead?
            return {}
        return {field.fieldPath: field for field in schema_metadata.fields}

    def _parse_schema_field_input(
        self, schema_field_input: SchemaFieldInputType
    ) -> models.SchemaFieldClass:
        if isinstance(schema_field_input, models.SchemaFieldClass):
            return schema_field_input
        elif isinstance(schema_field_input, tuple):
            # Support (name, type) and (name, type, description) forms
            if len(schema_field_input) == 2:
                name, field_type = schema_field_input
                description = None
            elif len(schema_field_input) == 3:
                name, field_type, description = schema_field_input
            else:
                assert_never(schema_field_input)
            return models.SchemaFieldClass(
                fieldPath=name,
                type=models.SchemaFieldDataTypeClass(
                    resolve_sql_type(
                        field_type,
                        platform=self.urn.get_data_platform_urn().platform_name,
                    )
                    or models.NullTypeClass()
                ),
                nativeDataType=field_type,
                description=description,
            )
        elif isinstance(schema_field_input, str):
            # TODO: Not sure this branch makes sense - we should probably just require types?
            return models.SchemaFieldClass(
                fieldPath=schema_field_input,
                type=models.SchemaFieldDataTypeClass(models.NullTypeClass()),
                nativeDataType="unknown",
                description=None,
            )
        else:
            assert_never(schema_field_input)

    def set_schema(self, schema: SchemaFieldsInputType) -> None:
        if isinstance(schema, models.SchemaMetadataClass):
            self._set_aspect(schema)
        else:
            parsed_schema = [self._parse_schema_field_input(field) for field in schema]
            self._set_aspect(
                models.SchemaMetadataClass(
                    fields=parsed_schema,
                    # The rest of these fields are not used, and so we can set them to dummy/default values.
                    schemaName="",
                    platform=self.urn.platform,
                    version=0,
                    hash="",
                    platformSchema=models.SchemalessClass(),
                )
            )


def graph_get_dataset(self: DataHubGraph, urn: UrnOrStr) -> Dataset:
    if not isinstance(urn, Urn):
        urn = Urn.from_string(urn)

    assert isinstance(urn, DatasetUrn)

    aspects = self.get_entity_semityped(str(urn))

    # TODO get the right entity type subclass
    # TODO: save the timestamp so we can use If-Unmodified-Since on the updates
    return Dataset._new_from_graph(urn, aspects)


def graph_create(self: DataHubGraph, dataset: Dataset) -> None:
    mcps = []

    # By putting this first, we can ensure that the request fails if the entity already exists?
    # TODO: actually validate that this works.
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=str(dataset.urn),
            aspect=dataset.urn.to_key_aspect(),
            changeType=models.ChangeTypeClass.CREATE_ENTITY,
        )
    )

    for aspect in dataset._aspects.values():
        assert isinstance(aspect, models._Aspect)
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(dataset.urn),
                aspect=aspect,
                changeType=models.ChangeTypeClass.CREATE,
            )
        )

    self.emit_mcps(mcps)


if __name__ == "__main__":
    d = Dataset(
        platform="bigquery",
        name="test",
        subtype="Table",
        schema=[("field1", "string"), ("field2", "int64")],
    )
    assert isinstance(d, HasUrn)
    print(d.urn)

    assert d.subtype == "Table"

    print(d.schema)

    with pytest.raises(AttributeError):
        # TODO: make this throw a nicer error
        d.owners = []  # type: ignore

    d.set_owners(["my_user", "other_user", ("third_user", "BUSINESS_OWNER")])
    print(d.owners)

    with pytest.raises(AttributeError):
        # ensure that all the slots are set, and so no extra attributes are allowed
        d.extra = {}  # type: ignore
