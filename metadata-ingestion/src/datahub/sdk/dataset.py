from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, Type, Union

from typing_extensions import Self, TypeAlias, assert_never

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import first_non_null
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.errors import SdkUsageError
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub.sdk._shared import (
    ContainerInputType,
    DatasetUrnOrStr,
    Entity,
    HasContainer,
    HasOwnership,
    HasSubtype,
    HasTags,
    OwnersInputType,
    TagsInputType,
    make_time_stamp,
    parse_time_stamp,
)


class DatasetEditMode(Enum):
    OVERWRITE_UI = "OVERWRITE_UI"
    DEFER_TO_UI = "DEFER_TO_UI"


# TODO: Add a way for ingestion to change the default edit mode
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

UpstreamInputType: TypeAlias = Union[
    # Dataset upstream variants.
    DatasetUrnOrStr,
    models.UpstreamClass,
    # Column upstream variants.
    models.FineGrainedLineageClass,
]
# Mapping of { downstream_column -> [upstream_columns] }
ColumnLineageMapping: TypeAlias = Dict[str, List[str]]
UpstreamLineageInputType: TypeAlias = Union[
    models.UpstreamLineageClass,
    List[UpstreamInputType],
    # Combined variant.
    # Map of { upstream_dataset -> { downstream_column -> [upstream_column] } }
    Dict[DatasetUrnOrStr, ColumnLineageMapping],
]


def _parse_upstream_input(
    upstream_input: UpstreamInputType,
) -> models.UpstreamClass | models.FineGrainedLineageClass:
    if isinstance(upstream_input, models.UpstreamClass):
        return upstream_input
    elif isinstance(upstream_input, models.FineGrainedLineageClass):
        return upstream_input
    elif isinstance(upstream_input, (str, DatasetUrn)):
        return models.UpstreamClass(
            dataset=str(upstream_input),
            type=models.DatasetLineageTypeClass.TRANSFORMED,
        )
    else:
        assert_never(upstream_input)


def _parse_cll_mapping(
    *,
    upstream: DatasetUrnOrStr,
    downstream: DatasetUrnOrStr,
    cll_mapping: ColumnLineageMapping,
) -> List[models.FineGrainedLineageClass]:
    cll = []
    for downstream_column, upstream_columns in cll_mapping.items():
        cll.append(
            models.FineGrainedLineageClass(
                upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    SchemaFieldUrn(upstream, upstream_column).urn()
                    for upstream_column in upstream_columns
                ],
                downstreams=[SchemaFieldUrn(downstream, downstream_column).urn()],
            )
        )
    return cll


def _parse_upstream_lineage_input(
    upstream_input: UpstreamLineageInputType, downstream_urn: DatasetUrn
) -> models.UpstreamLineageClass:
    if isinstance(upstream_input, models.UpstreamLineageClass):
        return upstream_input
    elif isinstance(upstream_input, list):
        upstreams = [_parse_upstream_input(upstream) for upstream in upstream_input]

        # Partition into table and column lineages.
        tll = [
            upstream
            for upstream in upstreams
            if isinstance(upstream, models.UpstreamClass)
        ]
        cll = [
            upstream
            for upstream in upstreams
            if not isinstance(upstream, models.UpstreamClass)
        ]

        # TODO: check that all things in cll are also in tll
        return models.UpstreamLineageClass(upstreams=tll, fineGrainedLineages=cll)
    elif isinstance(upstream_input, dict):
        tll = []
        cll = []
        for dataset_urn, column_lineage in upstream_input.items():
            tll.append(
                models.UpstreamClass(
                    dataset=str(dataset_urn),
                    type=models.DatasetLineageTypeClass.TRANSFORMED,
                )
            )
            cll.extend(
                _parse_cll_mapping(
                    upstream=dataset_urn,
                    downstream=downstream_urn,
                    cll_mapping=column_lineage,
                )
            )

        return models.UpstreamLineageClass(upstreams=tll, fineGrainedLineages=cll)
    else:
        assert_never(upstream_input)


class Dataset(HasSubtype, HasContainer, HasOwnership, HasTags, Entity):
    __slots__ = ("_edit_mode",)

    @classmethod
    def get_urn_type(cls) -> Type[DatasetUrn]:
        return DatasetUrn

    def __init__(
        self,
        *,
        # Identity.
        platform: str,
        name: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        # Settings.
        edit_mode: Optional[DatasetEditMode] = None,
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
        container: Optional[ContainerInputType] = None,
        owners: Optional[OwnersInputType] = None,
        tags: Optional[TagsInputType] = None,
        # TODO: do we need to support edit_mode for tags / other aspects?
        # TODO terms
        # TODO structured_properties
        # Dataset-specific aspects.
        schema: Optional[SchemaFieldsInputType] = None,
        upstreams: Optional[models.UpstreamLineageClass] = None,
    ):
        urn = DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=name,
            platform_instance=platform_instance,
            env=env,
        )
        super().__init__(urn)

        self._set_aspect(
            models.DataPlatformInstanceClass(
                platform=urn.platform,
                instance=platform_instance,
            )
        )

        if edit_mode is None:
            # When created via the constructor, we're likely creating a new dataset.
            # In this case, there's no UI changes yet, so it's fine to defer to the UI.
            edit_mode = DatasetEditMode.DEFER_TO_UI
        self._edit_mode = edit_mode

        if schema is not None:
            self.set_schema(schema)
        if upstreams is not None:
            self.set_upstreams(upstreams)

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
        if container is not None:
            self._set_container(container)
        if owners is not None:
            self.set_owners(owners)
        if tags is not None:
            self.set_tags(tags)
        # TODO: add support for field-level tags, terms, etc

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, DatasetUrn)
        entity = cls(
            platform=urn.platform,
            name=urn.name,
            env=urn.env,
            edit_mode=DatasetEditMode.OVERWRITE_UI,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> DatasetUrn:
        return self._urn  # type: ignore

    @property
    def platform_instance(self) -> Optional[str]:
        # TODO: Move this to a HasPlatformInstance mixin
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
        if self._edit_mode == DatasetEditMode.DEFER_TO_UI:
            editable_props = self._get_aspect(models.EditableDatasetPropertiesClass)
            if editable_props is not None and editable_props.description is not None:
                # TODO does it make sense to throw here?
                raise SdkUsageError(
                    "Setting the description will be hidden by UI-based edits. "
                    "Set the edit mode to OVERWRITE_UI to override this behavior."
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
        return parse_time_stamp(self._ensure_dataset_props().created)

    def set_created(self, created: datetime) -> None:
        self._ensure_dataset_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_dataset_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_dataset_props().lastModified = make_time_stamp(last_modified)

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

    @property
    def upstreams(self) -> Optional[models.UpstreamLineageClass]:
        return self._get_aspect(models.UpstreamLineageClass)

    def set_upstreams(self, upstreams: UpstreamLineageInputType) -> None:
        self._set_aspect(_parse_upstream_lineage_input(upstreams, self.urn))
