from __future__ import annotations

import warnings
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple, Type, Union

from typing_extensions import Self, TypeAlias, assert_never

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import first_non_null
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.errors import (
    IngestionAttributionWarning,
    ItemNotFoundError,
    SchemaFieldKeyError,
    SdkUsageError,
)
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub.sdk._attribution import is_ingestion_attribution
from datahub.sdk._shared import (
    DatasetUrnOrStr,
    DomainInputType,
    HasContainer,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasPlatformInstance,
    HasSubtype,
    HasTags,
    HasTerms,
    LinksInputType,
    OwnersInputType,
    ParentContainerInputType,
    TagInputType,
    TagsInputType,
    TermInputType,
    TermsInputType,
    make_time_stamp,
    parse_time_stamp,
)
from datahub.sdk._utils import add_list_unique, remove_list_unique
from datahub.sdk.entity import Entity, ExtraAspectsType
from datahub.utilities.sentinels import Unset, unset

SchemaFieldInputType: TypeAlias = Union[
    Tuple[str, str],  # (name, type)
    Tuple[str, str, str],  # (name, type, description)
    models.SchemaFieldClass,
]
SchemaFieldsInputType: TypeAlias = Union[
    Sequence[SchemaFieldInputType],
    models.SchemaMetadataClass,
]

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
) -> Union[models.UpstreamClass, models.FineGrainedLineageClass]:
    if isinstance(
        upstream_input, (models.UpstreamClass, models.FineGrainedLineageClass)
    ):
        return upstream_input
    elif isinstance(upstream_input, (str, DatasetUrn)):
        return models.UpstreamClass(
            dataset=str(upstream_input),
            type=models.DatasetLineageTypeClass.TRANSFORMED,
        )
    else:
        assert_never(upstream_input)


def parse_cll_mapping(
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
                parse_cll_mapping(
                    upstream=dataset_urn,
                    downstream=downstream_urn,
                    cll_mapping=column_lineage,
                )
            )

        return models.UpstreamLineageClass(upstreams=tll, fineGrainedLineages=cll)
    else:
        assert_never(upstream_input)


class SchemaField:
    __slots__ = ("_parent", "_field_path")

    def __init__(self, parent: Dataset, field_path: str):
        self._parent = parent
        self._field_path = field_path

    def _base_schema_field(self) -> models.SchemaFieldClass:
        # This must exist - if it doesn't, we've got a larger bug.
        schema_dict = self._parent._schema_dict()
        return schema_dict[self._field_path]

    def _get_editable_schema_field(
        self,
    ) -> Optional[models.EditableSchemaFieldInfoClass]:
        # This method does not make any mutations.
        editable_schema = self._parent._get_aspect(models.EditableSchemaMetadataClass)
        if editable_schema is None:
            return None
        for field in editable_schema.editableSchemaFieldInfo:
            if field.fieldPath == self._field_path:
                return field
        return None

    def _ensure_editable_schema_field(self) -> models.EditableSchemaFieldInfoClass:
        if is_ingestion_attribution():
            warnings.warn(
                "This method should not be used in ingestion mode.",
                IngestionAttributionWarning,
                stacklevel=2,
            )
        editable_schema = self._parent._setdefault_aspect(
            models.EditableSchemaMetadataClass(editableSchemaFieldInfo=[])
        )
        for field in editable_schema.editableSchemaFieldInfo:
            if field.fieldPath == self._field_path:
                return field

        # If we don't have an entry for this field yet, create one.
        field = models.EditableSchemaFieldInfoClass(fieldPath=self._field_path)
        editable_schema.editableSchemaFieldInfo.append(field)
        return field

    @property
    def field_path(self) -> str:
        return self._field_path

    @property
    def mapped_type(self) -> models.SchemaFieldDataTypeClass:
        return self._base_schema_field().type

    @property
    def native_type(self) -> str:
        return self._base_schema_field().nativeDataType

    # TODO expose nullability and primary/foreign key details

    @property
    def description(self) -> Optional[str]:
        editable_field = self._get_editable_schema_field()
        return first_non_null(
            [
                editable_field.description if editable_field is not None else None,
                self._base_schema_field().description,
            ]
        )

    def set_description(self, description: str) -> None:
        if is_ingestion_attribution():
            editable_field = self._get_editable_schema_field()
            if editable_field and editable_field.description is not None:
                warnings.warn(
                    "The field description will be hidden by UI-based edits. "
                    "Change the edit mode to OVERWRITE_UI to override this behavior.",
                    category=IngestionAttributionWarning,
                    stacklevel=2,
                )

            self._base_schema_field().description = description
        else:
            self._ensure_editable_schema_field().description = description

    @property
    def tags(self) -> Optional[List[models.TagAssociationClass]]:
        # Tricky: if either has a non-null globalTags, this will not return None.
        tags = None

        if (base_tags := self._base_schema_field().globalTags) is not None:
            tags = tags or []
            tags.extend(base_tags.tags)

        if editable_field := self._get_editable_schema_field():
            if (editable_tags := editable_field.globalTags) is not None:
                tags = tags or []
                tags.extend(editable_tags.tags)

        return tags

    def set_tags(self, tags: TagsInputType) -> None:
        parsed_tags = [self._parent._parse_tag_association_class(tag) for tag in tags]

        if is_ingestion_attribution():
            editable_field = self._get_editable_schema_field()
            if editable_field and editable_field.globalTags:
                warnings.warn(
                    "Overwriting non-ingestion tags from ingestion is an anti-pattern.",
                    category=IngestionAttributionWarning,
                    stacklevel=2,
                )
                editable_field.globalTags = None

            self._base_schema_field().globalTags = models.GlobalTagsClass(
                tags=parsed_tags
            )
        else:
            base_field = self._base_schema_field()
            if base_field.globalTags:
                base_field.globalTags = None

            self._ensure_editable_schema_field().globalTags = models.GlobalTagsClass(
                tags=parsed_tags
            )

    def add_tag(self, tag: TagInputType) -> None:
        parsed_tag = self._parent._parse_tag_association_class(tag)

        if is_ingestion_attribution():
            raise SdkUsageError(
                "Adding field tags in ingestion mode is not yet supported. "
                "Use set_tags instead."
            )
        else:
            editable_field = self._ensure_editable_schema_field()
            if editable_field.globalTags is None:
                editable_field.globalTags = models.GlobalTagsClass(tags=[])

            add_list_unique(
                editable_field.globalTags.tags,
                key=self._parent._tag_key,
                item=parsed_tag,
            )

    def remove_tag(self, tag: TagInputType) -> None:
        parsed_tag = self._parent._parse_tag_association_class(tag)

        if is_ingestion_attribution():
            raise SdkUsageError(
                "Adding field tags in ingestion mode is not yet supported. "
                "Use set_tags instead."
            )
        else:
            base_field = self._base_schema_field()
            if base_field.globalTags is not None:
                remove_list_unique(
                    base_field.globalTags.tags,
                    key=self._parent._tag_key,
                    item=parsed_tag,
                    missing_ok=True,
                )

            editable_field = self._ensure_editable_schema_field()
            if editable_field.globalTags is not None:
                remove_list_unique(
                    editable_field.globalTags.tags,
                    key=self._parent._tag_key,
                    item=parsed_tag,
                )

    @property
    def terms(self) -> Optional[List[models.GlossaryTermAssociationClass]]:
        # TODO: Basically the same implementation as tags - can we share code?
        terms = None

        if (base_terms := self._base_schema_field().glossaryTerms) is not None:
            terms = terms or []
            terms.extend(base_terms.terms)

        if editable_field := self._get_editable_schema_field():
            if (editable_terms := editable_field.glossaryTerms) is not None:
                terms = terms or []
                terms.extend(editable_terms.terms)

        return terms

    def set_terms(self, terms: TermsInputType) -> None:
        parsed_terms = [
            self._parent._parse_glossary_term_association_class(term) for term in terms
        ]

        if is_ingestion_attribution():
            editable_field = self._get_editable_schema_field()
            if editable_field and editable_field.glossaryTerms:
                warnings.warn(
                    "Overwriting non-ingestion terms from ingestion is an anti-pattern.",
                    category=IngestionAttributionWarning,
                    stacklevel=2,
                )
                editable_field.glossaryTerms = None

            self._base_schema_field().glossaryTerms = models.GlossaryTermsClass(
                terms=parsed_terms,
                auditStamp=self._parent._terms_audit_stamp(),
            )
        else:
            base_field = self._base_schema_field()
            if base_field.glossaryTerms:
                base_field.glossaryTerms = None

            self._ensure_editable_schema_field().glossaryTerms = (
                models.GlossaryTermsClass(
                    terms=parsed_terms,
                    auditStamp=self._parent._terms_audit_stamp(),
                )
            )

    def add_term(self, term: TermInputType) -> None:
        parsed_term = self._parent._parse_glossary_term_association_class(term)

        if is_ingestion_attribution():
            raise SdkUsageError(
                "Adding field terms in ingestion mode is not yet supported. "
                "Use set_terms instead."
            )
        else:
            editable_field = self._ensure_editable_schema_field()
            if editable_field.glossaryTerms is None:
                editable_field.glossaryTerms = models.GlossaryTermsClass(
                    terms=[],
                    auditStamp=self._parent._terms_audit_stamp(),
                )

            add_list_unique(
                editable_field.glossaryTerms.terms,
                key=self._parent._terms_key,
                item=parsed_term,
            )

    def remove_term(self, term: TermInputType) -> None:
        parsed_term = self._parent._parse_glossary_term_association_class(term)

        if is_ingestion_attribution():
            raise SdkUsageError(
                "Removing field terms in ingestion mode is not yet supported. "
                "Use set_terms instead."
            )
        else:
            base_field = self._base_schema_field()
            if base_field.glossaryTerms is not None:
                remove_list_unique(
                    base_field.glossaryTerms.terms,
                    key=self._parent._terms_key,
                    item=parsed_term,
                    missing_ok=True,
                )

            editable_field = self._ensure_editable_schema_field()
            if editable_field.glossaryTerms is not None:
                remove_list_unique(
                    editable_field.glossaryTerms.terms,
                    key=self._parent._terms_key,
                    item=parsed_term,
                    missing_ok=True,
                )


class Dataset(
    HasPlatformInstance,
    HasSubtype,
    HasContainer,
    HasOwnership,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    Entity,
):
    """Represents a dataset in DataHub.

    A dataset represents a collection of data, such as a table, view, or file.
    This class provides methods for managing dataset metadata including schema,
    lineage, and various aspects like ownership, tags, and terms.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[DatasetUrn]:
        """Get the URN type for datasets.

        Returns:
            The DatasetUrn class.
        """
        return DatasetUrn

    def __init__(
        self,
        *,
        # Identity.
        platform: str,
        name: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        # Dataset properties.
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        qualified_name: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        # Standard aspects.
        parent_container: ParentContainerInputType | Unset = unset,
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        # TODO structured_properties
        domain: Optional[DomainInputType] = None,
        extra_aspects: ExtraAspectsType = None,
        # Dataset-specific aspects.
        schema: Optional[SchemaFieldsInputType] = None,
        upstreams: Optional[models.UpstreamLineageClass] = None,
    ):
        """Initialize a new Dataset instance.

        Args:
            platform: The platform this dataset belongs to (e.g. "mysql", "snowflake").
            name: The name of the dataset.
            platform_instance: Optional platform instance identifier.
            env: The environment this dataset belongs to (default: DEFAULT_ENV).
            description: Optional description of the dataset.
            display_name: Optional display name for the dataset.
            qualified_name: Optional qualified name for the dataset.
            external_url: Optional URL to external documentation or source.
            custom_properties: Optional dictionary of custom properties.
            created: Optional creation timestamp.
            last_modified: Optional last modification timestamp.
            parent_container: Optional parent container for this dataset.
            subtype: Optional subtype of the dataset.
            owners: Optional list of owners.
            links: Optional list of links.
            tags: Optional list of tags.
            terms: Optional list of glossary terms.
            domain: Optional domain this dataset belongs to.
            extra_aspects: Optional list of additional aspects.
            schema: Optional schema definition for the dataset.
            upstreams: Optional upstream lineage information.
        """
        urn = DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=name,
            platform_instance=platform_instance,
            env=env,
        )
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(urn.platform, platform_instance)

        if schema is not None:
            self._set_schema(schema)
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

        if parent_container is not unset:
            self._set_container(parent_container)
        if subtype is not None:
            self.set_subtype(subtype)
        if owners is not None:
            self.set_owners(owners)
        if links is not None:
            self.set_links(links)
        if tags is not None:
            self.set_tags(tags)
        if terms is not None:
            self.set_terms(terms)
        if domain is not None:
            self.set_domain(domain)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, DatasetUrn)
        entity = cls(
            platform=urn.platform,
            name=urn.name,
            env=urn.env,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> DatasetUrn:
        return self._urn  # type: ignore

    def _ensure_dataset_props(self) -> models.DatasetPropertiesClass:
        return self._setdefault_aspect(models.DatasetPropertiesClass())

    def _get_editable_props(self) -> Optional[models.EditableDatasetPropertiesClass]:
        return self._get_aspect(models.EditableDatasetPropertiesClass)

    def _ensure_editable_props(self) -> models.EditableDatasetPropertiesClass:
        # Note that most of the fields in this aspect are not used.
        # The only one that's relevant for us is the description.
        return self._setdefault_aspect(models.EditableDatasetPropertiesClass())

    @property
    def description(self) -> Optional[str]:
        """Get the description of the dataset.

        Returns:
            The description if set, None otherwise.
        """
        editable_props = self._get_editable_props()
        return first_non_null(
            [
                editable_props.description if editable_props is not None else None,
                self._ensure_dataset_props().description,
            ]
        )

    def set_description(self, description: str) -> None:
        """Set the description of the dataset.

        Args:
            description: The description to set.

        Note:
            If called during ingestion, this will warn if overwriting
            a non-ingestion description.
        """
        if is_ingestion_attribution():
            editable_props = self._get_editable_props()
            if editable_props is not None and editable_props.description is not None:
                warnings.warn(
                    "Overwriting non-ingestion description from ingestion is an anti-pattern.",
                    category=IngestionAttributionWarning,
                    stacklevel=2,
                )
                # Force the ingestion description to show up.
                editable_props.description = None

            self._ensure_dataset_props().description = description
        else:
            self._ensure_editable_props().description = description

    @property
    def display_name(self) -> Optional[str]:
        """Get the display name of the dataset.

        Returns:
            The display name if set, None otherwise.
        """
        return self._ensure_dataset_props().name

    def set_display_name(self, display_name: str) -> None:
        """Set the display name of the dataset.

        Args:
            display_name: The display name to set.
        """
        self._ensure_dataset_props().name = display_name

    @property
    def qualified_name(self) -> Optional[str]:
        """Get the qualified name of the dataset.

        Returns:
            The qualified name if set, None otherwise.
        """
        return self._ensure_dataset_props().qualifiedName

    def set_qualified_name(self, qualified_name: str) -> None:
        """Set the qualified name of the dataset.

        Args:
            qualified_name: The qualified name to set.
        """
        self._ensure_dataset_props().qualifiedName = qualified_name

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the dataset.

        Returns:
            The external URL if set, None otherwise.
        """
        return self._ensure_dataset_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the dataset.

        Args:
            external_url: The external URL to set.
        """
        self._ensure_dataset_props().externalUrl = external_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the dataset.

        Returns:
            Dictionary of custom properties.
        """
        return self._ensure_dataset_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the dataset.

        Args:
            custom_properties: Dictionary of custom properties to set.
        """
        self._ensure_dataset_props().customProperties = custom_properties

    @property
    def created(self) -> Optional[datetime]:
        """Get the creation timestamp of the dataset.

        Returns:
            The creation timestamp if set, None otherwise.
        """
        return parse_time_stamp(self._ensure_dataset_props().created)

    def set_created(self, created: datetime) -> None:
        """Set the creation timestamp of the dataset.

        Args:
            created: The creation timestamp to set.
        """
        self._ensure_dataset_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        """Get the last modification timestamp of the dataset.

        Returns:
            The last modification timestamp if set, None otherwise.
        """
        return parse_time_stamp(self._ensure_dataset_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_dataset_props().lastModified = make_time_stamp(last_modified)

    def _schema_dict(self) -> Dict[str, models.SchemaFieldClass]:
        schema_metadata = self._get_aspect(models.SchemaMetadataClass)
        if schema_metadata is None:
            raise ItemNotFoundError(f"Schema is not set for dataset {self.urn}")
        return {field.fieldPath: field for field in schema_metadata.fields}

    @property
    def schema(self) -> List[SchemaField]:
        # TODO: Add some caching here to avoid iterating over the schema every time.
        """Get the schema fields of the dataset.

        Returns:
            List of SchemaField objects representing the dataset's schema.
        """
        schema_dict = self._schema_dict()
        return [SchemaField(self, field_path) for field_path in schema_dict]

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
        else:
            assert_never(schema_field_input)

    def _set_schema(self, schema: SchemaFieldsInputType) -> None:
        # This method is not public. Ingestion/restatement users should be setting
        # the schema via the constructor. SDK users that got a dataset from the graph
        # probably shouldn't be adding/removing fields ad-hoc. The field-level mutators
        # can be used instead.
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

    def __getitem__(self, field_path: str) -> SchemaField:
        # TODO: Automatically deal with field path v2?
        """Get a schema field by its path.

        Args:
            field_path: The path of the field to retrieve.

        Returns:
            A SchemaField instance.

        Raises:
            SchemaFieldKeyError: If the field is not found.
        """
        schema_dict = self._schema_dict()
        if field_path not in schema_dict:
            raise SchemaFieldKeyError(f"Field {field_path} not found in schema")
        return SchemaField(self, field_path)

    @property
    def upstreams(self) -> Optional[models.UpstreamLineageClass]:
        return self._get_aspect(models.UpstreamLineageClass)

    def set_upstreams(self, upstreams: UpstreamLineageInputType) -> None:
        self._set_aspect(_parse_upstream_lineage_input(upstreams, self.urn))
