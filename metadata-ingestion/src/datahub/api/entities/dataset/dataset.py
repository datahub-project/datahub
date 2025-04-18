import json
import logging
import time
from pathlib import Path
from typing import (
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    get_args,
)

import avro
import yaml
from pydantic import BaseModel, Field, root_validator, validator
from ruamel.yaml import YAML
from typing_extensions import TypeAlias

import datahub.metadata.schema_classes as models
from datahub.api.entities.structuredproperties.structuredproperties import AllowedTypes
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_schema_field_urn,
    make_tag_urn,
    make_term_urn,
    make_user_urn,
    validate_ownership_type,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeProposalClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
)
from datahub.metadata.urns import (
    DataPlatformUrn,
    GlossaryTermUrn,
    SchemaFieldUrn,
    StructuredPropertyUrn,
    TagUrn,
)
from datahub.pydantic.compat import (
    PYDANTIC_VERSION,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.dataset_urn import DatasetUrn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrictModel(BaseModel):
    """
    Base model with strict validation.
    Compatible with both Pydantic v1 and v2.
    """

    if PYDANTIC_VERSION >= 2:
        # Pydantic v2 config
        model_config = {
            "validate_assignment": True,
            "extra": "forbid",
        }
    else:
        # Pydantic v1 config
        class Config:
            validate_assignment = True
            extra = "forbid"


# Define type aliases for the complex types
PropertyValue: TypeAlias = Union[float, str]
PropertyValueList: TypeAlias = List[PropertyValue]
StructuredProperties: TypeAlias = Dict[str, Union[PropertyValue, PropertyValueList]]


class StructuredPropertiesHelper:
    @staticmethod
    def simplify_structured_properties_list(
        structured_properties: Optional[StructuredProperties],
    ) -> Optional[StructuredProperties]:
        def urn_strip(urn: str) -> str:
            if urn.startswith("urn:li:structuredProperty:"):
                return urn[len("urn:li:structuredProperty:") :]
            return urn

        if structured_properties:
            simplified_structured_properties = (
                {urn_strip(k): v for k, v in structured_properties.items()}
                if structured_properties
                else None
            )
            if simplified_structured_properties:
                # convert lists to single values if possible
                for k, v in simplified_structured_properties.items():
                    if isinstance(v, list):
                        if len(v) == 1:
                            simplified_structured_properties[k] = v[0]
                        else:
                            simplified_structured_properties[k] = v
                    else:
                        simplified_structured_properties[k] = v

            return simplified_structured_properties
        return None


class SchemaFieldSpecification(StrictModel):
    id: Optional[str] = None
    urn: Optional[str] = None
    structured_properties: Optional[StructuredProperties] = None
    type: Optional[str] = None
    nativeDataType: Optional[str] = None
    jsonPath: Union[None, str] = None
    nullable: bool = False
    description: Union[None, str] = None
    doc: Union[None, str] = None  # doc is an alias for description
    label: Optional[str] = None
    created: Optional[dict] = None
    lastModified: Optional[dict] = None
    recursive: bool = False
    globalTags: Optional[List[str]] = None
    glossaryTerms: Optional[List[str]] = None
    isPartOfKey: Optional[bool] = None
    isPartitioningKey: Optional[bool] = None
    jsonProps: Optional[dict] = None

    def remove_type_metadata(self) -> "SchemaFieldSpecification":
        """
        Removes type metadata from the schema field specification.
        This is useful when syncing field metadata back to yaml when
        the type information is already present in the schema file.
        """
        self.type = None
        self.nativeDataType = None
        self.jsonPath = None
        self.isPartitioningKey = None
        self.isPartOfKey = None
        self.jsonProps = None
        return self

    def with_structured_properties(
        self, structured_properties: Optional[StructuredProperties]
    ) -> "SchemaFieldSpecification":
        self.structured_properties = (
            StructuredPropertiesHelper.simplify_structured_properties_list(
                structured_properties
            )
        )
        return self

    @classmethod
    def from_schema_field(
        cls, schema_field: SchemaFieldClass, parent_urn: str
    ) -> "SchemaFieldSpecification":
        return SchemaFieldSpecification(
            id=Dataset._simplify_field_path(schema_field.fieldPath),
            urn=make_schema_field_urn(parent_urn, schema_field.fieldPath),
            type=SchemaFieldSpecification._from_datahub_type(
                schema_field.type, schema_field.nativeDataType, allow_complex=True
            ),
            nativeDataType=schema_field.nativeDataType,
            nullable=schema_field.nullable,
            description=schema_field.description,
            label=schema_field.label,
            created=schema_field.created.__dict__ if schema_field.created else None,
            lastModified=(
                schema_field.lastModified.__dict__
                if schema_field.lastModified
                else None
            ),
            recursive=schema_field.recursive,
            globalTags=[TagUrn(tag.tag).name for tag in schema_field.globalTags.tags]
            if schema_field.globalTags
            else None,
            glossaryTerms=[
                GlossaryTermUrn(term.urn).name
                for term in schema_field.glossaryTerms.terms
            ]
            if schema_field.glossaryTerms
            else None,
            isPartitioningKey=schema_field.isPartitioningKey,
            jsonProps=(
                json.loads(schema_field.jsonProps) if schema_field.jsonProps else None
            ),
        )

    @validator("urn", pre=True, always=True)
    def either_id_or_urn_must_be_filled_out(cls, v, values):
        if not v and not values.get("id"):
            raise ValueError("Either id or urn must be present")
        return v

    @root_validator(pre=True)
    def sync_description_and_doc(cls, values: Dict) -> Dict:
        """Synchronize doc and description fields if one is provided but not the other."""
        description = values.get("description")
        doc = values.get("doc")

        if description is not None and doc is None:
            values["doc"] = description
        elif doc is not None and description is None:
            values["description"] = doc

        return values

    def get_datahub_type(self) -> models.SchemaFieldDataTypeClass:
        PrimitiveType = Literal[
            "string",
            "number",
            "int",
            "long",
            "float",
            "double",
            "boolean",
            "bytes",
            "fixed",
        ]
        type = self.type.lower() if self.type else self.type
        if type not in set(get_args(PrimitiveType)):
            raise ValueError(f"Type {self.type} is not a valid primitive type")

        if type == "string":
            return models.SchemaFieldDataTypeClass(type=models.StringTypeClass())
        elif type in ["number", "long", "float", "double", "int"]:
            return models.SchemaFieldDataTypeClass(type=models.NumberTypeClass())
        elif type == "fixed":
            return models.SchemaFieldDataTypeClass(type=models.FixedTypeClass())
        elif type == "bytes":
            return models.SchemaFieldDataTypeClass(type=models.BytesTypeClass())
        elif type == "boolean":
            return models.SchemaFieldDataTypeClass(type=models.BooleanTypeClass())

        raise ValueError(f"Type {self.type} is not a valid primitive type")

    @staticmethod
    def _from_datahub_type(
        input_type: models.SchemaFieldDataTypeClass,
        native_data_type: str,
        allow_complex: bool = False,
    ) -> str:
        if isinstance(input_type.type, models.StringTypeClass):
            return "string"
        elif isinstance(input_type.type, models.NumberTypeClass):
            if native_data_type in ["long", "float", "double", "int"]:
                return native_data_type
            return "number"
        elif isinstance(input_type.type, models.FixedTypeClass):
            return "fixed"
        elif isinstance(input_type.type, models.BytesTypeClass):
            return "bytes"
        elif isinstance(input_type.type, models.BooleanTypeClass):
            return "boolean"
        elif allow_complex and isinstance(input_type.type, models.ArrayTypeClass):
            return "array"
        elif allow_complex and isinstance(input_type.type, models.MapTypeClass):
            return "map"
        elif allow_complex and isinstance(input_type.type, models.UnionTypeClass):
            return "union"
        elif allow_complex:
            return "record"
        raise ValueError(f"Type {input_type} is not a valid primitive type")

    if PYDANTIC_VERSION < 2:

        def dict(self, **kwargs):
            """Custom dict method for Pydantic v1 to handle YAML serialization properly."""
            exclude = kwargs.pop("exclude", None) or set()

            # If description and doc are identical, exclude doc from the output
            if self.description == self.doc and self.description is not None:
                exclude.add("doc")

            # if nativeDataType and type are identical, exclude nativeDataType from the output
            if self.nativeDataType == self.type and self.nativeDataType is not None:
                exclude.add("nativeDataType")

            # if the id is the same as the urn's fieldPath, exclude id from the output

            if self.urn:
                field_urn = SchemaFieldUrn.from_string(self.urn)
                if Dataset._simplify_field_path(field_urn.field_path) == self.id:
                    exclude.add("urn")

            kwargs.pop("exclude_defaults", None)

            self.structured_properties = (
                StructuredPropertiesHelper.simplify_structured_properties_list(
                    self.structured_properties
                )
            )

            return super().dict(exclude=exclude, exclude_defaults=True, **kwargs)

    else:
        # For v2, implement model_dump with similar logic as dict
        def model_dump(self, **kwargs):
            """Custom model_dump method for Pydantic v2 to handle YAML serialization properly."""
            exclude = kwargs.pop("exclude", None) or set()

            # If description and doc are identical, exclude doc from the output
            if self.description == self.doc and self.description is not None:
                exclude.add("doc")

            # if nativeDataType and type are identical, exclude nativeDataType from the output
            if self.nativeDataType == self.type and self.nativeDataType is not None:
                exclude.add("nativeDataType")

            # if the id is the same as the urn's fieldPath, exclude id from the output
            if self.urn:
                field_urn = SchemaFieldUrn.from_string(self.urn)
                if Dataset._simplify_field_path(field_urn.field_path) == self.id:
                    exclude.add("urn")

            self.structured_properties = (
                StructuredPropertiesHelper.simplify_structured_properties_list(
                    self.structured_properties
                )
            )
            if hasattr(super(), "model_dump"):
                return super().model_dump(  # type: ignore
                    exclude=exclude, exclude_defaults=True, **kwargs
                )


class SchemaSpecification(BaseModel):
    file: Optional[str] = None
    fields: Optional[List[SchemaFieldSpecification]] = None
    raw_schema: Optional[str] = None

    @validator("file")
    def file_must_be_avsc(cls, v):
        if v and not v.endswith(".avsc"):
            raise ValueError("file must be a .avsc file")
        return v


class Ownership(ConfigModel):
    id: str
    type: str

    @validator("type")
    def ownership_type_must_be_mappable_or_custom(cls, v: str) -> str:
        _, _ = validate_ownership_type(v)
        return v


class StructuredPropertyValue(ConfigModel):
    value: Union[str, int, float, List[str], List[int], List[float]]
    created: Optional[str] = None
    lastModified: Optional[str] = None


class DatasetRetrievalConfig(BaseModel):
    include_downstreams: Optional[bool] = False


class Dataset(StrictModel):
    id: Optional[str] = None
    platform: Optional[str] = None
    env: str = "PROD"
    urn: Optional[str] = None
    description: Optional[str] = None
    name: Optional[str] = None
    schema_metadata: Optional[SchemaSpecification] = Field(alias="schema")
    downstreams: Optional[List[str]] = None
    properties: Optional[Dict[str, str]] = None
    subtype: Optional[str] = None
    subtypes: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    glossary_terms: Optional[List[str]] = None
    owners: Optional[List[Union[str, Ownership]]] = None
    structured_properties: Optional[StructuredProperties] = None
    external_url: Optional[str] = None

    @property
    def platform_urn(self) -> str:
        if self.platform:
            return make_data_platform_urn(self.platform)
        else:
            assert self.urn is not None  # validator should have filled this in
            dataset_urn = DatasetUrn.from_string(self.urn)
            return str(dataset_urn.get_data_platform_urn())

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if urn is not"
            assert "platform" in values, "platform must be present if urn is not"
            assert "env" in values, "env must be present if urn is not"
            return make_dataset_urn(values["platform"], values["id"], values["env"])
        return v

    @validator("name", pre=True, always=True)
    def name_filled_with_id_if_not_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if name is not"
            return values["id"]
        return v

    @validator("platform")
    def platform_must_not_be_urn(cls, v):
        if v.startswith("urn:li:dataPlatform:"):
            return v[len("urn:li:dataPlatform:") :]
        return v

    @validator("structured_properties")
    def simplify_structured_properties(cls, v):
        return StructuredPropertiesHelper.simplify_structured_properties_list(v)

    def _mint_auditstamp(self, message: str) -> AuditStampClass:
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    def _mint_owner(self, owner: Union[str, Ownership]) -> OwnerClass:
        if isinstance(owner, str):
            return OwnerClass(
                owner=make_user_urn(owner),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        else:
            assert isinstance(owner, Ownership)
            ownership_type, ownership_type_urn = validate_ownership_type(owner.type)
            return OwnerClass(
                owner=make_user_urn(owner.id),
                type=ownership_type,
                typeUrn=ownership_type_urn,
            )

    @staticmethod
    def get_patch_builder(urn: str) -> DatasetPatchBuilder:
        return DatasetPatchBuilder(urn)

    def patch_builder(self) -> DatasetPatchBuilder:
        assert self.urn
        return DatasetPatchBuilder(self.urn)

    @classmethod
    def from_yaml(cls, file: str) -> Iterable["Dataset"]:
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            datasets: Union[dict, List[dict]] = yaml.load(fp)
            if isinstance(datasets, dict):
                datasets = [datasets]
            for dataset_raw in datasets:
                dataset = Dataset.parse_obj(dataset_raw)
                # dataset = Dataset.model_validate(dataset_raw, strict=True)
                yield dataset

    def entity_references(self) -> List[str]:
        urn_prefix = f"{StructuredPropertyUrn.URN_PREFIX}:{StructuredPropertyUrn.LI_DOMAIN}:{StructuredPropertyUrn.ENTITY_TYPE}"
        references = []
        if self.schema_metadata:
            if self.schema_metadata.fields:
                for field in self.schema_metadata.fields:
                    if field.structured_properties:
                        references.extend(
                            [
                                f"{urn_prefix}:{prop_key}"
                                if not prop_key.startswith(urn_prefix)
                                else prop_key
                                for prop_key in field.structured_properties
                            ]
                        )
                    if field.glossaryTerms:
                        references.extend(
                            [make_term_urn(term) for term in field.glossaryTerms]
                        )
                    # We don't check references for tags
        if self.structured_properties:
            references.extend(
                [
                    f"{urn_prefix}:{prop_key}"
                    if not prop_key.startswith(urn_prefix)
                    else prop_key
                    for prop_key in self.structured_properties
                ]
            )
        if self.glossary_terms:
            references.extend([make_term_urn(term) for term in self.glossary_terms])

        # We don't check references for tags
        return list(set(references))

    def generate_mcp(
        self,
    ) -> Iterable[Union[MetadataChangeProposalClass, MetadataChangeProposalWrapper]]:
        patch_builder = self.patch_builder()

        patch_builder.set_custom_properties(self.properties or {})
        patch_builder.set_description(self.description)
        patch_builder.set_display_name(self.name)
        patch_builder.set_external_url(self.external_url)

        yield from patch_builder.build()

        if self.schema_metadata:
            schema_fields = set()
            if self.schema_metadata.file:
                with open(self.schema_metadata.file) as schema_fp:
                    schema_string = schema_fp.read()
                    schema_fields_list = avro_schema_to_mce_fields(schema_string)
                    schema_fields = {field.fieldPath for field in schema_fields_list}
                    schema_metadata = SchemaMetadataClass(
                        schemaName=self.name or self.id or self.urn or "",
                        platform=self.platform_urn,
                        version=0,
                        hash="",
                        platformSchema=OtherSchemaClass(rawSchema=schema_string),
                        fields=avro_schema_to_mce_fields(schema_string),
                    )
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=self.urn, aspect=schema_metadata
                    )
                    yield mcp

            if self.schema_metadata.fields:
                field_type_info_present = any(
                    field.type for field in self.schema_metadata.fields
                )
                all_fields_type_info_present = all(
                    field.type for field in self.schema_metadata.fields
                )
                if field_type_info_present and not all_fields_type_info_present:
                    raise ValueError(
                        "Either all fields must have type information or none of them should"
                    )

                if all_fields_type_info_present:
                    update_technical_schema = True
                else:
                    update_technical_schema = False
                if update_technical_schema and not self.schema_metadata.file:
                    # We produce a schema metadata aspect only if we have type information
                    # and a schema file is not provided.
                    schema_metadata = SchemaMetadataClass(
                        schemaName=self.name or self.id or self.urn or "",
                        platform=self.platform_urn,
                        version=0,
                        hash="",
                        fields=[
                            SchemaFieldClass(
                                fieldPath=field.id,  # type: ignore[arg-type]
                                type=field.get_datahub_type(),
                                nativeDataType=field.nativeDataType or field.type,  # type: ignore[arg-type]
                                nullable=field.nullable,
                                description=field.description,
                                label=field.label,
                                created=None,  # This should be auto-populated.
                                lastModified=None,  # This should be auto-populated.
                                recursive=field.recursive,
                                globalTags=GlobalTagsClass(
                                    tags=[
                                        TagAssociationClass(tag=make_tag_urn(tag))
                                        for tag in field.globalTags
                                    ]
                                )
                                if field.globalTags is not None
                                else None,
                                glossaryTerms=GlossaryTermsClass(
                                    terms=[
                                        GlossaryTermAssociationClass(
                                            urn=make_term_urn(term)
                                        )
                                        for term in field.glossaryTerms
                                    ],
                                    auditStamp=self._mint_auditstamp("yaml"),
                                )
                                if field.glossaryTerms is not None
                                else None,
                                isPartOfKey=field.isPartOfKey,
                                isPartitioningKey=field.isPartitioningKey,
                                jsonProps=json.dumps(field.jsonProps)
                                if field.jsonProps is not None
                                else None,
                            )
                            for field in self.schema_metadata.fields
                        ],
                        platformSchema=OtherSchemaClass(
                            rawSchema=yaml.dump(
                                self.schema_metadata.dict(
                                    exclude_none=True, exclude_unset=True
                                )
                            )
                        ),
                    )
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=self.urn, aspect=schema_metadata
                    )
                    yield mcp

                for field in self.schema_metadata.fields:
                    if schema_fields:
                        # search for the field in the schema fields set
                        matched_fields = [
                            schema_field
                            for schema_field in schema_fields
                            if field.id == schema_field
                            or field.id == Dataset._simplify_field_path(schema_field)
                        ]
                        if not matched_fields:
                            raise ValueError(
                                f"Field {field.id} not found in the schema file"
                            )
                        if len(matched_fields) > 1:
                            raise ValueError(
                                f"Field {field.id} matches multiple entries {matched_fields}in the schema file. Use the fully qualified field path."
                            )
                        assert len(matched_fields) == 1
                        assert (
                            self.urn is not None
                        )  # validator should have filled this in
                        field.urn = make_schema_field_urn(self.urn, matched_fields[0])
                    field_urn = field.urn or make_schema_field_urn(
                        self.urn,  # type: ignore[arg-type]
                        field.id,  # type: ignore[arg-type]
                    )
                    assert field_urn.startswith("urn:li:schemaField:")

                    if field.structured_properties:
                        urn_prefix = f"{StructuredPropertyUrn.URN_PREFIX}:{StructuredPropertyUrn.LI_DOMAIN}:{StructuredPropertyUrn.ENTITY_TYPE}"
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=field_urn,
                            aspect=StructuredPropertiesClass(
                                properties=[
                                    StructuredPropertyValueAssignmentClass(
                                        propertyUrn=f"{urn_prefix}:{prop_key}"
                                        if not prop_key.startswith(urn_prefix)
                                        else prop_key,
                                        values=(
                                            prop_value
                                            if isinstance(prop_value, list)
                                            else [prop_value]
                                        ),
                                    )
                                    for prop_key, prop_value in field.structured_properties.items()
                                ]
                            ),
                        )
                        yield mcp

        if self.subtype or self.subtypes:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=SubTypesClass(
                    typeNames=[s for s in [self.subtype] + (self.subtypes or []) if s]
                ),
            )
            yield mcp

        if self.tags:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=make_tag_urn(tag)) for tag in self.tags
                    ]
                ),
            )
            yield mcp

        if self.glossary_terms:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(urn=make_term_urn(term))
                        for term in self.glossary_terms
                    ],
                    auditStamp=self._mint_auditstamp("yaml"),
                ),
            )
            yield mcp

        if self.owners:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=OwnershipClass(
                    owners=[self._mint_owner(o) for o in self.owners]
                ),
            )
            yield mcp

        if self.structured_properties:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=StructuredPropertiesClass(
                    properties=[
                        StructuredPropertyValueAssignmentClass(
                            propertyUrn=f"urn:li:structuredProperty:{prop_key}",
                            values=(
                                prop_value
                                if isinstance(prop_value, list)
                                else [prop_value]
                            ),
                        )
                        for prop_key, prop_value in self.structured_properties.items()
                    ]
                ),
            )
            yield mcp

        if self.downstreams:
            for downstream in self.downstreams:
                patch_builder = DatasetPatchBuilder(downstream)
                assert self.urn is not None  # validator should have filled this in
                patch_builder.add_upstream_lineage(
                    UpstreamClass(
                        dataset=self.urn,
                        type="COPY",
                    )
                )
                yield from patch_builder.build()

        logger.info(f"Created dataset {self.urn}")

    @staticmethod
    def validate_type(
        sp_name: str, sp_value: Union[str, float], allowed_type: str
    ) -> Tuple[str, Union[str, float]]:
        if allowed_type == AllowedTypes.NUMBER.value:
            return (sp_name, float(sp_value))
        else:
            return (sp_name, sp_value)

    @staticmethod
    def _simplify_field_path(field_path: str) -> str:
        # field paths with [type=array] or [type=map] or [type=union] should never be simplified
        for type in ["array", "map", "union"]:
            if f"[type={type}]" in field_path:
                return field_path
        if field_path.startswith("[version=2.0]"):
            # v2 field path
            field_components = []
            current_field = ""
            for c in field_path:
                if c == "[":
                    if current_field:
                        field_components.append(current_field)
                    current_field = ""
                    omit_next = True
                elif c == "]":
                    omit_next = False
                elif c == ".":
                    pass
                elif not omit_next:
                    current_field += c
            if current_field:
                field_components.append(current_field)
            return ".".join(field_components)
        else:
            return field_path

    @staticmethod
    def _schema_from_schema_metadata(
        graph: DataHubGraph, urn: str
    ) -> Optional[SchemaSpecification]:
        schema_metadata: Optional[SchemaMetadataClass] = graph.get_aspect(
            urn, SchemaMetadataClass
        )

        if schema_metadata:
            # If the schema is built off of an avro schema, we only extract the fields if they have structured properties
            # Otherwise, we extract all fields
            if (
                schema_metadata.platformSchema
                and isinstance(schema_metadata.platformSchema, models.OtherSchemaClass)
                and schema_metadata.platformSchema.rawSchema
            ):
                try:
                    maybe_avro_schema = avro.schema.parse(
                        schema_metadata.platformSchema.rawSchema
                    )
                    schema_fields = avro_schema_to_mce_fields(maybe_avro_schema)
                except Exception as e:
                    logger.debug("Failed to parse avro schema: %s", e)
                    schema_fields = []

            schema_specification = SchemaSpecification(
                raw_schema=schema_metadata.platformSchema.rawSchema
                if hasattr(schema_metadata.platformSchema, "rawSchema")
                else None,
                fields=[
                    SchemaFieldSpecification.from_schema_field(
                        field, urn
                    ).with_structured_properties(
                        {
                            sp.propertyUrn: sp.values
                            for sp in structured_props.properties
                        }
                        if structured_props
                        else None
                    )
                    for field, structured_props in [
                        (
                            field,
                            graph.get_aspect(
                                make_schema_field_urn(urn, field.fieldPath),
                                StructuredPropertiesClass,
                            )
                            or graph.get_aspect(
                                make_schema_field_urn(
                                    urn, Dataset._simplify_field_path(field.fieldPath)
                                ),
                                StructuredPropertiesClass,
                            ),
                        )
                        for field in schema_metadata.fields
                    ]
                ],
            )
            if schema_fields and schema_specification.fields:
                # Source was an avro schema, so we only include fields with structured properties, tags or glossary terms
                schema_specification.fields = [
                    field.remove_type_metadata()
                    for field in schema_specification.fields
                    if field.structured_properties
                    or field.globalTags
                    or field.glossaryTerms
                ]
                if (
                    not schema_specification.fields
                ):  # set fields to None if there are no fields after filtering
                    schema_specification.fields = None
            return schema_specification
        else:
            return None

    @staticmethod
    def extract_owners_if_exists(
        owners: Optional[OwnershipClass],
    ) -> Optional[List[Union[str, Ownership]]]:
        yaml_owners: Optional[List[Union[str, Ownership]]] = None
        if owners:
            yaml_owners = []
            for o in owners.owners:
                if o.type == OwnershipTypeClass.TECHNICAL_OWNER:
                    yaml_owners.append(o.owner)
                elif o.type == OwnershipTypeClass.CUSTOM:
                    yaml_owners.append(Ownership(id=o.owner, type=str(o.typeUrn)))
                else:
                    yaml_owners.append(Ownership(id=o.owner, type=str(o.type)))
        return yaml_owners

    @classmethod
    def from_datahub(
        cls,
        graph: DataHubGraph,
        urn: str,
        config: DatasetRetrievalConfig = DatasetRetrievalConfig(),
    ) -> "Dataset":
        dataset_urn = DatasetUrn.from_string(urn)
        platform_urn = DataPlatformUrn.from_string(dataset_urn.platform)
        dataset_properties: Optional[DatasetPropertiesClass] = graph.get_aspect(
            urn, DatasetPropertiesClass
        )
        subtypes: Optional[SubTypesClass] = graph.get_aspect(urn, SubTypesClass)
        tags: Optional[GlobalTagsClass] = graph.get_aspect(urn, GlobalTagsClass)
        glossary_terms: Optional[GlossaryTermsClass] = graph.get_aspect(
            urn, GlossaryTermsClass
        )
        owners: Optional[OwnershipClass] = graph.get_aspect(urn, OwnershipClass)
        yaml_owners = Dataset.extract_owners_if_exists(owners)
        structured_properties: Optional[StructuredPropertiesClass] = graph.get_aspect(
            urn, StructuredPropertiesClass
        )
        if structured_properties:
            structured_properties_map: StructuredProperties = {}
            for sp in structured_properties.properties:
                if sp.propertyUrn in structured_properties_map:
                    assert isinstance(structured_properties_map[sp.propertyUrn], list)
                    structured_properties_map[sp.propertyUrn].extend(sp.values)  # type: ignore[arg-type,union-attr]
                else:
                    structured_properties_map[sp.propertyUrn] = sp.values

        if config.include_downstreams:
            related_downstreams = graph.get_related_entities(
                urn,
                relationship_types=[
                    "DownstreamOf",
                ],
                direction=DataHubGraph.RelationshipDirection.INCOMING,
            )
            downstreams = [r.urn for r in related_downstreams]

        return Dataset(  # type: ignore[arg-type]
            id=dataset_urn.name,
            platform=platform_urn.platform_name,
            urn=urn,
            description=(
                dataset_properties.description
                if dataset_properties and dataset_properties.description
                else None
            ),
            name=(
                dataset_properties.name
                if dataset_properties and dataset_properties.name
                else None
            ),
            schema=Dataset._schema_from_schema_metadata(graph, urn),
            tags=[TagUrn(tag.tag).name for tag in tags.tags] if tags else None,
            glossary_terms=(
                [GlossaryTermUrn(term.urn).name for term in glossary_terms.terms]
                if glossary_terms
                else None
            ),
            owners=yaml_owners,
            properties=(
                dataset_properties.customProperties if dataset_properties else None
            ),
            subtypes=[subtype for subtype in subtypes.typeNames] if subtypes else None,
            structured_properties=(
                structured_properties_map if structured_properties else None
            ),
            downstreams=downstreams if config.include_downstreams else None,
        )

    if PYDANTIC_VERSION < 2:

        def dict(self, **kwargs):
            """Custom dict method for Pydantic v1 to handle YAML serialization properly."""
            exclude = kwargs.pop("exclude", set())

            # If id and name are identical, exclude name from the output
            if self.id == self.name and self.id is not None:
                exclude.add("name")

            # if subtype and subtypes are identical or subtypes is a singleton list, exclude subtypes from the output
            if self.subtypes and len(self.subtypes) == 1:
                self.subtype = self.subtypes[0]
                exclude.add("subtypes")

            result = super().dict(exclude=exclude, **kwargs)

            # Custom handling for schema_metadata/schema
            if self.schema_metadata and "schema" in result:
                schema_data = result["schema"]

                # Handle fields if they exist
                if "fields" in schema_data and isinstance(schema_data["fields"], list):
                    # Process each field using its custom dict method
                    processed_fields = []
                    if self.schema_metadata and self.schema_metadata.fields:
                        for field in self.schema_metadata.fields:
                            if field:
                                # Use dict method for Pydantic v1
                                processed_field = field.dict(**kwargs)
                                processed_fields.append(processed_field)

                    # Replace the fields in the result with the processed ones
                    schema_data["fields"] = processed_fields

            return result
    else:

        def model_dump(self, **kwargs):
            """Custom model_dump method for Pydantic v2 to handle YAML serialization properly."""
            exclude = kwargs.pop("exclude", None) or set()

            # If id and name are identical, exclude name from the output
            if self.id == self.name and self.id is not None:
                exclude.add("name")

            # if subtype and subtypes are identical or subtypes is a singleton list, exclude subtypes from the output
            if self.subtypes and len(self.subtypes) == 1:
                self.subtype = self.subtypes[0]
                exclude.add("subtypes")

            if hasattr(super(), "model_dump"):
                result = super().model_dump(exclude=exclude, **kwargs)  # type: ignore
            else:
                result = super().dict(exclude=exclude, **kwargs)

            # Custom handling for schema_metadata/schema
            if self.schema_metadata and "schema" in result:
                schema_data = result["schema"]

                # Handle fields if they exist
                if "fields" in schema_data and isinstance(schema_data["fields"], list):
                    # Process each field using its custom model_dump method
                    processed_fields = []
                    if self.schema_metadata and self.schema_metadata.fields:
                        for field in self.schema_metadata.fields:
                            if field:
                                processed_field = field.model_dump(**kwargs)
                                processed_fields.append(processed_field)

                    # Replace the fields in the result with the processed ones
                    schema_data["fields"] = processed_fields

            return result

    def to_yaml(
        self,
        file: Path,
    ) -> bool:
        """
        Write model to YAML file only if content has changed.
        Preserves comments and structure of the existing YAML file.
        Returns True if file was written, False if no changes were detected.
        """
        # Create new model data
        # Create new model data - choose dict() or model_dump() based on Pydantic version
        if PYDANTIC_VERSION >= 2:
            new_data = self.model_dump(
                exclude_none=True, exclude_unset=True, by_alias=True
            )
        else:
            new_data = self.dict(exclude_none=True, exclude_unset=True, by_alias=True)

        # Set up ruamel.yaml for preserving comments
        yaml_handler = YAML(typ="rt")  # round-trip mode
        yaml_handler.default_flow_style = False
        yaml_handler.preserve_quotes = True  # type: ignore[assignment]
        yaml_handler.indent(mapping=2, sequence=2, offset=0)

        if file.exists():
            try:
                # Load existing data with comments preserved
                with open(file, "r") as fp:
                    existing_data = yaml_handler.load(fp)

                # Determine if the file contains a list or a single document
                if isinstance(existing_data, dict):
                    existing_data = [existing_data]
                    is_original_list = False
                else:
                    is_original_list = True
                if isinstance(existing_data, list):
                    # Handle list case
                    updated = False
                    identifier = "urn"
                    model_id = self.urn

                    if model_id is not None:
                        # Try to find and update existing item
                        for item in existing_data:
                            existing_dataset = Dataset(**item)
                            item_identifier = item.get(identifier, existing_dataset.urn)
                            if item_identifier == model_id:
                                # Found the item to update - preserve structure while updating values
                                updated = True
                                if (
                                    existing_dataset.schema_metadata
                                    and existing_dataset.schema_metadata.file
                                ):
                                    # Preserve the existing schema file path
                                    new_data["schema"]["file"] = (
                                        existing_dataset.schema_metadata.file
                                    )
                                    # Check if the content of the schema file has changed
                                    with open(
                                        existing_dataset.schema_metadata.file
                                    ) as schema_fp:
                                        schema_fp_content = schema_fp.read()

                                    if (
                                        schema_fp_content
                                        != new_data["schema"]["raw_schema"]
                                    ):
                                        # If the content has changed, update the schema file
                                        schema_file_path = Path(
                                            existing_dataset.schema_metadata.file
                                        )
                                        schema_file_path.write_text(
                                            new_data["schema"]["raw_schema"]
                                        )
                                # Remove raw_schema from the schema aspect before updating
                                if "schema" in new_data:
                                    new_data["schema"].pop("raw_schema")

                                _update_dict_preserving_comments(
                                    item, new_data, ["urn", "properties", "raw_schema"]
                                )
                                break

                    if not updated:
                        # Item not found, append to the list
                        existing_data.append(new_data)
                        updated = True

                    # If no update was needed, return early
                    if not updated:
                        return False

                    # Write the updated data back
                    with open(file, "w") as fp:
                        if not is_original_list:
                            existing_data = existing_data[0]
                        yaml_handler.dump(existing_data, fp)

                return True

            except Exception as e:
                # If there's any error, we'll create a new file
                print(
                    f"Error processing existing file {file}: {e}. Will create a new one."
                )
        else:
            # File doesn't exist or had errors - create a new one with default settings
            yaml_handler.indent(mapping=2, sequence=2, offset=0)

        file.parent.mkdir(parents=True, exist_ok=True)

        with open(file, "w") as fp:
            yaml_handler.dump(new_data, fp)

        return True


def _update_dict_preserving_comments(
    target: Dict, source: Dict, optional_fields: Optional[List[str]] = None
) -> None:
    """
    Updates a target dictionary with values from source, preserving comments and structure.
    This modifies the target dictionary in-place.
    """
    if optional_fields is None:
        optional_fields = ["urn"]
    # For each key in the source dict
    for key, value in source.items():
        if key in target:
            if isinstance(value, dict) and isinstance(target[key], dict):
                # Recursively update nested dictionaries
                _update_dict_preserving_comments(target[key], value)
            else:
                # Update scalar or list values
                # If target value is an int, and source value is a float that is equal to the int, convert to int
                if isinstance(value, float) and int(value) == value:
                    target[key] = int(value)
                else:
                    target[key] = value
        elif key not in optional_fields:
            # Add new keys
            target[key] = value

    # Remove keys that are in target but not in source
    keys_to_remove = [k for k in target if k not in source]
    for key in keys_to_remove:
        del target[key]


def _dict_equal(dict1: Dict, dict2: Dict, optional_keys: List[str]) -> bool:
    """
    Compare two dictionaries for equality, ignoring ruamel.yaml's metadata.
    """

    if len(dict1) != len(dict2):
        # Check if the difference is only in optional keys
        if len(dict1) > len(dict2):
            for key in optional_keys:
                if key in dict1 and key not in dict2:
                    del dict1[key]
        elif len(dict2) > len(dict1):
            for key in optional_keys:
                if key in dict2 and key not in dict1:
                    del dict2[key]
        if len(dict1) != len(dict2):
            return False

    for key, value in dict1.items():
        if key not in dict2:
            return False

        if isinstance(value, dict) and isinstance(dict2[key], dict):
            if not _dict_equal(value, dict2[key], optional_keys):
                return False
        elif isinstance(value, list) and isinstance(dict2[key], list):
            if len(value) != len(dict2[key]):
                return False

            # Check list items (simplified for brevity)
            for i in range(len(value)):
                if isinstance(value[i], dict) and isinstance(dict2[key][i], dict):
                    if not _dict_equal(value[i], dict2[key][i], optional_keys):
                        return False
                elif value[i] != dict2[key][i]:
                    return False
        elif value != dict2[key]:
            return False

    return True
