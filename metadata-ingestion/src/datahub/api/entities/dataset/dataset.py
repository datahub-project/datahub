import json
import logging
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, validator
from ruamel.yaml import YAML

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
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.dataset_urn import DatasetUrn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaFieldSpecification(BaseModel):
    id: Optional[str] = None
    urn: Optional[str] = None
    structured_properties: Optional[
        Dict[str, Union[str, float, List[Union[str, float]]]]
    ] = None
    type: Optional[str] = None
    nativeDataType: Optional[str] = None
    jsonPath: Union[None, str] = None
    nullable: Optional[bool] = None
    description: Union[None, str] = None
    label: Optional[str] = None
    created: Optional[dict] = None
    lastModified: Optional[dict] = None
    recursive: Optional[bool] = None
    globalTags: Optional[List[str]] = None
    glossaryTerms: Optional[List[str]] = None
    isPartOfKey: Optional[bool] = None
    isPartitioningKey: Optional[bool] = None
    jsonProps: Optional[dict] = None

    def with_structured_properties(
        self,
        structured_properties: Optional[Dict[str, List[Union[str, float]]]],
    ) -> "SchemaFieldSpecification":
        self.structured_properties = (
            {k: v for k, v in structured_properties.items()}
            if structured_properties
            else None
        )
        return self

    @classmethod
    def from_schema_field(
        cls, schema_field: SchemaFieldClass, parent_urn: str
    ) -> "SchemaFieldSpecification":
        return SchemaFieldSpecification(
            id=Dataset._simplify_field_path(schema_field.fieldPath),
            urn=make_schema_field_urn(
                parent_urn, Dataset._simplify_field_path(schema_field.fieldPath)
            ),
            type=str(schema_field.type),
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
            globalTags=(
                schema_field.globalTags.__dict__ if schema_field.globalTags else None
            ),
            glossaryTerms=(
                schema_field.glossaryTerms.__dict__
                if schema_field.glossaryTerms
                else None
            ),
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


class SchemaSpecification(BaseModel):
    file: Optional[str] = None
    fields: Optional[List[SchemaFieldSpecification]] = None

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
    value: Union[str, float, List[str], List[float]]
    created: Optional[str] = None
    lastModified: Optional[str] = None


class Dataset(BaseModel):
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
    structured_properties: Optional[
        Dict[str, Union[str, float, List[Union[str, float]]]]
    ] = None
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

    @classmethod
    def from_yaml(cls, file: str) -> Iterable["Dataset"]:
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            datasets: Union[dict, List[dict]] = yaml.load(fp)
            if isinstance(datasets, dict):
                datasets = [datasets]
            for dataset_raw in datasets:
                dataset = Dataset.parse_obj(dataset_raw)
                yield dataset

    def generate_mcp(
        self,
    ) -> Iterable[Union[MetadataChangeProposalClass, MetadataChangeProposalWrapper]]:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=DatasetPropertiesClass(
                description=self.description,
                name=self.name,
                customProperties=self.properties,
                externalUrl=self.external_url,
            ),
        )
        yield mcp

        if self.schema_metadata:
            if self.schema_metadata.file:
                with open(self.schema_metadata.file) as schema_fp:
                    schema_string = schema_fp.read()
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
                for field in self.schema_metadata.fields:
                    field_urn = field.urn or make_schema_field_urn(
                        self.urn,  # type: ignore[arg-type]
                        field.id,  # type: ignore[arg-type]
                    )
                    assert field_urn.startswith("urn:li:schemaField:")

                    if field.globalTags:
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=field_urn,
                            aspect=GlobalTagsClass(
                                tags=[
                                    TagAssociationClass(tag=make_tag_urn(tag))
                                    for tag in field.globalTags
                                ]
                            ),
                        )
                        yield mcp

                    if field.glossaryTerms:
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=field_urn,
                            aspect=GlossaryTermsClass(
                                terms=[
                                    GlossaryTermAssociationClass(
                                        urn=make_term_urn(term)
                                    )
                                    for term in field.glossaryTerms
                                ],
                                auditStamp=self._mint_auditstamp("yaml"),
                            ),
                        )
                        yield mcp

                    if field.structured_properties:
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=field_urn,
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
            schema_specification = SchemaSpecification(
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
                ]
            )
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
    def from_datahub(cls, graph: DataHubGraph, urn: str) -> "Dataset":
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
            structured_properties_map: Dict[str, List[Union[str, float]]] = {}
            for sp in structured_properties.properties:
                if sp.propertyUrn in structured_properties_map:
                    assert isinstance(structured_properties_map[sp.propertyUrn], list)
                    structured_properties_map[sp.propertyUrn].extend(sp.values)  # type: ignore[arg-type,union-attr]
                else:
                    structured_properties_map[sp.propertyUrn] = sp.values

        return Dataset(  # type: ignore[call-arg]
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
            tags=[tag.tag for tag in tags.tags] if tags else None,
            glossary_terms=(
                [term.urn for term in glossary_terms.terms] if glossary_terms else None
            ),
            owners=yaml_owners,
            properties=(
                dataset_properties.customProperties if dataset_properties else None
            ),
            subtypes=[subtype for subtype in subtypes.typeNames] if subtypes else None,
            structured_properties=(
                structured_properties_map if structured_properties else None
            ),
        )

    def to_yaml(
        self,
        file: Path,
    ) -> None:
        with open(file, "w") as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.default_flow_style = False
            yaml.dump(self.dict(exclude_none=True, exclude_unset=True), fp)
