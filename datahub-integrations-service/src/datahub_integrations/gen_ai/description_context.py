import ast
import html
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import AspectBag
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from loguru import logger
from pydantic import BaseModel, Field

from datahub_integrations.gen_ai.bedrock import (
    BedrockModel,
    get_bedrock_model_env_variable,
)

DESCRIPTION_GENERATION_MODEL: BedrockModel | str = get_bedrock_model_env_variable(
    "DESCRIPTION_GENERATION_BEDROCK_MODEL", BedrockModel.CLAUDE_3_HAIKU
)

_MAX_UPSTREAM_TABLES = 5
_MAX_DOWNSTREAM_TABLES = 8
_MAX_UPSTREAM_FIELDS_PER_COLUMN = 5

# New constants for content truncation
_MAX_GLOSSARY_TERM_DEFINITION_LENGTH = 2000
_MAX_TABLE_DESCRIPTION_LENGTH = 2000
_MAX_COLUMN_DESCRIPTION_LENGTH = 1000
_MAX_TAG_DESCRIPTION_LENGTH = 500
_MAX_DOMAIN_DESCRIPTION_LENGTH = 800


class DescriptionParsingError(Exception):
    pass


class SchemaFieldMetadata(BaseModel):
    description: Optional[str] = None
    created: Optional[Any] = None
    nativeDataType: Optional[str] = None
    isPartOfKey: Optional[bool] = None
    isPartitioningKey: Optional[bool] = None


class UpstreamColumnMetadata(BaseModel):
    upstream_column_name: str
    upstream_column_description: Optional[str] = None
    upstream_column_native_type: Optional[str] = None


class UpstreamLineageInfo(BaseModel):
    lineage: Sequence[Union[str, UpstreamColumnMetadata]]
    transform_operation: Optional[str] = None


class QueryInfo(BaseModel):
    value: str
    language: str


class TableUpstreamLineageInfo(BaseModel):
    upstream_table_urn: str
    upstream_table_name: Optional[str] = None
    upstream_table_description: Optional[str] = None
    upstream_table_type: Optional[str] = None
    query: Optional[QueryInfo] = None
    lineage_type: Optional[str] = None


class TableDownstreamLineageInfo(BaseModel):
    downstream_table_urn: str
    downstream_table_name: Optional[str] = None
    downstream_table_description: Optional[str] = None
    downstream_table_type: Optional[str] = None


class TagInfo(BaseModel):
    tag_name: str
    tag_description: Optional[str] = None


class GlossaryTermInfo(BaseModel):
    term_name: str
    term_definition: Optional[str] = None


class DomainInfo(BaseModel):
    domain_name: Optional[str] = None
    domain_description: Optional[str] = None


class OwnerInfo(BaseModel):
    owner_name: str
    owner_type: str


class ViewInfo(BaseModel):
    materialized: bool
    view_logic: Optional[str] = None
    view_language: Optional[str] = None


class ExtractedTableInfo(BaseModel):
    urn: str
    column_names: Dict[str, str] = Field(default_factory=dict)
    column_metadata: Dict[str, SchemaFieldMetadata] = Field(default_factory=dict)
    column_descriptions: Dict[str, Optional[str]] = Field(default_factory=dict)
    column_upstream_lineages: Dict[str, List[UpstreamLineageInfo]] = Field(
        default_factory=dict
    )
    column_sample_values: Optional[Dict[str, List[str]]] = None
    column_tags: Dict[str, List[TagInfo]] = Field(default_factory=dict)
    column_glossary_terms: Dict[str, List[GlossaryTermInfo]] = Field(
        default_factory=dict
    )
    table_tags: List[TagInfo] = Field(default_factory=list)
    table_glossary_terms: List[GlossaryTermInfo] = Field(default_factory=list)
    table_view_properties: Optional[ViewInfo] = None
    table_name: Optional[str] = None
    table_description: Optional[str] = None
    table_subtype: Optional[str] = None
    table_domains_info: Optional[List[DomainInfo]] = None
    table_owners_info: Optional[List[OwnerInfo]] = None
    table_upstream_lineage_info: Optional[List[TableUpstreamLineageInfo]] = None
    table_downstream_lineage_info: List[TableDownstreamLineageInfo] = Field(
        default_factory=list
    )


@dataclass
class EntityDescriptionResult:
    table_description: Optional[str]
    column_descriptions: Optional[Dict[str, str]]
    extracted_entity_info: "ExtractedTableInfo"
    failure_reason: Optional[str] = None


class ColumnMetadataInfo(BaseModel):
    column_name: Optional[str] = None
    metadata: Optional[SchemaFieldMetadata] = None
    descriptions: Optional[str] = None
    upstream_lineages: Optional[List[UpstreamLineageInfo]] = None
    sample_values: Optional[List[str]] = None
    tags: Optional[List[TagInfo]] = None
    glossary_terms: Optional[List[GlossaryTermInfo]] = None


class TableInfo(BaseModel):
    type: Optional[str] = None

    tags: Optional[List[TagInfo]] = None
    glossary_terms: Optional[List[GlossaryTermInfo]] = None
    view_properties: Optional[ViewInfo] = None
    name: Optional[str] = None
    description: Optional[str] = None
    domains_info: Optional[List[DomainInfo]] = None
    owners_info: Optional[List[OwnerInfo]] = None
    upstream_lineage_info: Optional[List[TableUpstreamLineageInfo]] = None
    downstream_lineage_info: Optional[List[TableDownstreamLineageInfo]] = None


def sanitize_html_content(text: str) -> str:
    """Remove HTML tags and decode HTML entities from text."""
    if not text:
        return text

    # Remove HTML tags (including img tags)
    text = re.sub(r"<[^>]+>", "", text)

    # Decode HTML entities
    text = html.unescape(text)

    return text.strip()


def truncate_with_ellipsis(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to max_length and add suffix if truncated."""
    if not text or len(text) <= max_length:
        return text

    # Account for suffix length
    actual_max = max_length - len(suffix)
    return text[:actual_max] + suffix


def sanitize_markdown_content(text: str) -> str:
    """Remove markdown-style embeds that contain encoded data from text, but preserve alt text."""
    if not text:
        return text

    # Remove markdown embeds with data URLs (base64 encoded content) but preserve alt text
    # Pattern: ![alt text](data:image/type;base64,encoded_data) -> alt text
    text = re.sub(r"!\[([^\]]*)\]\(data:[^)]+\)", r"\1", text)

    return text.strip()


def sanitize_and_truncate_description(text: str, max_length: int) -> str:
    """Sanitize HTML content and truncate to specified length."""
    if not text:
        return text

    try:
        # First sanitize HTML content
        sanitized = sanitize_html_content(text)

        # Then sanitize markdown content (preserving alt text)
        sanitized = sanitize_markdown_content(sanitized)

        # Then truncate if needed
        return truncate_with_ellipsis(sanitized, max_length)
    except Exception as e:
        logger.warning(f"Error sanitizing and truncating description: {e}")
        return text[:max_length] if len(text) > max_length else text


def get_lineage_query(graph_client: DataHubGraph, urn: str) -> Optional[QueryInfo]:
    entity = graph_client.get_entity_semityped(urn)
    if "queryProperties" not in entity:
        logger.warning(f"Query properties not found on the query entity {urn}")
        return None

    query = entity["queryProperties"].statement.value
    language = entity["queryProperties"].statement.language

    return QueryInfo(value=query, language=language)


def make_schema_field_metadata(
    schema_field: models.SchemaFieldClass,
) -> SchemaFieldMetadata:
    description = schema_field.description
    if description:
        description = sanitize_and_truncate_description(
            description, _MAX_COLUMN_DESCRIPTION_LENGTH
        )

    field_metadata = SchemaFieldMetadata(
        description=description,
        created=schema_field.created,
        nativeDataType=schema_field.nativeDataType,
        isPartOfKey=schema_field.isPartOfKey if schema_field.isPartOfKey else None,
        isPartitioningKey=(
            schema_field.isPartitioningKey if schema_field.isPartitioningKey else None
        ),
    )
    return field_metadata


def get_column_upstream_metadata(
    graph_client: DataHubGraph, upstreams: List[str]
) -> List[UpstreamColumnMetadata]:
    upstreams_metadata: List[UpstreamColumnMetadata] = []
    for urn in upstreams:
        schema_field_urn = SchemaFieldUrn.from_string(urn)
        entity = graph_client.get_entity_semityped(schema_field_urn.parent)
        schema_metadata = entity.get("schemaMetadata")
        column_name = schema_field_urn.field_path
        if schema_metadata is None:
            continue
        for field in schema_metadata.fields:
            if field.fieldPath == column_name:
                description = field.description
                if description:
                    description = sanitize_and_truncate_description(
                        description, _MAX_COLUMN_DESCRIPTION_LENGTH
                    )

                upstreams_metadata.append(
                    UpstreamColumnMetadata(
                        upstream_column_name=urn,
                        upstream_column_description=description,
                        upstream_column_native_type=field.nativeDataType,
                    )
                )
                break
    return upstreams_metadata


def is_shell_or_soft_deleted_entity(
    graph_client: DataHubGraph, urn: str, entity: AspectBag
) -> bool:
    """Check if the entity is a ghost entity /shell entity or soft deleted entity."""
    if not graph_client.exists(urn):
        return True

    if entity.get("status") is not None:
        status_aspect = entity["status"]
        if status_aspect.removed:
            return True
    return False


def get_table_upstream_lineage_info(
    upstreams: List[models.UpstreamClass], graph_client: DataHubGraph
) -> List[TableUpstreamLineageInfo]:
    table_upstream_lineage_info: List[TableUpstreamLineageInfo] = []
    for upstream in upstreams:
        if len(table_upstream_lineage_info) >= _MAX_UPSTREAM_TABLES:
            break
        dataset_urn = upstream.dataset
        entity = graph_client.get_entity_semityped(dataset_urn)
        if is_shell_or_soft_deleted_entity(
            graph_client=graph_client, urn=dataset_urn, entity=entity
        ):
            logger.debug(f"Skipping shell or soft deleted entity: {dataset_urn}")
            continue
        (
            upstream_table_name,
            upstream_table_description,
        ) = get_table_name_and_description(entity=entity, urn=dataset_urn)
        upstream_table_subtype = get_table_subtype(entity)
        if upstream.query is not None:
            query = get_lineage_query(graph_client, upstream.query)
        else:
            query = None
        lineage_type = str(upstream.type) if upstream.type is not None else None
        table_upstream_lineage_info.append(
            TableUpstreamLineageInfo(
                upstream_table_urn=dataset_urn,
                upstream_table_name=upstream_table_name,
                upstream_table_description=upstream_table_description,
                upstream_table_type=upstream_table_subtype,
                query=query,
                lineage_type=lineage_type,
            )
        )
    return table_upstream_lineage_info


def get_downstream_urns_for_table(
    table_urn: str, graph_client: DataHubGraph
) -> list[str]:
    downstream_urns = []
    query = """query getDatasetDownstreams($input: String!) {
      dataset(urn: $input) {
        lineage(input: {direction: DOWNSTREAM}) {
          relationships {
            entity {
              urn
            }
          }
        }
      }
    }"""
    variables = {"input": table_urn}
    entity = graph_client.execute_graphql(query, variables)
    if entity.get("dataset") is not None:
        lineage = entity["dataset"].get("lineage")
        if lineage is not None:
            relationships = entity["dataset"]["lineage"].get("relationships")
            for relationship in relationships or []:
                if relationship.get("entity") is not None:
                    downstream_urns.append(relationship.get("entity").get("urn"))
    return downstream_urns


def get_table_downstream_lineage_info(
    urn: str, graph_client: DataHubGraph
) -> List[TableDownstreamLineageInfo]:
    table_downstream_lineage_info: List[TableDownstreamLineageInfo] = []
    downstream_urns = get_downstream_urns_for_table(urn, graph_client)
    # TODO: should we consider non-dataset downstreams (e.g. tasks, dashboards, etc.)?
    downstream_dataset_urns = [
        urn for urn in downstream_urns if urn.startswith("urn:li:dataset:(")
    ]
    for downstream_urn in downstream_dataset_urns:
        if len(table_downstream_lineage_info) >= _MAX_DOWNSTREAM_TABLES:
            break
        entity = graph_client.get_entity_semityped(downstream_urn)
        if is_shell_or_soft_deleted_entity(
            graph_client=graph_client, urn=downstream_urn, entity=entity
        ):
            logger.debug(f"Skipping shell or soft deleted entity: {downstream_urn}")
            continue
        (
            downstream_table_name,
            downstream_table_description,
        ) = get_table_name_and_description(entity=entity, urn=downstream_urn)
        downstream_table_subtype = get_table_subtype(entity)
        lineage_info = TableDownstreamLineageInfo(
            downstream_table_urn=downstream_urn,
            downstream_table_name=downstream_table_name,
            downstream_table_description=downstream_table_description,
            downstream_table_type=downstream_table_subtype,
        )
        table_downstream_lineage_info.append(lineage_info)
    return table_downstream_lineage_info


def get_upstream_finegrained_lineage_info(
    column_urns: list[str],
    finegrained_lineages: List[models.FineGrainedLineageClass],
    graph_client: DataHubGraph,
) -> Dict[str, List[UpstreamLineageInfo]]:
    column_lineages: Dict[str, List[UpstreamLineageInfo]] = {}
    for lineage in finegrained_lineages:
        if not lineage.upstreams or not lineage.downstreams:
            continue
        for downstream in lineage.downstreams:
            column_urn = downstream
            if column_urn in column_urns:
                upstreams_with_metadata = get_column_upstream_metadata(
                    graph_client, lineage.upstreams
                )
                column_upstream: UpstreamLineageInfo
                if len(upstreams_with_metadata) != 0:
                    column_upstream = UpstreamLineageInfo(
                        lineage=upstreams_with_metadata,
                        transform_operation=lineage.transformOperation,
                    )
                else:
                    # We do not consider upstreams without metadata to avoid shell entities
                    continue
                if column_urn in column_lineages:
                    if (
                        len(column_lineages[column_urn])
                        < _MAX_UPSTREAM_FIELDS_PER_COLUMN
                    ):
                        column_lineages[column_urn].append(column_upstream)
                    else:
                        continue
                else:
                    column_lineages[column_urn] = [column_upstream]

    for column_urn in column_urns:
        if column_urn not in column_lineages:
            column_lineages[column_urn] = []
    return column_lineages


def get_sample_values(
    urn: str, graph_client: DataHubGraph
) -> Optional[Dict[str, List[str]]]:
    sample_values: Dict[str, List[str]] = {}
    dataset_profiles = graph_client.get_timeseries_values(
        entity_urn=urn, aspect_type=models.DatasetProfileClass, filter={}, limit=1
    )
    if len(dataset_profiles) == 0:
        return None
    else:
        latest_dataset_profile = dataset_profiles[0]
        field_profiles = latest_dataset_profile.fieldProfiles
        if field_profiles is None:
            return None
        else:
            for field in field_profiles:
                if field.sampleValues is not None:
                    sample_values[f"urn:li:schemaField:({urn},{field.fieldPath})"] = (
                        field.sampleValues
                    )
    return sample_values if sample_values else None


def get_table_name_and_description(
    entity: AspectBag, urn: str
) -> tuple[Optional[str], Optional[str]]:
    if dataset_properties := entity.get("datasetProperties"):
        dataset_name, dataset_description = (
            dataset_properties.name,
            dataset_properties.description,
        )
    else:
        dataset_key = entity.get("datasetKey")
        if dataset_key is None or dataset_key.name in [None, ""]:
            dataset_name = DatasetUrn.from_string(urn).name
        else:
            dataset_name = dataset_key.name.split(".")[-1]
        dataset_description = None

    if (
        editable_dataset_properties := entity.get("editableDatasetProperties")
    ) and editable_dataset_properties.description:
        # If we have an edited description, that takes precedence over the one in the dataset properties.
        dataset_description = editable_dataset_properties.description

    if dataset_description:
        dataset_description = sanitize_and_truncate_description(
            dataset_description, _MAX_TABLE_DESCRIPTION_LENGTH
        )

    return dataset_name, dataset_description


def get_table_domain_info(
    domain_urns: List[str], graph_client: DataHubGraph
) -> List[DomainInfo]:
    domain_info: List[DomainInfo] = []
    for domain_urn in domain_urns:
        domain = graph_client.get_entity_semityped(domain_urn)
        domain_properties = domain.get("domainProperties")
        if domain_properties is not None:
            domain_name = domain_properties.name
            domain_description = domain_properties.description
            if domain_description:
                domain_description = sanitize_and_truncate_description(
                    domain_description, _MAX_DOMAIN_DESCRIPTION_LENGTH
                )
        else:
            domain_name = None
            domain_description = None
        domain_info.append(
            DomainInfo(domain_name=domain_name, domain_description=domain_description)
        )
    return domain_info


def get_ownership_info(owners: List[models.OwnerClass]) -> List[OwnerInfo]:
    owners_info: List[OwnerInfo] = []
    for owner in owners:
        owner_name = owner.owner.split(":")[-1]
        owner_type = str(owner.type) if owner.type is not None else ""
        owners_info.append(OwnerInfo(owner_name=owner_name, owner_type=owner_type))
    return owners_info


def get_tag_names_and_description(
    tags: models.GlobalTagsClass, graph_client: DataHubGraph
) -> List[TagInfo]:
    tag_info: List[TagInfo] = []
    for tag in tags.tags:
        tag_urn = tag.tag
        tag_details = graph_client.get_entity_semityped(tag_urn).get("tagProperties")
        if tag_details is not None:
            tag_name = tag_details.name
            tag_desc = tag_details.description
            if tag_desc:
                tag_desc = sanitize_and_truncate_description(
                    tag_desc, _MAX_TAG_DESCRIPTION_LENGTH
                )
        else:
            tag_name = tag_urn.rsplit("urn:li:tag:")[-1]
            tag_desc = ""
        tag_info.append(TagInfo(tag_name=tag_name, tag_description=tag_desc))
    return tag_info


def get_table_and_column_level_tags(
    entity: AspectBag, urn: str, graph_client: DataHubGraph
) -> Tuple[List[TagInfo], Dict[str, List[TagInfo]]]:
    # Table level tags:
    global_tags = entity.get("globalTags")
    if global_tags:
        table_level_tags = get_tag_names_and_description(global_tags, graph_client)
    else:
        table_level_tags = []

    # Column Level Tags:
    column_level_tags: Dict[str, List[TagInfo]] = {}
    if "schemaMetadata" in entity:
        for field in entity["schemaMetadata"].fields:
            column_urn = f"urn:li:schemaField:({urn},{field.fieldPath})"
            column_global_tags = field.globalTags
            if column_global_tags is not None:
                column_tag_names = get_tag_names_and_description(
                    column_global_tags, graph_client
                )
                column_level_tags[column_urn] = column_tag_names
    return table_level_tags, column_level_tags


def get_glossary_term_names_and_definition(
    terms: List[models.GlossaryTermAssociationClass], graph_client: DataHubGraph
) -> List[GlossaryTermInfo]:
    term_info: List[GlossaryTermInfo] = []
    for term in terms:
        term_urn = term.urn
        term_details = graph_client.get_entity_semityped(term_urn).get(
            "glossaryTermInfo"
        )
        if term_details is not None and term_details.name is not None:
            term_name = term_details.name
            term_def = term_details.definition
            if term_def:
                term_def = sanitize_and_truncate_description(
                    term_def, _MAX_GLOSSARY_TERM_DEFINITION_LENGTH
                )
            term_info.append(
                GlossaryTermInfo(term_name=term_name, term_definition=term_def)
            )
    return term_info


def get_table_and_column_level_glossary_terms(
    entity: AspectBag, urn: str, graph_client: DataHubGraph
) -> Tuple[List[GlossaryTermInfo], Dict[str, List[GlossaryTermInfo]]]:
    # Table level terms:
    global_terms = entity.get("glossaryTerms")
    if global_terms is not None:
        table_level_terms = get_glossary_term_names_and_definition(
            global_terms.terms, graph_client
        )
    else:
        table_level_terms = []

    # Column Level Terms:
    column_level_terms: Dict[str, List[GlossaryTermInfo]] = {}
    if "schemaMetadata" in entity:
        for field in entity["schemaMetadata"].fields:
            column_urn = f"urn:li:schemaField:({urn},{field.fieldPath})"
            column_global_terms = field.glossaryTerms
            if column_global_terms is not None:
                column_term_names = get_glossary_term_names_and_definition(
                    column_global_terms.terms, graph_client
                )
                column_level_terms[column_urn] = column_term_names
    return table_level_terms, column_level_terms


class ShellEntityError(Exception):
    pass


class TooManyColumnsError(Exception):
    pass


def extract_metadata_for_urn(
    entity: AspectBag, urn: str, graph_client: DataHubGraph
) -> ExtractedTableInfo:
    if "schemaMetadata" not in entity:
        raise ShellEntityError(
            f"Schema metadata not found in the entity {urn}; likely a shell entity."
        )

    column_names: Dict[str, str] = {}
    # TODO: This also contains the schema field description, which is redundant.
    column_metadata: Dict[str, SchemaFieldMetadata] = {}
    for field in entity["schemaMetadata"].fields:
        field_urn = make_schema_field_urn(urn, field.fieldPath)
        field_metadata = make_schema_field_metadata(field)
        column_metadata[field_urn] = field_metadata
        column_names[field_urn] = field.fieldPath

    column_descriptions = {}
    for field in entity["schemaMetadata"].fields:
        field_urn = make_schema_field_urn(urn, field.fieldPath)
        description = field.description
        if description:
            description = sanitize_and_truncate_description(
                description, _MAX_COLUMN_DESCRIPTION_LENGTH
            )
        column_descriptions[field_urn] = description

    if editableSchemaMetadata := entity.get("editableSchemaMetadata"):
        for efield in editableSchemaMetadata.editableSchemaFieldInfo:
            field_urn = make_schema_field_urn(urn, efield.fieldPath)
            if field_urn in column_descriptions and efield.description:
                column_descriptions[field_urn] = sanitize_and_truncate_description(
                    efield.description, _MAX_COLUMN_DESCRIPTION_LENGTH
                )

    # TODO: We should consider AI-generated descriptions for tables/columns
    # if no user-generated description is available.

    # Upstream Lineage Information
    upstream_lineages = entity.get("upstreamLineage")
    if upstream_lineages is None or upstream_lineages.fineGrainedLineages is None:
        upstream_lineage_info: dict[str, List[UpstreamLineageInfo]] = {
            column: [] for column in column_metadata.keys()
        }
    else:
        finegrained_lineages = upstream_lineages.fineGrainedLineages
        upstream_lineage_info = get_upstream_finegrained_lineage_info(
            column_urns=list(column_metadata.keys()),
            finegrained_lineages=finegrained_lineages,
            graph_client=graph_client,
        )

    # Table upstream lineage information
    if upstream_lineages is None:
        table_upstream_lineage_info = None
    else:
        table_upstream_lineage_info = get_table_upstream_lineage_info(
            upstreams=upstream_lineages.upstreams, graph_client=graph_client
        )

    # Table Downstream lineage information
    table_downstream_lineage_info = get_table_downstream_lineage_info(
        urn=urn, graph_client=graph_client
    )

    # Tags:
    table_level_tags, column_level_tags = get_table_and_column_level_tags(
        entity, urn, graph_client
    )

    # Glossary Terms:
    table_level_terms, column_level_terms = get_table_and_column_level_glossary_terms(
        entity, urn, graph_client
    )

    # Table name and description:
    table_name, table_description = get_table_name_and_description(
        entity=entity, urn=urn
    )

    # View Information:
    view_properties = entity.get("viewProperties")
    if view_properties is not None:
        table_view_properties = ViewInfo(
            materialized=view_properties.materialized,
            view_logic=view_properties.formattedViewLogic or view_properties.viewLogic,
            view_language=view_properties.viewLanguage,
        )
    else:
        table_view_properties = None

    # Table subtype:
    table_subtype = get_table_subtype(entity)

    # Table domain information:
    table_domains = entity.get("domains")
    if table_domains is not None:
        table_domain_info = get_table_domain_info(
            domain_urns=table_domains.domains, graph_client=graph_client
        )
    else:
        table_domain_info = None

    # Table owner Information:
    table_owners = entity.get("ownership")
    if table_owners is not None:
        table_owners_info = get_ownership_info(table_owners.owners)
    else:
        table_owners_info = None

    # Column Sample Values:
    column_sample_values = get_sample_values(urn=urn, graph_client=graph_client)

    extracted_table_info = ExtractedTableInfo(
        urn=urn,
        column_names=column_names,
        column_metadata=column_metadata,
        column_descriptions=column_descriptions,
        column_upstream_lineages=upstream_lineage_info,
        column_sample_values=column_sample_values,
        column_tags=column_level_tags,
        column_glossary_terms=column_level_terms,
        table_tags=table_level_tags,
        table_glossary_terms=table_level_terms,
        table_view_properties=table_view_properties,
        table_name=table_name,
        table_description=table_description,
        table_subtype=table_subtype,
        table_domains_info=table_domain_info,
        table_owners_info=table_owners_info,
        table_upstream_lineage_info=table_upstream_lineage_info,
        table_downstream_lineage_info=table_downstream_lineage_info,
    )
    return extracted_table_info


def get_table_subtype(entity: AspectBag) -> Optional[str]:
    table_subtypes = entity.get("subTypes")
    if table_subtypes is not None and table_subtypes.typeNames:
        table_subtype = table_subtypes.typeNames[0]
    else:
        table_subtype = None
    return table_subtype


def transform_table_info_for_llm(
    extracted_table_info: ExtractedTableInfo,
) -> Tuple[TableInfo, Dict[str, ColumnMetadataInfo]]:
    column_urns = extracted_table_info.column_names.keys()
    column_info: Dict[str, ColumnMetadataInfo] = {}
    for column in column_urns:
        column_name = SchemaFieldUrn.from_string(column).field_path
        column_info[column_name] = ColumnMetadataInfo(
            column_name=column_name,
            metadata=extracted_table_info.column_metadata.get(column),
            descriptions=extracted_table_info.column_descriptions.get(column),
            upstream_lineages=extracted_table_info.column_upstream_lineages.get(column),
            sample_values=(
                extracted_table_info.column_sample_values.get(column)
                if extracted_table_info.column_sample_values
                else None
            ),
            tags=extracted_table_info.column_tags.get(column),
            glossary_terms=extracted_table_info.column_glossary_terms.get(column),
        )
    # Create a properly typed TableInfo
    table_info = TableInfo(
        tags=extracted_table_info.table_tags,
        glossary_terms=extracted_table_info.table_glossary_terms,
        view_properties=extracted_table_info.table_view_properties,
        name=extracted_table_info.table_name,
        description=extracted_table_info.table_description,
        domains_info=extracted_table_info.table_domains_info,
        owners_info=extracted_table_info.table_owners_info,
        upstream_lineage_info=extracted_table_info.table_upstream_lineage_info,
        downstream_lineage_info=extracted_table_info.table_downstream_lineage_info,
    )
    return table_info, column_info


def parse_table_desc_llm_output(
    text: str,
) -> str:
    match = re.search(r"###.*", text, re.DOTALL)
    if match:
        md_str = match.group(0)
        table_description: str = md_str.strip().strip("'\"").strip()
        return table_description
    else:
        logger.info("No markdown heading found in the text.")
        raise DescriptionParsingError("No markdown heading found in the text.")


def parse_columns_llm_output(
    text: str,
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    match = re.search(r"\{[^}]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        # dict_str_cleaned = dict_str.replace("\n", " ").strip()
        dict_str_cleaned = dict_str.strip()
        # Escape single quotes that appear between lowercase letters to prevent syntax errors during ast.literal_eval
        dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)
            return extracted_dict, None

        except (SyntaxError, ValueError) as e:
            logger.info(f"Error evaluating dictionary: {e}. Text: {text}")
            return None, str(e)
    else:
        logger.info("No dictionary found in the text.")
        return None, "No dictionary found in the text."
