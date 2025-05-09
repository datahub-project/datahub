import ast
import os
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
    call_bedrock_llm,
    get_bedrock_model_env_variable,
)

PROMPT_TEMPLATE = '''\
You are tasked with generating concise descriptions for a DataHub table and its columns based on provided metadata. Here is the information you will be working with:

<table_info>
{table_info}
</table_info>

<column_info>
{column_info}
</column_info>

Generate the descriptions as follows:

1. Table Description:
   Create a few paragraphs of Markdown-formatted text that includes:
   a) A summary of the primary purpose and business importance of the table.
   b) If metadata is available, a summary of the upstream tables and transformations applied.
   c) A summary of the downstream tables (consumers) and general use cases for the table. Only include information that can be substantiated by the provided table_info.
   d) Technical notes and usage tips, including the table type (fact or dimension) and grain if available.
   e) A note on whether the table directly contains any PII data, like names, emails, and addresses. Do not provide recommendations related to access control, monitoring, or governance.

   Format any references to other entities as markdown links, using the entity URN as the link. For example: [table_name](urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD))
   Use Markdown sections like H2 and H3 with appropriate section titles. The first line should be "# <table name>", followed by a blank line.

2. Column Descriptions:
   For each column, create a concise description of one or two sentences. Prefer elliptical sentences that are direct and to the point. If available, include details about how the column was generated or calculated.

When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak phrases like "suggests", "could be", "likely", or "is considered". Only include information you are confident about based on the provided metadata.
- Be concise and to the point.

Provide your output in the following dictionary format:

{{
    "table_description": """
[Your multi-line table description here]
""",
    "column_name1": "Column description",
    "column_name2": "Column description",
    ...
}}

Ensure that the dictionary is properly formatted and parsable. Use the column display names as keys for the column descriptions. Include the table description with the key "table_description".\
'''


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
    query: Optional[QueryInfo] = None
    lineage_type: Optional[str] = None


class TableDownstreamLineageInfo(BaseModel):
    downstream_table_urn: str
    downstream_table_name: Optional[str] = None
    downstream_table_description: Optional[str] = None


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
    raw_llm_output: Optional[str]
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
    tags: Optional[List[TagInfo]] = None
    glossary_terms: Optional[List[GlossaryTermInfo]] = None
    view_properties: Optional[ViewInfo] = None
    name: Optional[str] = None
    description: Optional[str] = None
    domains_info: Optional[List[DomainInfo]] = None
    owners_info: Optional[List[OwnerInfo]] = None
    upstream_lineage_info: Optional[List[TableUpstreamLineageInfo]] = None
    downstream_lineage_info: Optional[List[TableDownstreamLineageInfo]] = None


DESCRIPTION_GENERATION_MODEL: BedrockModel | str = get_bedrock_model_env_variable(
    "DESCRIPTION_GENERATION_BEDROCK_MODEL", BedrockModel.CLAUDE_3_HAIKU
)
_MAX_COLUMNS = int(os.getenv("DESCRIPTION_GENERATION_MAX_COLUMNS", 100))

_MAX_UPSTREAM_TABLES = 5
_MAX_DOWNSTREAM_TABLES = 8
_MAX_UPSTREAM_FIELDS_PER_COLUMN = 5


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
    field_metadata = SchemaFieldMetadata(
        description=schema_field.description,
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
                upstreams_metadata.append(
                    UpstreamColumnMetadata(
                        upstream_column_name=urn,
                        upstream_column_description=field.description,
                        upstream_column_native_type=field.nativeDataType,
                    )
                )
                break
    return upstreams_metadata


def get_table_upstream_lineage_info(
    upstreams: List[models.UpstreamClass], graph_client: DataHubGraph
) -> List[TableUpstreamLineageInfo]:
    table_upstream_lineage_info: List[TableUpstreamLineageInfo] = []
    for upstream_idx, upstream in enumerate(upstreams):
        if upstream_idx == _MAX_UPSTREAM_TABLES:
            break
        dataset_urn = upstream.dataset
        entity = graph_client.get_entity_semityped(dataset_urn)
        (
            upstream_table_name,
            upstream_table_description,
        ) = get_table_name_and_description(entity=entity, urn=dataset_urn)
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


# def get_query_for_matching_upstream(
#     upstreams: List[models.UpstreamClass], urn: str, graph_client: DataHubGraph
# ) -> Tuple[dict, str]:
#     query_info = {}
#     lineage_type = None
#     for upstream in upstreams:
#         if upstream.dataset == urn and upstream.query is not None:
#             query_info = get_lineage_query(graph_client, upstream.query)
#             lineage_type = upstream.type
#             break
#         else:
#             continue
#     return query_info, lineage_type


def get_table_downstream_lineage_info(
    urn: str, graph_client: DataHubGraph
) -> List[TableDownstreamLineageInfo]:
    table_downstream_lineage_info: List[TableDownstreamLineageInfo] = []
    downstream_urns = get_downstream_urns_for_table(urn, graph_client)
    # TODO: should we consider non-dataset downstreams (e.g. tasks, dashboards, etc.)?
    downstream_dataset_urns = [
        urn for urn in downstream_urns if urn.startswith("urn:li:dataset:(")
    ]
    for downstream_idx, downstream_urn in enumerate(downstream_dataset_urns):
        if downstream_idx >= _MAX_DOWNSTREAM_TABLES:
            break
        entity = graph_client.get_entity_semityped(downstream_urn)
        (
            downstream_table_name,
            downstream_table_description,
        ) = get_table_name_and_description(entity=entity, urn=downstream_urn)
        lineage_info = TableDownstreamLineageInfo(
            downstream_table_urn=downstream_urn,
            downstream_table_name=downstream_table_name,
            downstream_table_description=downstream_table_description,
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
                if len(upstreams_with_metadata) == 0:
                    column_upstream = UpstreamLineageInfo(
                        lineage=lineage.upstreams,
                        transform_operation=lineage.transformOperation,
                    )
                else:
                    column_upstream = UpstreamLineageInfo(
                        lineage=upstreams_with_metadata,
                        transform_operation=lineage.transformOperation,
                    )
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

    column_descriptions = {
        make_schema_field_urn(urn, field.fieldPath): field.description
        for field in entity["schemaMetadata"].fields
    }
    if editableSchemaMetadata := entity.get("editableSchemaMetadata"):
        for efield in editableSchemaMetadata.editableSchemaFieldInfo:
            field_urn = make_schema_field_urn(urn, efield.fieldPath)
            if field_urn in column_descriptions and efield.description:
                column_descriptions[field_urn] = efield.description

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
        table_domains_info=table_domain_info,
        table_owners_info=table_owners_info,
        table_upstream_lineage_info=table_upstream_lineage_info,
        table_downstream_lineage_info=table_downstream_lineage_info,
    )
    return extracted_table_info


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


def generate_entity_descriptions_for_urn_eval(
    urn: str,
    extracted_entity_info: ExtractedTableInfo,
    prompt: str,
    model: BedrockModel | str,
) -> EntityDescriptionResult:
    table_info, column_infos = transform_table_info_for_llm(extracted_entity_info)
    if len(column_infos) > _MAX_COLUMNS:
        raise TooManyColumnsError(
            f"Too many columns ({len(column_infos)}) for urn: {urn}. "
            f"Select a table with less than {_MAX_COLUMNS} columns."
        )

    # TODO: This may use model_dump() instead of dict()
    formatted_prompt = prompt.format(
        table_info=table_info.dict(exclude_none=True),
        column_info={
            col: column_info.dict(exclude_none=True)
            for col, column_info in column_infos.items()
        },
    )

    entity_descriptions = call_bedrock_llm(
        formatted_prompt,
        max_tokens=5000,
        model=model,
    )

    table_description, column_descriptions, failure_reason = parse_llm_output(
        entity_descriptions
    )
    return EntityDescriptionResult(
        table_description=table_description,
        column_descriptions=column_descriptions,
        extracted_entity_info=extracted_entity_info,
        raw_llm_output=entity_descriptions,
        failure_reason=failure_reason,
    )


def generate_entity_descriptions_for_urn(
    graph_client: DataHubGraph, urn: str
) -> EntityDescriptionResult:
    """
    This function also returns column_info for debugging purpose (To check the if metadata information is generated correctly) and can be removed
    """

    entity = graph_client.get_entity_semityped(urn)
    extracted_entity_info = extract_metadata_for_urn(entity, urn, graph_client)

    return generate_entity_descriptions_for_urn_eval(
        urn,
        extracted_entity_info,
        prompt=PROMPT_TEMPLATE,
        model=DESCRIPTION_GENERATION_MODEL,
    )


def parse_llm_output(
    text: str,
) -> tuple[Optional[str], Optional[Dict[str, str]], Optional[str]]:
    match = re.search(r"\{[^}]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        # dict_str_cleaned = dict_str.replace("\n", " ").strip()
        dict_str_cleaned = dict_str.strip()
        dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)
            table_description: str = extracted_dict.pop("table_description")
            table_description = table_description.strip("'\"").strip()
            return table_description, extracted_dict, None

        except (SyntaxError, ValueError) as e:
            logger.info(f"Error evaluating dictionary: {e}. Text: {text}")
            return None, None, f"Error evaluating dictionary: {e}."
    else:
        logger.info("No dictionary found in the text.")
        return None, None, "No dictionary found in the text."
