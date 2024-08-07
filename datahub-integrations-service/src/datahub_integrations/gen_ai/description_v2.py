import ast
import functools
import json
import os
import re
import time
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import boto3
import botocore.config
import datahub.metadata.schema_classes as models
from aws_assume_role_lib import aws_assume_role_lib
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import AspectBag, UpstreamClass
from datahub.metadata.urns import SchemaFieldUrn
from loguru import logger

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient


# modelId = "anthropic.claude-3-5-sonnet-20240620-v1:0"
modelId = "anthropic.claude-3-haiku-20240307-v1:0"
TABLE_UPSTREAM_MAX_COUNT = 5


@functools.cache
def get_bedrock_client() -> "BedrockRuntimeClient":
    # Set up Bedrock client - the cache decorator ensures that this is a singleton

    # Increase the read and connect timeouts, since Bedrock can be slow.
    config = botocore.config.Config(read_timeout=300, connect_timeout=60)

    if "BEDROCK_AWS_ROLE" in os.environ:
        # When using assume_role(), the tokens from the assume role call expire
        # after an hour. The boto3 library doesn't have anything out of the
        # box to support this if you're not using profiles with role chaining,
        # which is the case for EKS and instance credentials.
        # The AWS team released the `aws-assume-role-lib` library to bridge
        # this gap while they figure out how to add this to boto3.
        # See https://github.com/boto/boto3/issues/443#issuecomment-1574257880

        base_session = boto3.Session()

        boto3_session = aws_assume_role_lib.assume_role(
            base_session, os.environ["BEDROCK_AWS_ROLE"]
        )

    else:
        boto3_session = boto3.Session(
            aws_access_key_id=os.environ["BEDROCK_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["BEDROCK_AWS_SECRET_ACCESS_KEY"],
        )

    return boto3_session.client("bedrock-runtime", config=config)  # type: ignore


def call_bedrock_llm(prompt: str, max_tokens: int) -> str:
    start_time = time.time()
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }
    accept = "application/json"
    contentType = "application/json"

    boto3_bedrock = get_bedrock_client()
    response = boto3_bedrock.invoke_model(
        body=json.dumps(body), modelId=modelId, accept=accept, contentType=contentType
    )
    response_body = json.loads(response["body"].read())

    # print(response_body)
    outputText = response_body["content"][0]["text"]

    logger.debug(f"LLM call took {time.time() - start_time} seconds")
    return outputText


def get_lineage_query(graph_client: DataHubGraph, urn: str) -> dict:
    entity = graph_client.get_entity_semityped(urn)
    assert "queryProperties" in entity, "Query properties not found on the query entity"
    query = entity["queryProperties"].statement.value
    language = entity["queryProperties"].statement.language
    return {"value": query, "language": language}


def remove_nulls(obj: dict) -> dict:
    return {k: v for k, v in obj.items() if v is not None}


def filter_schema_fields(schema_field: models.SchemaFieldClass) -> dict:
    filtered_schema_metadata = {
        "description": schema_field.description,
        "created": schema_field.created,
        "nativeDataType": schema_field.nativeDataType,
        **(
            {"isPartOfKey": schema_field.isPartOfKey}
            if schema_field.isPartOfKey
            else {}
        ),
        **(
            {"isPartitioningKey": schema_field.isPartitioningKey}
            if schema_field.isPartitioningKey
            else {}
        ),
    }
    return remove_nulls(filtered_schema_metadata)


def get_column_upstream_metadata(
    graph_client: DataHubGraph, upstreams: List[str]
) -> List[Dict[str, str | None]]:
    upstreams_metadata = []
    for urn in upstreams:
        schema_field_urn = SchemaFieldUrn.from_string(urn)
        entity = graph_client.get_entity_semityped(schema_field_urn.parent)
        column_name = schema_field_urn.field_path
        schema_metadata = entity.get("schemaMetadata")
        if schema_metadata is None:
            continue
        for field in schema_metadata.fields:
            if field.fieldPath == column_name:
                upstreams_metadata.append(
                    {
                        "upstream_column_name": urn,
                        "upstream_column_description": field.description,
                        "upstream_column_native_type": field.nativeDataType,
                    }
                )
    return upstreams_metadata


def get_table_upstream_lineage_info(
    upstreams: List[UpstreamClass], graph_client: DataHubGraph
):
    table_upstream_lineage_info = []
    for upstream_table_count, upstream in enumerate(upstreams):
        if upstream_table_count == TABLE_UPSTREAM_MAX_COUNT:
            break
        dataset_urn = upstream.dataset
        entity = graph_client.get_entity_semityped(dataset_urn)
        upstream_table_name, upstream_table_description = (
            get_table_name_and_description(entity=entity, urn=dataset_urn)
        )
        if upstream.query is not None:
            query = get_lineage_query(graph_client, upstream.query)
        else:
            query = {}
        lineage_type = upstream.type
        table_upstream_lineage_info.append(
            {
                "upstream_table_urn": dataset_urn,
                "upstream_table_name": upstream_table_name,
                "upstream_table_description": upstream_table_description,
                "query": query,
                "lineage_type": lineage_type,
            }
        )
    return table_upstream_lineage_info


def get_upstream_finegrained_lineage_info(
    column_urns: list[str],
    finegrained_lineages: List[models.FineGrainedLineageClass],
    graph_client: DataHubGraph,
) -> dict[str, list[dict[str, Any]]]:
    lineage_info: dict[str, list[dict[str, str | None]]] = {}
    for lineage in finegrained_lineages:
        downstreams = lineage.downstreams
        for downstream in downstreams or []:
            column_urn = downstream
            if column_urn in column_urns:
                # Queries are included in the table lineage object.
                # if lineage.query is not None:
                #     query = get_lineage_query(graph_client, lineage.query)
                # else:
                #     query = {}
                upstreams = lineage.upstreams
                if not upstreams:
                    continue
                upstreams_with_metadata = get_column_upstream_metadata(
                    graph_client, upstreams
                )
                if upstreams_with_metadata == []:
                    upstream_dict: dict = {
                        "lineage": lineage.upstreams,
                        # "query": query,
                        "transform_operation": lineage.transformOperation,
                    }
                else:
                    upstream_dict = {
                        "lineage": upstreams_with_metadata,
                        # "query": query,
                        "transform_operation": lineage.transformOperation,
                    }
                upstream_dict = remove_nulls(upstream_dict)
                if column_urn in lineage_info.keys():
                    lineage_info[column_urn].extend(upstream_dict)
                else:
                    lineage_info[column_urn] = [upstream_dict]
    for column_urn in column_urns:
        if column_urn not in lineage_info.keys():
            lineage_info[column_urn] = []
    return lineage_info


def get_sample_values(urn, graph_client):
    sample_values = {}
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
                sample_values[f"urn:li:schemaField:({urn},{field.fieldPath})"] = (
                    field.sampleValues
                )
    return sample_values


def get_table_name_and_description(entity, urn):
    dataset_properties = entity.get("datasetPoperties")
    if dataset_properties is None:
        dataset_key = entity.get("datasetKey")
        if dataset_key is None or dataset_key.name in [None, ""]:
            dataset_name = urn.split(",")[-2]
            dataset_description = ""
        else:
            dataset_name = dataset_key.name.split(".")[-1]
            dataset_description = ""
    else:
        dataset_name, dataset_description = (
            dataset_properties.name,
            dataset_properties.description,
        )
    return dataset_name, dataset_description


def get_table_domain_info(domain_urns, graph_client):
    domain_info = []
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
            {"domain_name": domain_name, "domain_description": domain_description}
        )
    return domain_info


def get_ownership_info(owners: List[models.OwnerClass]) -> list[dict]:
    owners_info = []
    for owner in owners:
        owner_name = owner.owner.split(":")[-1]
        owner_type = owner.type
        owners_info.append({"owner_name": owner_name, "owner_type": owner_type})
    return owners_info


def get_tag_names_and_description(
    tags: models.GlobalTagsClass, graph_client: DataHubGraph
):
    tag_info = []
    for tag in tags.tags:
        tag_urn = tag.tag
        tag_details = graph_client.get_entity_semityped(tag_urn).get("tagProperties")
        if tag_details is not None:
            tag_name = tag_details.name
            tag_desc = tag_details.description
        else:
            tag_name = tag_urn.rsplit("urn:li:tag:")[-1]
            tag_desc = ""
        tag_info.append({"tag_name": tag_name, "tag_description": tag_desc})
    return tag_info


def get_table_and_column_level_tags(
    entity: AspectBag, urn: str, graph_client: DataHubGraph
):
    # Table level tags:
    global_tags = entity.get("globalTags")
    if global_tags:
        table_level_tags = get_tag_names_and_description(global_tags, graph_client)
    else:
        table_level_tags = []

    # Column Level Tags:
    column_level_tags = {}
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
):
    term_info = []
    for term in terms:
        term_urn = term.urn
        term_details = graph_client.get_entity_semityped(term_urn).get(
            "glossaryTermInfo"
        )
        if term_details is not None:
            term_name = term_details.name
            term_def = term_details.definition
        else:
            continue
        term_info.append({"term_name": term_name, "term_definition": term_def})
    return term_info


def get_table_and_column_level_glossary_terms(
    entity: AspectBag, urn: str, graph_client: DataHubGraph
):
    # Table level terms:
    global_terms = entity.get("glossaryTerms")
    if global_terms is not None:
        table_level_terms = get_glossary_term_names_and_definition(
            global_terms.terms, graph_client
        )
    else:
        table_level_terms = []

    # Column Level Terms:
    column_level_terms = {}
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


def extract_metadata_for_urn(
    entity: AspectBag, urn: str, graph_client: DataHubGraph
) -> Dict[str, dict]:
    assert "schemaMetadata" in entity, "Schema metadata not found in the entity"
    column_metadata = {
        f"urn:li:schemaField:({urn},{field.fieldPath})": filter_schema_fields(field)
        for field in entity["schemaMetadata"].fields
    }
    column_descriptions = {
        f"urn:li:schemaField:({urn},{field.fieldPath})": field.description
        for field in entity["schemaMetadata"].fields
    }

    # Upstream Lineage Information
    upstream_lineages = entity.get("upstreamLineage")
    if upstream_lineages is None or upstream_lineages.fineGrainedLineages is None:
        upstream_lineage_info: dict[str, list] = {
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

    # TODO: Downstream lineage information

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
    table_view_properties = entity.get("viewProperties")

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

    # Column field paths:
    column_names = {
        column: column.rsplit(",")[-1][:-1] for column in column_metadata.keys()
    }

    # Column Sample Values:
    column_sample_values = get_sample_values(urn=urn, graph_client=graph_client)

    extracted_table_info = {
        "column_names": column_names,
        "column_metadata": column_metadata,
        "column_descriptions": column_descriptions,
        "column_upstream_lineages": upstream_lineage_info,
        "column_sample_values": column_sample_values,
        "column_tags": column_level_tags,
        "column_glossary_terms": column_level_terms,
        "table_tags": table_level_tags,
        "table_glossary_terms": table_level_terms,
        "table_view_properties": table_view_properties,
        "table_name": table_name,
        "table_description": table_description,
        "table_domains_info": table_domain_info,
        "table_owners_info": table_owners_info,
        "table_upstream_lineage_info": table_upstream_lineage_info,
    }
    return extracted_table_info


def transform_table_info_for_llm(extracted_table_info):
    column_level_keys = [key for key in extracted_table_info.keys() if "column" in key]
    table_level_keys = [key for key in extracted_table_info.keys() if "table" in key]
    column_urns = extracted_table_info.get("column_names").keys()
    column_info: dict[str, dict] = {}
    for column in column_urns:
        column_name = SchemaFieldUrn.from_string(column).field_path
        column_info[column_name] = {}
        for key in column_level_keys:
            value = extracted_table_info.get(key)
            if value is None or value.get(column) is None:
                continue
            else:
                new_key = key.replace("column_", "")
                if new_key == "names":
                    new_key = "column_name"
                column_info[column_name][new_key] = value[column]
    table_info = {
        key.replace("table_", ""): extracted_table_info[key] for key in table_level_keys
    }
    return table_info, column_info


def generate_entity_descriptions_for_urn(
    graph_client: DataHubGraph, urn: str, model_config: dict = {}
) -> Tuple[str, dict]:
    """
    This function also returns column_info for debugging purpose (To check the if metadata information is generated correctly) and can be removed
    """

    # try:
    entity = graph_client.get_entity_semityped(urn)
    extracted_entity_info = extract_metadata_for_urn(entity, urn, graph_client)
    table_info, column_info = transform_table_info_for_llm(extracted_entity_info)
    prompt = f'''\
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
   c) A summary of the downstream tables (consumers) and general use cases for the table. Only include information that can be substantiated by the provided metadata.
   d) Technical notes and usage tips, including the table type (fact or dimension) and grain if available.
   e) A note on whether the table directly contains any PII data. Do not provide recommendations related to access control, monitoring, or governance.

   Format any references to other entities as markdown links, using the entity URN as the link. For example: [table_name](urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD))
   Use Markdown section as appropriate.

2. Column Descriptions:
   For each column, create a concise description of one or two sentences. Prefer elliptical sentences that are direct and to the point. If available, include details about how the column was generated or calculated.

When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak words like "suggests" or "could be". Only include information you are confident about based on the provided metadata.
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
    # print(prompt)
    entity_descriptions = call_bedrock_llm(
        prompt, max_tokens=model_config.get("max_tokens", 3000)
    )
    # except Exception as e:
    #     print(f"Exception occured while generating column descriptions for urn: {urn} {e}")
    #     return None
    return entity_descriptions, extracted_entity_info


def parse_llm_output(text: str) -> tuple[str | None, Dict[str, str] | None]:
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
            return table_description, extracted_dict

        except (SyntaxError, ValueError) as e:
            logger.info(f"Error evaluating dictionary: {e}. Text: {text}")
            return None, None
    else:
        logger.info("No dictionary found in the text.")
        return None, None
