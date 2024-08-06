import ast
import functools
import json
import os
import re
import time
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import boto3
import datahub.metadata.schema_classes as models
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import AspectBag
from datahub.metadata.urns import SchemaFieldUrn
from loguru import logger

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient


# modelId = "anthropic.claude-3-sonnet-20240229-v1:0"
modelId = "anthropic.claude-3-haiku-20240307-v1:0"


@functools.cache
def get_bedrock_client() -> "BedrockRuntimeClient":
    # Set up Bedrock client

    if "BEDROCK_AWS_ROLE" in os.environ:
        sts_client = boto3.client("sts")
        response = sts_client.assume_role(
            RoleArn=os.environ["BEDROCK_AWS_ROLE"], RoleSessionName="bedrock-client"
        )
        credentials = response["Credentials"]
        boto3_session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

    else:
        boto3_session = boto3.Session(
            aws_access_key_id=os.environ["BEDROCK_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["BEDROCK_AWS_SECRET_ACCESS_KEY"],
        )

    return boto3_session.client("bedrock-runtime")  # type: ignore


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
        # "max_tokens_to_sample": 300,
        # "temperature": 0.5,
        # "top_k": 250,
        # "top_p": 1,
    }
    accept = "application/json"
    contentType = "application/json"

    boto3_bedrock = get_bedrock_client()
    response = boto3_bedrock.invoke_model(
        body=json.dumps(body), modelId=modelId, accept=accept, contentType=contentType
    )
    response_body = json.loads(response.get("body").read())

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


def filter_schema_fields(schema_field, urn):
    filtered_schema_metadata = {
        "description": schema_field.description,
        "created": schema_field.created,
        "nativeDataType": schema_field.nativeDataType,
        "isPartOfKey": schema_field.isPartOfKey,
        "isPartitioningKey": schema_field.isPartitioningKey,
    }
    return filtered_schema_metadata


def get_upstream_metadata(
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


def get_upstream_lineage_info(
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
                if lineage.query is not None:
                    query = get_lineage_query(graph_client, lineage.query)
                else:
                    query = {}
                upstreams = lineage.upstreams
                if not upstreams:
                    continue
                upstreams_with_metadata = get_upstream_metadata(graph_client, upstreams)
                if upstreams_with_metadata == []:
                    upstream_dict: dict = {
                        "lineage": lineage.upstreams,
                        "query": query,
                        "transform_operation": lineage.transformOperation,
                    }
                else:
                    upstream_dict = {
                        "lineage": upstreams_with_metadata,
                        "query": query,
                        "transform_operation": lineage.transformOperation,
                    }
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


def get_table_name_and_description(entity, graph_client, urn):
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
        f"urn:li:schemaField:({urn},{field.fieldPath})": filter_schema_fields(
            field, urn
        )
        for field in entity["schemaMetadata"].fields
    }
    column_types = {
        f"urn:li:schemaField:({urn},{field.fieldPath})": field.nativeDataType
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
        upstream_lineage_info = get_upstream_lineage_info(
            column_urns=list(column_metadata.keys()),
            finegrained_lineages=finegrained_lineages,
            graph_client=graph_client,
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
        entity=entity, graph_client=graph_client, urn=urn
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

    column_info = {
        "column_metadata": column_metadata,
        "column_types": column_types,
        "column_descriptions": column_descriptions,
        "column_upstream_lineages": upstream_lineage_info,
        "column_names": column_names,
        "column_sample_values": column_sample_values,
        "column_level_tags": column_level_tags,
        "column_level_glossary_terms": column_level_terms,
        "table_level_tags": table_level_tags,
        "table_level_glossary_terms": table_level_terms,
        "table_view_properties": table_view_properties,
        "table_name": table_name,
        "table_description": table_description,
        "table_domains_info": table_domain_info,
        "table_owners_info": table_owners_info,
    }
    return column_info


def generate_column_descriptions_for_urn(
    graph_client: DataHubGraph, urn: str, model_config: dict = {}
) -> Tuple[str, dict]:
    """
    This function also returns column_info for debugging purpose (To check the if metadata information is generated correctly) and can be removed
    """

    # try:
    entity = graph_client.get_entity_semityped(urn)
    column_info = extract_metadata_for_urn(entity, urn, graph_client)
    prompt = f"""\
Provided a datahub column and table details, generate a concise description for the table in one or two paragraphs and also for each field/column of the table in a sentence or a paragraph for each column without fail, make sure that in the description of each column the information given below like 'tags', 'glossary terms', 'upstream lineages', 'types' etc are also used if relevant. While generating output, strictly adhere to the Response output structure mentioned in below details.
Include the table description in the same output dictionary where key for table description is "table_description"
Table Level Information to include in the table description :
    Table_Name: {column_info['table_name']}
    Table_Description: {column_info['table_description']}
    Table_Tags: {column_info['table_level_tags']}
    Table_Glossary_Terms: {column_info['table_level_glossary_terms']}
    Table_View_Information: {column_info['table_view_properties']}
    Table_Owner_Information: {column_info['table_owners_info']}
    Table_Domains_Information: {column_info['table_domains_info']}

Column Level Information to include in the column descriptions:
    Column urns: {column_info["column_metadata"].keys()}
    Schema_Information: {column_info["column_metadata"]}
    Column_Display_Names: {column_info["column_names"]}
    Column_Types: {column_info["column_types"]}
    Column_Descriptions: {column_info["column_descriptions"]}
    Column_Upstream_Lineage_Information: {column_info["column_upstream_lineages"]}
    Column_Tags: {column_info['column_level_tags']}
    Column_Glossary_Terms: {column_info['column_level_glossary_terms']}
    Column_Sample_Values : {column_info['column_sample_values']}


Response output structure: {Dict[str, str]} Create a proper parsable dictionary.
keys: Column_Display_Names.
values: inferred description of the column.

Sample_Output: {{
    "table_description": "detailed summary of the table.",
    "column_displayname1": "summary.",
    "column_displayname2": "summary",
    "column_displayname3": "summary",
    "column_displayname4": "summary",
    "column_displayname5": "summary",
    "column_displayname6": "summary",
}}
    """
    # print(prompt)
    column_descriptions = call_bedrock_llm(
        prompt, max_tokens=model_config.get("max_tokens", 1500)
    )
    # except Exception as e:
    #     print(f"Exception occured while generating column descriptions for urn: {urn} {e}")
    #     return None
    return column_descriptions, column_info


def parse_llm_output(text: str) -> tuple[str | None, Dict[str, str] | None]:
    match = re.search(r"\{[^}]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        dict_str_cleaned = dict_str.replace("\n", " ").strip()
        dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)

            table_description = extracted_dict.pop("table_description")
            return table_description, extracted_dict

        except (SyntaxError, ValueError) as e:
            logger.info("Error evaluating dictionary:", e)
            return None, None
    else:
        logger.info("No dictionary found in the text.")
        return None, None
