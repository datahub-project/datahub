from datahub_integrations.gen_ai.mlflow_init import (  # noqa: F401
    MLFLOW_ENABLED,
    MLFLOW_INITIALIZED,
)

import json
from typing import Dict, List, Optional, Tuple

import asyncer
import mlflow
import more_itertools
import tenacity
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.gen_ai.bedrock import (
    BedrockModel,
    call_bedrock_llm_with_retry,
    get_bedrock_model_env_variable,
)
from datahub_integrations.gen_ai.description_context import (
    ColumnMetadataInfo,
    DescriptionParsingError,
    EntityDescriptionResult,
    ExtractedTableInfo,
    TableInfo,
    TooManyColumnsError,
    extract_metadata_for_urn,
    parse_llm_output,
    transform_table_info_for_llm,
)

_MAX_COLUMNS = 1000
_MAX_COLUMNS_PER_BATCH = 50

# TODO: adaptive batching for schema field v2 paths OR preprocessing them to covert to v1 paths
# TODO: Add retry on parsing failure of the output - less likely to happen with v3 - need to track


def split_columns_into_batch(columns: List[str], batch_size: int) -> List[List[str]]:
    """
    Split columns into batches of specified size.

    Args:
        columns: List of column names
        batch_size: Maximum number of columns per batch

    Returns:
        List of dictionaries, each containing a batch of column metadata

    Note:
        Current implementation uses simple sequential splitting. Future versions could implement
        more sophisticated batching strategies such as:
        - Grouping related columns together (e.g., all columns belonging to particular struct, all date columns in same batch)

    """

    return list(more_itertools.chunked_even(columns, batch_size))


TABLE_DESC_PROMPT = '''\
You are tasked with generating concise description for a DataHub table based on provided metadata. Here is the information you will be working with:

<table_info>
{table_info}
</table_info>

<column_info>
{column_info}
</column_info>

Generate the description as follows:

   Create a few paragraphs of Markdown-formatted text that includes:
   a) A summary of the primary purpose and business importance of the table.
   b) If metadata is available, a summary of the upstream tables and transformations applied.
   c) A summary of the downstream tables (consumers) and general use cases for the table. Only include information that can be substantiated by the provided table_info.
   d) Technical notes and usage tips, including the table type (fact or dimension) and grain if available.
   e) A note on whether the table directly contains any PII data, like names, emails, and addresses. Do not provide recommendations related to access control, monitoring, or governance.

   Format any references to other entities as markdown links, using the entity URN as the link. For example: [table_name](urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD))
   Use Markdown sections like H2 and H3 with appropriate section titles. The first line should be "# <table name>", followed by a blank line.

When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak phrases like "suggests", "could be", "likely", or "is considered". Only include information you are confident about based on the provided metadata.
- Be concise and to the point.

Provide your output in the following dictionary format:

{{
    "table_description": """
[Your multi-line table description here]
"""
}}

Ensure that the dictionary is properly formatted and parsable. Include the table description with the key "table_description".\
'''


COLUMN_DESC_PROMPT = """\
You are tasked with generating concise description for a columns of a DataHub table based on provided metadata. Here is the information you will be working with:

<table_info>
{table_info}
</table_info>

<column_info>
{column_info}
</column_info>

<table_description>
{table_description}
</table_description>

Generate the description as follows:

  For each column, create a concise description of one or two sentences. Prefer elliptical sentences that are direct and to the point. If available, include details about how the column was generated or calculated.

{column_batch_instructions}

When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak phrases like "suggests", "could be", "likely", or "is considered". Only include information you are confident about based on the provided metadata.
- Be concise and to the point.

Provide your output in the following dictionary format:

{{
    "column_name1": "Column description",
    "column_name2": "Column description",
    ...
}}

Ensure that the dictionary is properly formatted and parsable. Use the column display names as keys for the column descriptions.\
"""

COLUMN_BATCH_INSTRUCTIONS = """\
Generate descriptions for only the following {num_columns} columns:
{columns}
"""

CURRENT_MODEL: BedrockModel | str = get_bedrock_model_env_variable(
    "DESCRIPTION_GENERATION_BEDROCK_MODEL", BedrockModel.CLAUDE_3_HAIKU
)


def generate_entity_descriptions_for_urn(
    graph_client: DataHubGraph, urn: str
) -> EntityDescriptionResult:
    """
    This function also returns column_info for debugging purpose (To check the if metadata information is generated correctly) and can be removed
    """

    entity = graph_client.get_entity_semityped(urn)
    extracted_entity_info = extract_metadata_for_urn(entity, urn, graph_client)

    return generate_entity_descriptions_for_urn_eval_v3(
        urn,
        extracted_entity_info,
    )


def generate_entity_descriptions_for_urn_eval_v3(
    urn: str,
    extracted_entity_info: ExtractedTableInfo,
) -> EntityDescriptionResult:
    table_info, column_infos = transform_table_info_for_llm(extracted_entity_info)
    if len(column_infos) > _MAX_COLUMNS:
        raise TooManyColumnsError(
            f"Too many columns ({len(column_infos)}) for urn: {urn}. "
            f"Select a table with less than {_MAX_COLUMNS} columns."
        )
    table_description = None
    failure_reason = None
    column_descriptions = None

    try:
        table_description = generate_table_description(table_info, column_infos)
        if table_description is not None:
            column_descriptions, failure_reason_columns = (
                generate_all_columns_description(
                    table_info, column_infos, table_description
                )
            )

            if failure_reason_columns is not None:
                failure_reason = failure_reason_columns

    except Exception as e:
        logger.error(f"Error generating entity descriptions for urn: {urn}. Error: {e}")
        failure_reason = str(e)

    return EntityDescriptionResult(
        table_description=table_description,
        column_descriptions=column_descriptions,
        extracted_entity_info=extracted_entity_info,
        failure_reason=failure_reason,
    )


@mlflow.trace(name="generate_all_columns_description", span_type="function")
def generate_all_columns_description(
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    table_description: str,
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    column_descriptions, failure_reason_columns = asyncer.syncify(
        generate_column_descriptions, raise_sync_error=False
    )(table_info, column_infos, table_description)

    return column_descriptions, failure_reason_columns


_MAX_ATTEMPTS = 3  # Original attempt + 2 retries


@tenacity.retry(
    stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
    retry=tenacity.retry_if_exception_type(DescriptionParsingError),
    before_sleep=lambda retry_state: logger.info(
        f"Retry table description generation attempt {retry_state.attempt_number} of {_MAX_ATTEMPTS - 1}"
    ),
)
def generate_table_description(
    table_info: TableInfo, column_infos: Dict[str, ColumnMetadataInfo]
) -> Optional[str]:
    formatted_prompt = TABLE_DESC_PROMPT.format(
        table_info=table_info.dict(exclude_none=True),
        column_info={
            col: column_info.dict(exclude_none=True)
            for col, column_info in column_infos.items()
        },
    )

    entity_descriptions = call_bedrock_llm_with_retry(
        formatted_prompt,
        max_tokens=4096,
        model=CURRENT_MODEL,
    )

    table_description = parse_llm_output(entity_descriptions)

    return table_description


async def generate_column_descriptions(
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    generated_table_description: str,
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    batch_failure_reason = None
    all_column_names = list(column_infos.keys())
    column_splits = split_columns_into_batch(all_column_names, _MAX_COLUMNS_PER_BATCH)
    # NOTE: We can do further experimentation here.
    # We can pass previous batch's descriptions to the next batch to improve the quality of the descriptions.

    generated_batch_column_descriptions: List[
        asyncer.SoonValue[Tuple[Optional[str], Dict[str, str]]]
    ] = []
    async with asyncer.create_task_group() as task_group:
        for columns_batch in column_splits:
            if len(column_splits) == 1:
                column_batch_instructions = ""
            else:
                column_batch_instructions = COLUMN_BATCH_INSTRUCTIONS.format(
                    num_columns=len(columns_batch),
                    columns=json.dumps(columns_batch),
                )

            generated_batch_column_descriptions.append(
                task_group.soonify(
                    asyncer.asyncify(generate_column_batch_descriptions)
                )(
                    table_info,
                    column_infos,
                    generated_table_description,
                    column_batch_instructions,
                )
            )

    all_column_descriptions = {}
    batch_failure_reason = ""
    for i, result in enumerate(generated_batch_column_descriptions):
        batch_failure_reason, batch_column_descriptions = result.value
        if batch_failure_reason is not None:
            batch_failure_reason += f"Error in batch {i}: {batch_failure_reason}\n"
        else:
            assert batch_column_descriptions is not None
            all_column_descriptions.update(batch_column_descriptions)

    return all_column_descriptions, (
        batch_failure_reason if batch_failure_reason != "" else None
    )


@tenacity.retry(
    stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
    retry=tenacity.retry_if_result(lambda x: x[1] is None),
    before_sleep=lambda retry_state: logger.info(
        f"Retry column batch description generation attempt {retry_state.attempt_number} of {_MAX_ATTEMPTS - 1}"
    ),
)
@mlflow.trace(name="generate_column_batch_descriptions", span_type="function")
def generate_column_batch_descriptions(
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    generated_table_description: str,
    column_batch_instructions: str,
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    formatted_prompt = COLUMN_DESC_PROMPT.format(
        table_info=table_info.dict(exclude_none=True),
        column_info={
            col: column_info.dict(exclude_none=True)
            for col, column_info in column_infos.items()
        },
        table_description=generated_table_description,
        column_batch_instructions=column_batch_instructions,
    )
    try:
        column_descriptions_raw = call_bedrock_llm_with_retry(
            formatted_prompt,
            max_tokens=5000,
            model=CURRENT_MODEL,
        )

        batch_column_descriptions, batch_failure_reason = parse_column_descriptions(
            column_descriptions_raw
        )
    except Exception as e:
        # As we do not wish to fail entire description generation for single column batch
        # we do not raise error here, instead retry for batch_column_descriptions is None
        logger.error(f"Error generating column descriptions for batch: {e}")
        batch_failure_reason = str(e)
        batch_column_descriptions = None

    return batch_failure_reason, batch_column_descriptions


def parse_column_descriptions(
    column_descriptions_raw: str,
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    try:
        return json.loads(column_descriptions_raw), None
    except json.JSONDecodeError as e:
        raise DescriptionParsingError(f"Error parsing column descriptions: {e}") from e
