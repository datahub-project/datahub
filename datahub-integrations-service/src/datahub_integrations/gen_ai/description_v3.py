from datahub_integrations.gen_ai.mlflow_init import initialize_mlflow

import json
import os
from typing import Dict, List, Optional, Tuple

import asyncer
import cachetools
import mlflow
import more_itertools
import tenacity
from anyio import to_thread
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path
from loguru import logger

from datahub_integrations.gen_ai.description_context import (
    ColumnMetadataInfo,
    DescriptionParsingError,
    EntityDescriptionResult,
    ExtractedTableInfo,
    TableInfo,
    TooManyColumnsError,
    extract_metadata_for_urn,
    parse_columns_llm_output,
    parse_table_desc_llm_output,
    transform_table_info_for_llm,
)
from datahub_integrations.gen_ai.linkify import auto_fix_entity_mention_links
from datahub_integrations.gen_ai.litellm import (
    LiteLLM,
    LiteLLMPromptMessage,
)
from datahub_integrations.gen_ai.model_config import model_config

# Initialize MLflow for @mlflow.trace decorators in this module
initialize_mlflow()

_MAX_COLUMNS = int(os.getenv("DESCRIPTION_GENERATION_MAX_COLUMNS", 3000))
MAX_COLUMNS_PER_BATCH = int(
    os.getenv("DESCRIPTION_GENERATION_MAX_COLUMNS_PER_BATCH", 30)
)
ANYIO_THREAD_COUNT = 100
LARGE_TABLE_THRESHOLD = 100

DEFAULT_TEMPERATURE = 0.3
DEFAULT_MAX_TOKENS = 4096


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


PROMPT_COMMON_CONTEXT = """\
You are tasked with generating concise description for a DataHub table and its columns based on provided metadata. Here is the information you will be working with:

<table_info>
{table_info}
</table_info>

"""
PROMPT_COLUMNS_CONTEXT = """\
<column_info>
{column_info}
</column_info>

"""

TABLE_DESC_PROMPT = """\
Generate the description as follows:

   Create a few paragraphs of Markdown-formatted text that includes:
   a) A summary of the primary purpose and business importance of the table.
   b) If metadata is available, a summary of the upstream tables and transformations applied.
   c) A summary of the downstream tables (consumers) and general use cases for the table. Only include information that can be substantiated by the provided table_info.
   d) Technical notes and usage tips, including the table type and grain if available.
   e) A note on whether the table directly contains any PII data, like names, emails, and addresses. Do not provide any recommendations related to access control, monitoring, or governance.

   Format any references to other entities as markdown links, using the entity URN as the link. For example: [@table_name](urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD))
   Use Markdown sections like H4 and H5 with appropriate section titles. The first line should be "### <table name>", followed by a blank line.

When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak phrases like "suggests", "could be", "likely", or "is considered". Only include information you are confident about based on the provided metadata.
- Aim for self-sufficient description, avoid phrases like "based on the provided metadata", "table does not have documented upstream".
- Be concise and to the point.

Provide your output in the markdown format:

### <table name>

[Your multi-line table description here]


Ensure that the markdown is properly formatted.\
"""


COLUMN_DESC_PROMPT = """\
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

Ensure that the dictionary is properly formatted and parsable. Use the column names as keys for the column descriptions.\
"""

COLUMN_BATCH_INSTRUCTIONS = """\
Generate descriptions for only the following {num_columns} columns:
{columns}
"""


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=60 * 5))
def get_extra_documentation_instructions(graph: DataHubGraph) -> Optional[str]:
    """
    Retrieve optional documentation AI instructions from GraphQL API.

    Instructions are cached for 5 minutes via TTLCache decorator.
    Fetches the most recent ACTIVE GENERAL_CONTEXT instruction from global documentation AI settings.

    Args:
        graph: DataHub graph client to use for GraphQL queries

    Returns:
        Optional extra instructions string, or None if not configured.

    Raises:
        Exception: If GraphQL call fails
    """
    query = """
    query getGlobalSettings {
        globalSettings {
            documentationAi {
                instructions {
                    id
                    type
                    state
                    instruction
                    lastModified {
                        time
                        actor
                    }
                }
            }
        }
    }
    """

    try:
        response = graph.execute_graphql(query)

        # Extract documentation AI instructions
        global_settings = response.get("globalSettings")
        if not global_settings:
            return None

        documentation_ai = global_settings.get("documentationAi")
        if not documentation_ai:
            return None

        instructions = documentation_ai.get("instructions", [])

        # Filter for GENERAL_CONTEXT and ACTIVE instructions
        valid_instructions = [
            instr
            for instr in instructions
            if (
                instr.get("type") == "GENERAL_CONTEXT"
                and instr.get("state") == "ACTIVE"
            )
        ]

        if not valid_instructions:
            logger.info("no extra instructions found")
            return None

        logger.info("found extra instructions")
        # Use the last item from the array as the most recent instruction
        latest_instruction = valid_instructions[-1]
        instruction_text = latest_instruction.get("instruction")

        if instruction_text and instruction_text.strip():
            return instruction_text.strip()
        else:
            return None

    except Exception as e:
        # GraphQL call failed - re-raise the exception as per requirements
        raise Exception(
            f"Failed to fetch documentation AI instructions from GraphQL: {str(e)}"
        ) from e


class FieldPathProcessor:
    """Handles conversion between v1 and v2 field paths for column metadata."""

    def __init__(self, column_infos: Dict[str, ColumnMetadataInfo]):
        self.original_column_infos = column_infos
        self.v1_column_infos: Dict[str, ColumnMetadataInfo] = {}
        self.v1_to_v2_mapping: Dict[str, str] = {}
        self.conversion_successful = False

    def _convert_to_v1(self) -> bool:
        """
        Convert v2 field paths to v1 field paths.

        Returns:
            True if conversion was successful, False otherwise
        """
        try:
            for v2_field_path, column_info in self.original_column_infos.items():
                v1_field_path = get_simple_field_path_from_v2_field_path(v2_field_path)

                # Check for collisions
                if v1_field_path in self.v1_to_v2_mapping:
                    logger.warning(
                        f"Multiple v2 field paths map to same v1 path '{v1_field_path}': "
                        f"'{self.v1_to_v2_mapping[v1_field_path]}' and '{v2_field_path}'. "
                        f"Using original v2 paths."
                    )
                    return False

                self.v1_to_v2_mapping[v1_field_path] = v2_field_path
                self.v1_column_infos[v1_field_path] = column_info.model_copy(
                    update={"column_name": v1_field_path}
                )

            self.conversion_successful = True
            return True

        except Exception as e:
            logger.warning(
                f"Failed to convert v2 field paths to v1: {e}. Using original v2 paths."
            )
            return False

    def simplify(self) -> Dict[str, ColumnMetadataInfo]:
        """
        Attempt to convert v2 field paths to v1 and return the appropriate column infos.

        Returns:
            Column infos with v1 field paths if conversion successful, otherwise original v2 paths
        """
        self._convert_to_v1()
        return (
            self.v1_column_infos
            if self.conversion_successful
            else self.original_column_infos
        )

    def restore_v2_paths(self, column_descriptions: Dict[str, str]) -> Dict[str, str]:
        """
        Convert v1 field paths back to v2 field paths in column descriptions.

        Args:
            column_descriptions: Dictionary with field paths as keys and descriptions as values

        Returns:
            Dictionary with v2 field paths as keys if conversion was successful, otherwise unchanged
        """
        if not self.conversion_successful:
            return column_descriptions

        v2_column_descriptions = {}
        for v1_field_path, description in column_descriptions.items():
            v2_field_path = self.v1_to_v2_mapping.get(v1_field_path)
            if v2_field_path is not None:
                v2_column_descriptions[v2_field_path] = description
            else:
                # This shouldn't happen if conversion was successful
                logger.warning(
                    f"No v2 mapping found for v1 field path: {v1_field_path}"
                )
                v2_column_descriptions[v1_field_path] = description

        return v2_column_descriptions


def generate_entity_descriptions_for_urn(
    graph_client: DataHubGraph, urn: str
) -> EntityDescriptionResult:
    """
    This function also returns column_info for debugging purpose (To check the if metadata information is generated correctly) and can be removed
    """

    entity = graph_client.get_entity_semityped(urn)
    extracted_entity_info = extract_metadata_for_urn(entity, urn, graph_client)

    # Resolve extra instructions at entry point
    extra_instructions = get_extra_documentation_instructions(graph_client)

    return generate_entity_descriptions_for_urn_eval_v3(
        urn,
        extracted_entity_info,
        extra_instructions=extra_instructions,
    )


def generate_entity_descriptions_for_urn_eval_v3(
    urn: str,
    extracted_entity_info: ExtractedTableInfo,
    extra_instructions: Optional[str] = None,
) -> EntityDescriptionResult:
    table_info, column_infos = transform_table_info_for_llm(extracted_entity_info)

    if len(column_infos) > _MAX_COLUMNS:
        raise TooManyColumnsError(
            f"Too many columns ({len(column_infos)}) for urn: {urn}. "
            f"Select a table with less than {_MAX_COLUMNS} columns."
        )

    # Handle field path conversion
    processor = FieldPathProcessor(column_infos)
    simplified_column_infos = processor.simplify()

    table_description = None
    failure_reason = None
    column_descriptions = None

    litellm = LiteLLM(
        model=model_config.documentation_ai.model,
        max_tokens=DEFAULT_MAX_TOKENS,
        temperature=DEFAULT_TEMPERATURE,
    )

    try:
        table_description = generate_table_description(
            litellm, table_info, simplified_column_infos, extra_instructions
        )
        if table_description is not None:
            column_descriptions, failure_reason_columns = (
                generate_all_columns_description(
                    litellm,
                    table_info,
                    simplified_column_infos,
                    table_description,
                    extra_instructions,
                )
            )

            if failure_reason_columns is not None:
                failure_reason = failure_reason_columns

    except Exception as e:
        logger.error(f"Error generating entity descriptions for urn: {urn}. Error: {e}")
        failure_reason = str(e)

    # Convert back to v2 field paths if needed
    if column_descriptions is not None:
        column_descriptions = processor.restore_v2_paths(column_descriptions)

    return EntityDescriptionResult(
        table_description=table_description,
        column_descriptions=column_descriptions,
        extracted_entity_info=extracted_entity_info,
        failure_reason=failure_reason,
    )


@mlflow.trace(name="generate_all_columns_description", span_type="function")
def generate_all_columns_description(
    litellm: LiteLLM,
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    table_description: str,
    extra_instructions: Optional[str] = None,
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    column_descriptions, failure_reason_columns = asyncer.syncify(
        generate_column_descriptions, raise_sync_error=False
    )(litellm, table_info, column_infos, table_description, extra_instructions)

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
    litellm: LiteLLM,
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    extra_instructions: Optional[str] = None,
) -> Optional[str]:
    formatted_common_context = PROMPT_COMMON_CONTEXT.format(
        table_info=table_info.model_dump(exclude_none=True),
    )
    formatted_columns_context = PROMPT_COLUMNS_CONTEXT.format(
        column_info={
            col: (
                column_info.minimal_dict()
                if len(column_infos) > LARGE_TABLE_THRESHOLD
                else column_info.model_dump(exclude_none=True)
            )
            for col, column_info in column_infos.items()
        },
    )

    # Build the prompt messages
    messages = [
        LiteLLMPromptMessage(
            text=formatted_common_context,
            cache=True,
        ),
        LiteLLMPromptMessage(
            text=formatted_columns_context,
            cache=True,
        ),
    ]

    messages.append(
        LiteLLMPromptMessage(
            text=TABLE_DESC_PROMPT,
            cache=False,
        )
    )

    # Add extra instructions if provided
    if extra_instructions:
        messages.append(
            LiteLLMPromptMessage(
                text=f"\nADDITIONAL REQUIREMENTS:\n{extra_instructions}\n",
                cache=False,
            )
        )

    entity_descriptions = litellm.call_lite_llm(prompt=messages)

    table_description = parse_table_desc_llm_output(entity_descriptions)

    # post process table description to fix links
    table_description = auto_fix_entity_mention_links(table_description)

    return table_description


async def generate_column_descriptions(
    litellm: LiteLLM,
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    generated_table_description: str,
    extra_instructions: Optional[str] = None,
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    batch_failure_reason = None
    all_column_names = list(column_infos.keys())
    column_splits = split_columns_into_batch(all_column_names, MAX_COLUMNS_PER_BATCH)
    # NOTE: We can do further experimentation here.
    # We can pass previous batch's descriptions to the next batch to improve the quality of the descriptions.
    to_thread.current_default_thread_limiter().total_tokens = ANYIO_THREAD_COUNT

    generated_batch_column_descriptions: List[
        asyncer.SoonValue[Tuple[Optional[str], Dict[str, str]]]
    ] = []
    async with asyncer.create_task_group() as task_group:
        for i in range(len(column_splits)):
            generated_batch_column_descriptions.append(
                task_group.soonify(generate_column_batch_descriptions)(
                    litellm,
                    table_info,
                    column_infos,
                    generated_table_description,
                    column_splits[i],
                    i,
                    extra_instructions,
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


def _return_last_value(
    retry_state: tenacity.RetryCallState,
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    """return the result of the last call attempt"""
    return (
        retry_state.outcome.result()
        if retry_state.outcome is not None
        else (None, None)
    )


@mlflow.trace(name="generate_column_batch_descriptions", span_type="function")
@tenacity.retry(
    stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
    retry=tenacity.retry_if_result(lambda x: x[1] is None),
    before_sleep=lambda retry_state: logger.info(
        f"Retry column batch description generation attempt {retry_state.attempt_number} of {_MAX_ATTEMPTS - 1}"
    ),
    retry_error_callback=_return_last_value,
)
async def generate_column_batch_descriptions(
    litellm: LiteLLM,
    table_info: TableInfo,
    column_infos: Dict[str, ColumnMetadataInfo],
    generated_table_description: str,
    columns_batch: List[str],
    i: int,
    extra_instructions: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    logger.debug(f"Starting batch {i} description generation")
    formatted_common_context = PROMPT_COMMON_CONTEXT.format(
        table_info=table_info.model_dump(exclude_none=True),
    )
    formatted_columns_context = PROMPT_COLUMNS_CONTEXT.format(
        column_info={
            col: column_info.model_dump(exclude_none=True)
            for col, column_info in column_infos.items()
            # pass only columns in current batch for large tables
            if len(column_infos) <= LARGE_TABLE_THRESHOLD or col in columns_batch
        },
    )
    column_batch_instructions = COLUMN_BATCH_INSTRUCTIONS.format(
        num_columns=len(columns_batch),
        columns=json.dumps(columns_batch),
    )
    formatted_column_prompt = COLUMN_DESC_PROMPT.format(
        table_description=generated_table_description,
        column_batch_instructions=column_batch_instructions,
    )
    try:
        logger.debug(f"Calling LLM for batch {i}")

        # Build the prompt messages
        messages = [
            LiteLLMPromptMessage(
                text=formatted_common_context,
                cache=True,
            ),
            LiteLLMPromptMessage(
                text=formatted_columns_context,
            ),
        ]

        messages.append(
            LiteLLMPromptMessage(
                text=formatted_column_prompt,
                cache=False,
            )
        )

        # Add extra instructions if provided
        if extra_instructions:
            messages.append(
                LiteLLMPromptMessage(
                    text=f"\nADDITIONAL REQUIREMENTS:\n{extra_instructions}\n",
                    cache=False,
                )
            )

        column_descriptions_raw = await asyncer.asyncify(litellm.call_lite_llm)(
            prompt=messages
        )
        logger.debug(f"Finished LLM call for batch {i}")

        batch_column_descriptions, batch_failure_reason = parse_columns_llm_output(
            column_descriptions_raw
        )
        if batch_column_descriptions is not None:
            missing_columns = set(columns_batch) - set(batch_column_descriptions.keys())
            if missing_columns:
                logger.warning(
                    f"{len(missing_columns)} columns missing descriptions in batch {i} for urn {table_info.name}"
                )
    except Exception as e:
        # As we do not wish to fail entire description generation for single column batch
        # we do not raise error here, instead retry for batch_column_descriptions is None
        logger.error(f"Error generating column descriptions for batch {i}: {e}")
        batch_failure_reason = str(e)
        batch_column_descriptions = None
    logger.debug(f"Finished batch {i} description generation")

    return batch_failure_reason, batch_column_descriptions
