import ast
import collections
import os
import pathlib
import re
from typing import Dict, List, Tuple

import asyncer
import more_itertools
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger
from pydantic import BaseModel, Field, ValidationError, parse_obj_as

from datahub_integrations.gen_ai.bedrock import BedrockModel
from datahub_integrations.gen_ai.description_v2 import (
    call_bedrock_llm,
    extract_metadata_for_urn,
    transform_table_info_for_llm,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import GlossaryInfo

TERM_SUGGESTION_GENERATION_MODEL: BedrockModel = parse_obj_as(
    BedrockModel,
    os.getenv(
        "TERM_SUGGESTION_GENERATION_BEDROCK_MODEL", BedrockModel.CLAUDE_3_HAIKU.value
    ),
)

# The AWS quota is 1000 requests per minute for Haiku 3 and
# 20-50 (depending on region) for Claude 3.5 Sonnet.
# However, this limits the number of concurrent requests we can make, and not
# the requests per minute. (Ideally we'll switch to a leaky bucket ratelimiter instead.)
# Given that an average request might take a couple seconds, 80 seemed like
# a reasonable enough default for now.
# Update - this actually defaults to 40, which is a decent enough default for now.
# _concurrency_limiter = anyio.CapacityLimiter(
#     total_tokens=int(os.getenv("TERM_SUGGESTION_CONCURRENCY_LIMIT", 80))
# )

# Suggestions with a confidence score strictly below this should be filtered out.
TERM_SUGGESTION_CONFIDENCE_THRESHOLD = float(
    os.getenv("TERM_SUGGESTION_CONFIDENCE_THRESHOLD", 9)
)
TEMPERATURE = float(os.getenv("TEMPERATURE", 0.1))
# The maximum number of columns to include in a single prompt.
# If there's more columns than this, we split it across multiple prompts.
COLUMN_SPLIT_LENGTH = 20

# The maximum number of glossary terms to include in a single prompt.
# If there's more glossary terms than this, we split it across multiple prompts.
NUM_GLOSSARY_TERMS_IN_BATCH = 12

_USE_EXTRACTION = False
_EXTRACTION_PROMPT_PATH = pathlib.Path(__file__).parent / "extraction_prompt.txt"
_PROMPT_PATH = pathlib.Path(__file__).parent / "term_suggestion_prompt.txt"
_TABLE_INFO_FOR_PROMPT = ["name", "description"]
_COLUMN_INFO_FOR_PROMPT = [
    "column_name",
    "descriptions",
    "sample_values",
    "datatype",
]


class TermSuggestionBundle(BaseModel):
    urn: str
    name: str
    reasoning: str
    confidence_score: float
    is_fake: bool | None = Field(default=None)


def parse_terms_list_obj(terms_list: List[dict]) -> List[TermSuggestionBundle]:
    parsed_terms_list: List[TermSuggestionBundle] = []
    for term_dict in terms_list:
        try:
            parsed_terms_list.append(TermSuggestionBundle.parse_obj(term_dict))
        except ValidationError as e:
            logger.info(f"Validation error for element {term_dict}: {e}")
    return parsed_terms_list


def parse_llm_output(
    text: str,
) -> Tuple[
    List[TermSuggestionBundle] | None,
    Dict[str, List[TermSuggestionBundle]] | None,
]:
    match = re.search(r"\{[\s\S]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        dict_str_cleaned = dict_str.strip()
        dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)
            parsed_extracted_dict: Dict[str, List[TermSuggestionBundle]] = {
                key: parse_terms_list_obj(value)
                for key, value in extracted_dict.items()
            }
            table_terms = parsed_extracted_dict.pop("table", None)
            return table_terms, parsed_extracted_dict
        except (SyntaxError, ValueError) as e:
            logger.info(f"Error evaluating dictionary: {e}")  # . Text: {text}")
            return None, None
    else:
        logger.info("No dictionary found in the text.")
        return None, None


def label_fake_column_terms(
    column_terms: Dict[str, List[TermSuggestionBundle]] | None,
    all_terms: List[str],
) -> Dict[str, List[TermSuggestionBundle]] | None:
    if isinstance(column_terms, dict):
        for column, terms in column_terms.items():
            updated_terms = []
            for term in terms:
                if isinstance(term, TermSuggestionBundle):
                    if term.name in all_terms:
                        term.is_fake = False
                    else:
                        term.is_fake = True
                updated_terms.append(term)
            column_terms[column] = updated_terms
    return column_terms


def generate_extraction_prompt(
    raw_llm_response: str,
) -> str:
    prompt_template = _EXTRACTION_PROMPT_PATH.read_text()
    prompt = prompt_template.format(
        raw_llm_response=raw_llm_response,
    )
    return prompt


def generate_prompt(
    table_info: dict,
    column_info: dict,
    glossary_info: GlossaryInfo,
    prompt_path: str | None | pathlib.Path,
) -> str:
    if prompt_path is not None:
        logger.info(f"Reading prompt from {prompt_path}")
        prompt_template = pathlib.Path(prompt_path).read_text()
    else:
        logger.info(f"Reading default prompt from {_PROMPT_PATH}")
        prompt_template = _PROMPT_PATH.read_text()
    prompt = prompt_template.format(
        table_info=table_info,
        column_info=column_info,
        glossary_terms_info=glossary_info.glossary,
    )
    # logger.debug(f"Generated prompt: {prompt}")
    return prompt


def split_list_in_equal_parts(columns: list[str], limit: int) -> list[list[str]]:
    return list(more_itertools.chunked_even(columns, limit))


def filter_table_information(table_info: dict, column_info: dict) -> tuple[dict, dict]:
    for column in column_info.keys():
        column_info[column].update(
            {
                "datatype": column_info[column]
                .get("metadata", {})
                .get("nativeDataType", "")
            }
        )
    table_info_filtered = {
        k: v for k, v in table_info.items() if k in _TABLE_INFO_FOR_PROMPT
    }
    column_info_filtered = {}
    for column_name, column_info_dict in column_info.items():
        column_info_filtered[column_name] = {
            k: v for k, v in column_info_dict.items() if k in _COLUMN_INFO_FOR_PROMPT
        }
    return table_info_filtered, column_info_filtered


def get_terms_splits(glossary_info: GlossaryInfo) -> list[GlossaryInfo]:
    glossary_term_urns = list(glossary_info.glossary.keys())
    glossary_term_splits = split_list_in_equal_parts(
        glossary_term_urns, limit=NUM_GLOSSARY_TERMS_IN_BATCH
    )
    glossary_batches = []
    for glossary_term_split in glossary_term_splits:
        glossary_batches.append(
            GlossaryInfo({k: glossary_info.glossary[k] for k in glossary_term_split})
        )
    return glossary_batches


async def get_term_recommendations_for_column_splits(
    column_splits: List[List[str]],
    table_info: dict,
    column_info: dict,
    glossary_info: GlossaryInfo,
    prompt_path: str | None | pathlib.Path,
) -> tuple[
    List[TermSuggestionBundle] | None, Dict[str, List[TermSuggestionBundle]] | None, str
]:
    logger.debug(f"Column split lengths: {[len(split) for split in column_splits]}")
    term_splits = get_terms_splits(glossary_info)
    logger.debug(f"Term splits: {len(term_splits)}")

    # Run the LLM calls in parallel.
    raw_llm_responses: Dict[int, Dict[int, asyncer.SoonValue[str]]] = (
        collections.defaultdict(dict)
    )  # (col_split_idx, term_split_idx) -> raw_llm_response future
    async with asyncer.create_task_group() as task_group:
        for col_split_idx, column_split in enumerate(column_splits):
            column_split_info = {
                key: value for key, value in column_info.items() if key in column_split
            }

            for term_split_idx, term_split in enumerate(term_splits):
                prompt = generate_prompt(
                    table_info,
                    column_split_info,
                    glossary_info=term_split,
                    prompt_path=prompt_path,
                )
                raw_llm_responses[col_split_idx][term_split_idx] = task_group.soonify(
                    asyncer.asyncify(call_bedrock_llm)
                )(
                    prompt=prompt,
                    model=TERM_SUGGESTION_GENERATION_MODEL,
                    max_tokens=5000,
                    temperature=TEMPERATURE,
                )

    column_terms: Dict[str, List[TermSuggestionBundle]] | None = None
    table_terms: List[TermSuggestionBundle] | None = None
    raw_llm_response: str = ""
    for col_split_idx, _ in enumerate(column_splits):
        for term_split_idx, _ in enumerate(term_splits):
            raw_llm_response_for_column_split = raw_llm_responses[col_split_idx][
                term_split_idx
            ].value

            if _USE_EXTRACTION:
                extraction_prompt = generate_extraction_prompt(
                    raw_llm_response=raw_llm_response_for_column_split
                )
                raw_llm_response_for_column_split = call_bedrock_llm(
                    prompt=extraction_prompt,
                    model=TERM_SUGGESTION_GENERATION_MODEL,
                    max_tokens=5000,
                    temperature=TEMPERATURE,
                )

            table_terms, column_split_terms = parse_llm_output(
                raw_llm_response_for_column_split
            )

            if isinstance(column_split_terms, dict):
                if isinstance(column_terms, dict):
                    for key, value in column_split_terms.items():
                        if key in column_terms:
                            column_terms[key].extend(
                                value
                            )  # Append values to the existing list
                        else:
                            column_terms[key] = value  # Create a new entry
                    # column_terms.update(column_split_terms)
                else:
                    column_terms = column_split_terms
            raw_llm_response = (
                f"{raw_llm_response} \n\n {raw_llm_response_for_column_split}"
            )
    return table_terms, column_terms, raw_llm_response


def get_entity_info_for_term_suggestion(
    graph: DataHubGraph, table_urn: str
) -> tuple[dict, dict]:
    entity = graph.get_entity_semityped(table_urn)
    extracted_entity_info = extract_metadata_for_urn(entity, table_urn, graph)
    table_info, column_info = transform_table_info_for_llm(extracted_entity_info)
    table_info_filtered, column_info_filtered = filter_table_information(
        table_info, column_info
    )
    return table_info_filtered, column_info_filtered


def get_term_recommendations(
    table_urn: str,
    graph_client: DataHubGraph,
    glossary_info: GlossaryInfo,
    prompt_path: str | None = None,
) -> tuple[
    List[TermSuggestionBundle] | None,
    Dict[str, List[TermSuggestionBundle]] | None,
    str,
]:
    table_info_filtered, column_info_filtered = get_entity_info_for_term_suggestion(
        graph_client, table_urn
    )
    column_splits = split_list_in_equal_parts(
        list(column_info_filtered.keys()), limit=COLUMN_SPLIT_LENGTH
    )
    (
        table_terms,
        column_terms,
        raw_llm_response,
    ) = asyncer.syncify(
        get_term_recommendations_for_column_splits,
        raise_sync_error=False,
    )(
        column_splits=column_splits,
        table_info=table_info_filtered,
        column_info=column_info_filtered,
        glossary_info=glossary_info,
        prompt_path=prompt_path,
    )
    all_glossary_terms = []
    for term in glossary_info.glossary.values():
        all_glossary_terms.append(term["term_name"])
    if isinstance(table_terms, list):
        for term in table_terms:
            if term.name in all_glossary_terms:
                term.is_fake = False
            else:
                term.is_fake = True
    column_terms = label_fake_column_terms(column_terms, all_glossary_terms)
    return table_terms, column_terms, raw_llm_response
