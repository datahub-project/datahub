import json
from pprint import pp
from typing import List

import openai
from datahub.utilities.urns.urn import guess_entity_type
from openai_function_call.function_calls import _remove_a_key
from pydantic import BaseModel, Extra, parse_obj_as

from datahub_integrations.app import graph
from datahub_integrations.gen_ai.entity_context import generate_context


def fetch_tags_universe() -> List[str]:
    """Fetch the universe of tags in their instance."""

    tags = list(graph.get_urns_by_filter(entity_types=["tag"]))
    # TODO: Should we also fetch descriptions for the tags?

    tags = [tag for tag in tags if not is_system_tag(tag)]

    return tags


def is_system_tag(tag_urn: str) -> bool:
    """Return whether the tag is a system tag."""

    if tag_urn.startswith("urn:li:tag:__default__"):
        return True

    # Special column tag for BigQuery.
    if tag_urn in {
        "urn:li:tag:PARTITION_KEY",
    }:
        return True

    # Looker tags.
    if tag_urn in {
        "urn:li:tag:Dimension",
        "urn:li:tag:Temporal",
        "urn:li:tag:Measure",
    }:
        return True

    # Tableau tags.
    if tag_urn in {
        "urn:li:tag:MEASURE",
        "urn:li:tag:COLUMNFIELD",
        "urn:li:tag:CALCULATEDFIELD",
        "urn:li:tag:DATASOURCEFIELD",
        "urn:li:tag:HIERARCHYFIELD",
        "urn:li:tag:ATTRIBUTE",
        "urn:li:tag:SUM",
        "urn:li:tag:COUNT",
        "urn:li:tag:AVG",
        "urn:li:tag:YEAR",
    }:
        return True

    return False


class TagSuggestion(BaseModel, extra=Extra.forbid):
    """A suggested tag for an entity."""

    tag_urn: str
    explanation: str
    confidence_score: float


def suggest_tags(
    tag_universe: List[str], entity_urn: str, entity_context: dict
) -> List[str]:
    """Generate a description for the entity."""

    function_name = "extract_suggested_tags"

    tag_suggestion_schema = TagSuggestion.schema()
    _remove_a_key(tag_suggestion_schema, "title")

    function_spec = {
        "name": function_name,
        "description": "Extract suggested tags for an entity.",
        # should be a json schema of of {tag_suggestions: List[TagSuggestion]}
        "parameters": {
            "type": "object",
            "properties": {
                "tag_suggestions": {
                    "type": "array",
                    "items": tag_suggestion_schema,
                },
            },
        },
    }

    entity_type = guess_entity_type(entity_urn)

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0613",
        functions=[
            function_spec,
        ],
        messages=[
            {
                "role": "system",
                "content": ""
                "You are a senior data scientist at a large company, and are an expert in dimensional modeling. "
                "You're leading a team-wide documentation effort. "  # not sure if this is necessary
                f"I'm going to give you a JSON object with information about a {entity_type}. "
                f"Call {function_name} a list of 2-3 suggested tags, along with "
                "a brief explanation of why that tag is applies to this specific entity. "
                "Be sure to include details from the entity context in the explanation. "
                "Also include a confidence score for each tag suggestion. "
                "The suggested tags should be urns, and the confidence score should be a float between 0 and 1 and a median of 0.7. "
                "\n\n"
                "The tags should be chosen from the following list of tags: "
                f"{json.dumps(tag_universe)}. "
                "Do not suggest tags that are not present in that list.",
            },
            {"role": "user", "content": json.dumps(entity_context)},
        ],
        max_tokens=300,
    )
    pp(completion)

    # TODO If not a function call, retry with more tokens?
    assert completion.choices[0].finish_reason == "function_call"
    function_call_message = completion.choices[0].message.function_call
    assert function_call_message.name == function_name
    details_raw = json.loads(function_call_message.arguments)["tag_suggestions"]

    details = parse_obj_as(List[TagSuggestion], details_raw)

    # TODO: filter out tags that are already present
    # TODO: validate that the tags are in the universe

    # confidence_threshold = 0.65
    # TODO: drop tags below the confidence threshold

    tag_suggestions = [tag_suggestion.tag_urn for tag_suggestion in details]

    return tag_suggestions


if __name__ == "__main__":
    target_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.active_customer_ltv,PROD)"

    all_tags = fetch_tags_universe()

    entity_context = generate_context(target_urn)
    pp(entity_context)

    suggestions = suggest_tags(all_tags, target_urn, entity_context)
    print(suggestions)
    breakpoint()
