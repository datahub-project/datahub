import json
from pprint import pp

import openai
from openai_function_call import OpenAISchema
from pydantic import Field

from datahub_integrations.gen_ai.entity_context import generate_context

# class ColumnDocumentation(OpenAISchema):
#     """A description of a column in an entity."""

#     fieldPath: str = Field(description="The column's fieldPath")
#     documentation: str = Field(
#         description="A markdown-formatted description of the column."
#     )


class EntitySummary(OpenAISchema):
    """A description of the entity that encompasses it's contents, usage, and derivation."""

    summary: str = Field(
        description="The description of the entity. This is a markdown string and can include newlines."
    )

    # column_documentation: list[ColumnDocumentation] = Field(description="A list of column descriptions.")

    # column_descriptions: dict[str, str] = Field(
    #     description="A mapping of column field paths to descriptions.",
    # )


def generate_desc(entity_context: dict, use_flattery: bool) -> str:
    """Generate a description for the entity."""

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0613",
        functions=[EntitySummary.openai_schema],
        messages=[
            {
                "role": "system",
                "content": ""
                + (
                    "You are a senior data scientist at a large company, and are an expert in dimensional modeling. "
                    "You're leading a team-wide documentation effort. "
                    if use_flattery
                    else ""
                )
                + "I'm going to give you a JSON object with information about an entity in a data warehouse, orchestrator, or transformation tool. "
                "Call EntitySummary with a brief readme for the entity."
                "\n\n"
                "This summary will be presented as part a large entity-specific interface. "
                "It should be concise and to the point, and not include any information that is already present in the UI. "
                "In particular, it should not restate the name of the entity. "
                "Do not restate the database or schema that the entity lives in. "
                "Do not say 'located in the X schema' or 'located in the Y database'. This information is already shown to the user elsewhere. "
                "Do not include the external URL. "
                "\n\n"
                "Link to entities like datasets and tags with markdown using [`entity_name`](urn:li:<entity_urn>). "
                "You can only reference datasets, tags, or other entities using links - do not ever simply include the name of the entity without generating a link. "
                "\n\n"
                "Good information to include would be the purpose of the entity, which can be inferred from the schema, SQL code, and the list of upstreams. "
                "Also include usage tips if appropriate. "
                "Remember not to restate any information that is already present in the UI. "
                "",
                # TODO maybe include a template for what we want docs to look like.
            },
            {"role": "user", "content": json.dumps(entity_context)},
        ],
        max_tokens=500,
        temperature=0.4,
    )
    pp(completion)
    # TODO assert that "finish_reason" is "function_call". If not, retry with more tokens?

    # breakpoint()

    desc = EntitySummary.from_response(completion)

    pp(desc)
    print()
    print(desc.summary)

    return desc.summary


if __name__ == "__main__":
    target_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.active_customer_ltv,PROD)"

    entity_context = generate_context(target_urn)
    desc = generate_desc(entity_context, use_flattery=True)
