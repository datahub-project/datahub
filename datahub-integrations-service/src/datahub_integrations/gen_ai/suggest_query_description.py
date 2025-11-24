import dataclasses

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.gen_ai.description_v3 import (
    get_extra_documentation_instructions,
)
from datahub_integrations.gen_ai.llm.factory import get_llm_client
from datahub_integrations.gen_ai.model_config import model_config


@dataclasses.dataclass
class QueryContext:
    query_urn: str
    query_text: str


def get_query_context(graph: DataHubGraph, query_urn: str) -> QueryContext:
    query_res = graph.execute_graphql(
        """\
query Query($urn: String!) {
  entity(urn: $urn) {
    urn
    ... on QueryEntity {
      properties {
        statement {
          value
          language
        }
        created {
          actor
        }
      }
    }
  }
}
""",
        {"urn": query_urn},
    )

    print(query_res)
    print(graph)

    # TODO: Fetch more context from the query entity.
    return QueryContext(
        query_urn=query_urn,
        query_text=query_res["entity"]["properties"]["statement"]["value"],
    )


def generate_query_desc(graph: DataHubGraph, entity_context: QueryContext) -> str:
    """Generate a description for the entity.

    Args:
        graph: DataHub graph client to use for fetching extra instructions
        entity_context: Context information about the query

    Returns:
        Generated description for the query
    """
    # Retrieve extra instructions internally
    extra_instructions = get_extra_documentation_instructions(graph)

    base_prompt = """\
Provide a detailed summary of the following SQL query logic for Business Analysts or Data Scientists unfamiliar with the query.
Explain the purpose of the query, the data it accesses, and the insights it provides. Focus on the semantic meaning of the query.
Include significant context, assumptions, or caveats that the query includes.

Additional Requirements:

- Do not use bullet points.
- Do not include any code snippets or special symbols.
- Be concise and to the point.
- Write with an imperative mood.
- Include a maximum of 3-4 well-structured paragraphs.
"""

    # Add extra instructions if provided
    if extra_instructions:
        base_prompt += f"\n\nCUSTOMER-SPECIFIC REQUIREMENTS:\n{extra_instructions}"

    base_prompt += f"""

<query>
{entity_context.query_text}
</query>
"""

    model = model_config.documentation_ai.query_description_model
    llm_client = get_llm_client(model)

    response = llm_client.converse(
        system=[],
        messages=[{"role": "user", "content": [{"text": base_prompt}]}],  # type: ignore[list-item]
        inferenceConfig={
            "temperature": 0.3,
            "maxTokens": 500,
        },
    )

    return response["output"]["message"]["content"][0]["text"]


if __name__ == "__main__":
    # This is intended for local testing. It uses the local ~/.datahubenv file.
    import sys

    from datahub.ingestion.graph.client import get_default_graph

    # Pass in a query urn e.g. urn:li:query:61705a7013ab63277266251565750876
    target_urn = sys.argv[1]

    graph = get_default_graph()
    context = get_query_context(graph, target_urn)
    logger.info(f"For query {context.query_urn}")
    logger.debug(context.query_text)
    desc = generate_query_desc(graph, context)
    logger.info(desc)
