import dataclasses

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.gen_ai.bedrock import (
    BedrockModel,
    call_bedrock_llm,
    get_bedrock_model_env_variable,
)

QUERY_DESCRIPTION_GENERATION_MODEL: BedrockModel | str = get_bedrock_model_env_variable(
    "QUERY_DESCRIPTION_GENERATION_BEDROCK_MODEL", BedrockModel.CLAUDE_35_SONNET
)


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


def generate_query_desc(entity_context: QueryContext) -> str:
    """Generate a description for the entity."""

    description = call_bedrock_llm(
        model=QUERY_DESCRIPTION_GENERATION_MODEL,
        max_tokens=500,
        prompt=f"""\
Provide a detailed summary of the following SQL query logic for Business Analysts or Data Scientists unfamiliar with the query.
Explain the purpose of the query, the data it accesses, and the insights it provides. Focus on the semantic meaning of the query.
Include significant context, assumptions, or caveats that the query includes.

Additional Requirements:

- Do not use bullet points.
- Do not include any code snippets or special symbols.
- Be concise and to the point.
- Write with an imperative mood.
- Include a maximum of 3-4 well-structured paragraphs.

<query>
{entity_context.query_text}
</query>
""",
    )

    return description


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
    desc = generate_query_desc(context)
    logger.info(desc)
