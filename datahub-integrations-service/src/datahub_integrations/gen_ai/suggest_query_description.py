import dataclasses

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.gen_ai.bedrock import BedrockModel, call_bedrock_llm


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

    # TODO: Fetch more context from the query entity.
    return QueryContext(
        query_urn=query_urn,
        query_text=query_res["entity"]["properties"]["statement"]["value"],
    )


def generate_query_desc(entity_context: QueryContext) -> str:
    """Generate a description for the entity."""

    description = call_bedrock_llm(
        model=BedrockModel.CLAUDE_35_SONNET,
        max_tokens=200,
        prompt=f"""\
Provide a 1-2 sentence summary of this SQL query. Write with an imperative mood.

Here's a few examples of good summaries:
- Pull user counts and team owners, broken down by domain and plan tier.
- View education users from domain 'example.com' who have not logged in for 30 days.

<query>
{entity_context.query_text}
</query>
""",
    )

    return description


if __name__ == "__main__":
    import sys

    from datahub_integrations.app import graph

    if len(sys.argv) > 1:
        target_urn = sys.argv[1]
    else:
        target_urn = "urn:li:query:61705a7013ab63277266251565750876"

    context = get_query_context(graph, target_urn)
    logger.info(f"For query {context.query_urn}")
    logger.debug(context.query_text)
    desc = generate_query_desc(context)
    logger.info(desc)
