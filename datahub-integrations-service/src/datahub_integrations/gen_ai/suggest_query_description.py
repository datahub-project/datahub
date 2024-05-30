import dataclasses

import openai
from loguru import logger

from datahub_integrations.app import graph


@dataclasses.dataclass
class QueryContext:
    query_urn: str
    query_text: str


def get_query_context(query_urn: str) -> QueryContext:
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

    return QueryContext(
        query_urn=query_urn,
        query_text=query_res["entity"]["properties"]["statement"]["value"],
    )


def generate_query_desc(entity_context: QueryContext) -> str:
    """Generate a description for the entity."""

    completion = openai.ChatCompletion.create(
        model="gpt-4o-2024-05-13",
        messages=[
            {
                "role": "system",
                "content": """\
Provide a one-sentence summary of this SQL query. Write with an imperative mood.

Here's a few examples of good summaries:
- Pull user counts and team owners, broken down by domain and plan tier.
- View education users from domain 'example.com' who have not logged in for 30 days.

```sql
{query_text}
```""".format(
                    query_text=entity_context.query_text
                ),
            },
        ],
        max_tokens=200,
        temperature=0.4,
    )

    # TODO assert that "finish_reason" is "function_call". If not, retry with more tokens?
    description = completion.choices[0].message["content"]

    return description


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        target_urn = sys.argv[1]
    else:
        target_urn = "urn:li:query:61705a7013ab63277266251565750876"

    context = get_query_context(target_urn)
    logger.info(f"For query {context.query_urn}")
    logger.debug(context.query_text)
    desc = generate_query_desc(context)
    logger.info(desc)
