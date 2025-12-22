from locust import HttpUser, constant, task
import random

from test_utils.datahub_sessions import DataHubSessions
from test_utils.graphql_queries import GraphQLQueries
from test_utils.mock_data_helper import default_mock_data


datahub_instances = DataHubSessions()
graphql_queries = GraphQLQueries()


class ScrollAcrossLineageUser(HttpUser):
    """
    Performance test for scrollAcrossLineage GraphQL query with mock lineage data.

    Tests lineage queries against ~1M mock tables to measure cache behavior and query performance.
    Uses MockDataHelper to generate diverse URNs spanning the full dataset to avoid cache hits.

    Query Configuration:
    - First page only (count=10, no scrollId)
    - Degree=1 filtering (1-hop lineage)
    - Type=DATASET only
    - Random UPSTREAM/DOWNSTREAM direction
    - Random URNs from ~1M table dataset
    """
    wait_time = constant(1)

    @task
    def scroll_across_lineage(self):
        # Get random URN from the full ~1M table dataset to avoid cache hits
        urn = default_mock_data.get_random_urn()

        # Random direction
        direction = random.choice(["UPSTREAM", "DOWNSTREAM"])

        # Get scrollAcrossLineage query
        session = datahub_instances.get_session(self.host)
        gql_query = graphql_queries.get_query("scrollAcrossLineage")

        # Build query with random URN and direction
        query_json = gql_query.get_query_with_variables({
            "input": {
                "urn": urn,
                "direction": direction,
                "types": ["DATASET"],
                "query": "",
                "scrollId": None,  # First page only
                "keepAlive": "5m",
                "count": 10,
                "orFilters": [{
                    "and": [{
                        "field": "degree",
                        "condition": "EQUAL",
                        "values": ["1"],
                        "negated": False
                    }]
                }]
            }
        })

        # Execute query
        test_name = f"scrollAcrossLineage[{direction}]"
        response = self.client.request(
            method="POST",
            name=f"{session.get_short_host()}[{test_name}]",
            url=f"{self.host}/api/graphql",
            cookies=session.get_cookies(),
            json=query_json
        )
        response.raise_for_status()
