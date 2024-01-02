from typing import Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph


class DataHubSecretsClient:
    """Class used to fetch secrets from DataHub."""

    graph: DataHubGraph

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        if len(secret_names) == 0:
            return {}

        request_json = {
            "query": """query getSecretValues($input: GetSecretValuesInput!) {\n
                getSecretValues(input: $input) {\n
                    name\n
                    value\n
                }\n
            }""",
            "variables": {"input": {"secrets": secret_names}},
        }
        # TODO: Use graph.execute_graphql() instead.

        # Fetch secrets using GraphQL API f
        response = self.graph._session.post(
            f"{self.graph.config.server}/api/graphql", json=request_json
        )
        response.raise_for_status()

        # Verify response
        res_data = response.json()
        if "errors" in res_data:
            raise Exception("Failed to retrieve secrets from DataHub.")

        # Convert list of name, value secret pairs into a dict and return
        secret_value_list = res_data["data"]["getSecretValues"]
        secret_value_dict = dict()
        for secret_value in secret_value_list:
            secret_value_dict[secret_value["name"]] = secret_value["value"]
        return secret_value_dict
