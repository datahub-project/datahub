from typing import Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph


class DataHubSecretsClient:
    """Class used to fetch secrets from DataHub."""

    graph: DataHubGraph

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def _cleanup_secret_name(self, secret_names: List[str]) -> List[str]:
        """Remove empty strings from the list of secret names."""
        return [secret_name for secret_name in secret_names if secret_name]

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        if len(secret_names) == 0:
            return {}

        res_data = self.graph.execute_graphql(
            query="""query getSecretValues($input: GetSecretValuesInput!) {
                getSecretValues(input: $input) {
                    name
                    value
                }
            }""",
            variables={"input": {"secrets": self._cleanup_secret_name(secret_names)}},
        )
        # Convert list of name, value secret pairs into a dict and return
        secret_value_list = res_data["getSecretValues"]
        secret_value_dict = dict()
        for secret_value in secret_value_list:
            secret_value_dict[secret_value["name"]] = secret_value["value"]
        return secret_value_dict
