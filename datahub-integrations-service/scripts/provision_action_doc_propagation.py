import os

from datahub.ingestion.graph.client import get_default_graph
import yaml, json

if __name__ == "__main__":
    # server = os.environ["DATAHUB_SERVER"]
    # token = os.environ.get("DATAHUB_TOKEN")

    with open("./recipes/doc_propagation_action.yaml") as f:
        recipe = yaml.safe_load(f)

    query = """
    mutation create_pipeline($urn: String!, $input: UpdateActionPipelineInput!) {
      upsertActionPipeline(urn: $urn, input: $input)
    }
    """
    variables = {
        "urn": "urn:li:dataHubAction:docs_propagation_action",
        "input": {
            "name": "docs_propagation_action",
            "type": "docs_propagation_pipeline",
            "config": {
                "recipe": json.dumps(recipe),
                "executorId": "default",
            },
        },
    }

    with get_default_graph() as graph:
        result = graph.execute_graphql(query=query, variables=variables)
        print(result)
