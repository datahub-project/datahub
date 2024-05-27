import os

from datahub.ingestion.graph.client import get_default_graph
import yaml, json
import sys

if __name__ == "__main__":
    # first argument is the action pipeline file
    action_pipeline_file = sys.argv[1]
    # we first load it
    with open(action_pipeline_file) as f:
        recipe = yaml.safe_load(f)

    # this tells us the name of the action pipeline
    action_pipeline_name = recipe["name"]
    action_pipeline_type = recipe["type"]
    action_pipeline_urn = (
        f"urn:li:dataHubAction:{action_pipeline_name}"
        if not action_pipeline_name.startswith("urn:li:dataHubAction:")
        else action_pipeline_name
    )

    query = """
    mutation create_pipeline($urn: String!, $input: UpdateActionPipelineInput!) {
      upsertActionPipeline(urn: $urn, input: $input)
    }
    """
    variables = {
        "urn": action_pipeline_urn,
        "input": {
            "name": action_pipeline_name,
            "type": action_pipeline_type,
            "config": {
                "recipe": json.dumps(recipe),
                "executorId": "default",
            },
        },
    }

    with get_default_graph() as graph:
        result = graph.execute_graphql(query=query, variables=variables)
        print(result)
