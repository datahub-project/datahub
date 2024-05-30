import os

from datahub.ingestion.graph.client import get_default_graph
import yaml, json
import click


@click.command()
@click.option("--file", help="The action pipeline file")
@click.option(
    "--operation",
    type=click.Choice(
        ["create", "stop", "rollback", "start", "status"], case_sensitive=False
    ),
    help="The operation to perform",
    default="create",
)
def main(file, operation):
    # first argument is the action pipeline file
    action_pipeline_file = file
    # we first load it
    with open(action_pipeline_file) as f:
        recipe = yaml.safe_load(f)

    # this tells us the name of the action pipeline
    action_pipeline_name = recipe.pop("name")
    action_pipeline_type = recipe["action"]["type"]
    action_pipeline_urn = (
        f"urn:li:dataHubAction:{action_pipeline_name}"
        if not action_pipeline_name.startswith("urn:li:dataHubAction:")
        else action_pipeline_name
    )

    if operation == "create":
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
    elif operation == "stop":
        print("Stopping the pipeline")
        query = """
        mutation stop_pipeline($urn: String!) {
          stopActionPipeline(urn: $urn)
        }
        """
        variables = {
            "urn": action_pipeline_urn,
        }
    elif operation == "rollback":
        print("Rolling back the pipeline")
        query = """
        mutation rollback_pipeline($urn: String!) {
            rollbackActionPipeline(urn: $urn)
            }
            """
        variables = {
            "urn": action_pipeline_urn,
        }
    elif operation == "start":
        print("Starting the pipeline")
        query = """
        mutation start_pipeline($urn: String!) {
            startActionPipeline(urn: $urn)
            }
            """
        variables = {
            "urn": action_pipeline_urn,
        }
    elif operation == "status":
        print("Getting the status of the pipeline")
        query = """
        query get_pipeline_status($urn: String!) {
            actionPipeline(urn: $urn) {
                status
            }
            }
            """
        variables = {
            "urn": action_pipeline_urn,
        }
    else:
        print("Invalid operation {}".format(operation))
    with get_default_graph() as graph:
        result = graph.execute_graphql(query=query, variables=variables)
        print(result)


if __name__ == "__main__":
    main()
