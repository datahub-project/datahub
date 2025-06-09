import json

import click
import yaml
from datahub.ingestion.graph.client import get_default_graph


@click.command()
@click.option("--file", help="The action pipeline file", required=True)
@click.option(
    "--operation",
    type=click.Choice(
        ["create", "stop", "stop-hard", "rollback", "start", "status"],
        case_sensitive=False,
    ),
    help="The operation to perform",
    default="create",
)
def main(file: str, operation: str) -> None:
    with open(file) as f:
        recipe = yaml.safe_load(f)

    # this tells us the name of the action pipeline
    action_pipeline_name = recipe.pop("name")
    action_pipeline_type = recipe["action"]["type"]
    action_pipeline_urn = (
        f"urn:li:dataHubAction:{action_pipeline_name}"
        if not action_pipeline_name.startswith("urn:li:dataHubAction:")
        else action_pipeline_name
    )

    graph = get_default_graph()

    if operation == "create":
        remove_keys = ["source", "datahub"]
        for key in remove_keys:
            if key in recipe:
                click.echo(f"Removing {key} block from recipe")
                del recipe[key]

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
    elif operation in {"stop", "stop-hard"}:
        if operation == "stop":
            print("Stopping the pipeline")
            pass
        else:
            print("Hard stopping the pipeline")
            graph.delete(action_pipeline_urn, soft=False)

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

    result = graph.execute_graphql(query=query, variables=variables)
    print(result)


if __name__ == "__main__":
    main()
