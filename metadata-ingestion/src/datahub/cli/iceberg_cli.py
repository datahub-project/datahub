import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
import botocore
import click

import datahub.metadata.schema_classes
from datahub.cli.cli_utils import post_entity
from datahub.configuration.common import GraphError
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.schema_classes import SystemMetadataClass
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)

DEFAULT_CREDS_EXPIRY_DURATION_SECONDS = 60 * 60
DEFAULT_FABRIC_TYPE = datahub.metadata.schema_classes.FabricTypeClass.PROD

DATA_PLATFORM_INSTANCE_WAREHOUSE_ASPECT = "icebergWarehouseInfo"


@click.group()
def iceberg() -> None:
    """A group of commands to manage Iceberg warehouses using DataHub as the Iceberg Catalog."""
    pass


def validate_creds(client_id: str, client_secret: str, region: str) -> Any:
    try:
        # Create a boto3 client with the provided credentials
        # Using STS (Security Token Service) for validation
        sts_client = boto3.client(
            "sts",
            aws_access_key_id=client_id,
            aws_secret_access_key=client_secret,
            region_name=region,
        )

        # Try to get caller identity
        sts_client.get_caller_identity()

        # If successful, return True and the account info
        return sts_client

    except (
        botocore.exceptions.ClientError,
        botocore.exceptions.NoCredentialsError,
    ):
        # If credentials are invalid, return False with error message
        click.secho(
            "Invalid credentials",
            fg="red",
            err=True,
        )
        sys.exit(1)


def validate_role(role: str, sts_client: Any, duration_seconds: Optional[int]) -> None:
    try:
        session_name = (
            f"datahub-cli-iceberg-validation-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )
        # Assume the IAM role to ensure the settings we have are valid and if not, can report them at config time.

        # If duration_seconds is not specified, datahub will attempt to default to an internal default
        # defined in S3CredentialProvider.java DEFAULT_CREDS_DURATION_SECS. However, it is not possible to know for sure
        # if that value is permitted based on how the role is configured. So, during the configuration of the warehouse
        # we must attempt to use the intended expiration duration (default or explicitly supplied) to ensure it
        # actually does work.
        if duration_seconds is None:
            duration_seconds = DEFAULT_CREDS_EXPIRY_DURATION_SECONDS

        assumed_role = sts_client.assume_role(
            RoleArn=role,
            RoleSessionName=session_name,
            DurationSeconds=duration_seconds,
        )

        # Extract the temporary credentials
        credentials = assumed_role["Credentials"]
        return credentials

    except Exception as e:
        click.secho(
            f"Failed to assume role using '{role}' with error: {e}",
            fg="red",
            err=True,
        )
        sys.exit(1)


def validate_warehouse(data_root: str) -> None:
    # validate data_root location
    scheme = urlparse(data_root).scheme
    if scheme != "s3":
        click.secho(
            f"Unsupported warehouse location '{data_root}', supported schemes: s3",
            fg="red",
            err=True,
        )
        sys.exit(1)


@iceberg.command()
@click.option(
    "-w", "--warehouse", required=True, type=str, help="The name of the warehouse"
)
@click.option(
    "-p", "--description", required=False, type=str, help="Description of the warehouse"
)
@click.option(
    "-d",
    "--data_root",
    required=True,
    type=str,
    help="The path to the data root for the warehouse data",
)
@click.option(
    "-i",
    "--client_id",
    required=True,
    type=str,
    help="Client ID to authenticate with the storage provider of the data root",
)
@click.option(
    "-s",
    "--client_secret",
    required=True,
    type=str,
    help="Client Secret to authenticate with the storage provider of the data root",
)
@click.option(
    "-g",
    "--region",
    required=True,
    type=str,
    help="Storage provider specific region where the warehouse data root is located",
)
@click.option(
    "-r",
    "--role",
    required=True,
    type=str,
    help="Storage provider specific role to be used when vending credentials",
)
@click.option(
    "-e",
    "--env",
    required=False,
    type=str,
    help=f"Environment where all assets stored in this warehouse belong to. Defaults to {DEFAULT_FABRIC_TYPE} if unspecified",
)
@click.option(
    "-x",
    "--duration_seconds",
    required=False,
    type=int,
    help=f"Expiration duration for temporary credentials used for role. Defaults to {DEFAULT_CREDS_EXPIRY_DURATION_SECONDS} seconds if unspecified",
)
@telemetry.with_telemetry(capture_kwargs=["duration_seconds"])
def create(
    warehouse: str,
    description: Optional[str],
    data_root: str,
    client_id: str,
    client_secret: str,
    region: str,
    role: str,
    duration_seconds: Optional[int],
    env: Optional[str],
) -> None:
    """
    Create an iceberg warehouse.
    """

    client = get_default_graph(ClientMode.CLI)

    urn = iceberg_data_platform_instance_urn(warehouse)

    if client.exists(urn):
        click.secho(
            f"Warehouse with name {warehouse} already exists",
            fg="red",
            err=True,
        )
        sys.exit(1)

    # will throw an actionable error message if invalid.
    validate_warehouse(data_root)
    storage_client = validate_creds(client_id, client_secret, region)
    validate_role(role, storage_client, duration_seconds)

    client_id_urn, client_secret_urn = create_iceberg_secrets(
        client, warehouse, client_id, client_secret
    )

    if env is None:
        env = DEFAULT_FABRIC_TYPE

    warehouse_aspect = DATA_PLATFORM_INSTANCE_WAREHOUSE_ASPECT
    warehouse_aspect_obj: Dict[str, Any] = {
        "dataRoot": data_root,
        "clientId": client_id_urn,
        "clientSecret": client_secret_urn,
        "region": region,
        "role": role,
        "env": env,
    }

    if duration_seconds:
        warehouse_aspect_obj["tempCredentialExpirationSeconds"] = duration_seconds

    data_platform_instance_properties_aspect_obj = {
        "name": warehouse,
    }

    if description:
        data_platform_instance_properties_aspect_obj["description"] = description

    data_platform_instance_properties_aspect = "dataPlatformInstanceProperties"

    entity_type = "dataPlatformInstance"
    system_metadata: Union[None, SystemMetadataClass] = None

    post_entity(
        client._session,
        client.config.server,
        urn=urn,
        aspect_name=data_platform_instance_properties_aspect,
        entity_type=entity_type,
        aspect_value=data_platform_instance_properties_aspect_obj,
        system_metadata=system_metadata,
    )

    # If status is non 200, post_entity will raise an exception.

    post_entity(
        client._session,
        client.config.server,
        urn=urn,
        aspect_name=warehouse_aspect,
        entity_type=entity_type,
        aspect_value=warehouse_aspect_obj,
        system_metadata=system_metadata,
    )

    click.secho(
        f"✅ Created warehouse with urn {urn}, clientID: {client_id_urn}, and clientSecret: {client_secret_urn}",
        fg="green",
    )


@iceberg.command()
@click.option(
    "-w", "--warehouse", required=True, type=str, help="The name of the warehouse"
)
@click.option(
    "-p",
    "--description",
    required=False,
    type=str,
    help="Description of the warehouse",
)
@click.option(
    "-d",
    "--data_root",
    required=True,
    type=str,
    help="The path to the data root for the warehouse data",
)
@click.option(
    "-i",
    "--client_id",
    required=True,
    type=str,
    help="Client ID to authenticate with the storage provider of the data root",
)
@click.option(
    "-s",
    "--client_secret",
    required=True,
    type=str,
    help="Client Secret to authenticate with the storage provider of the data root",
)
@click.option(
    "-g",
    "--region",
    required=True,
    type=str,
    help="Storage provider specific region where the warehouse data root is located",
)
@click.option(
    "-r",
    "--role",
    required=True,
    type=str,
    help="Storage provider specific role to be used when vending credentials",
)
@click.option(
    "-e",
    "--env",
    required=False,
    type=str,
    help=f"Environment where all assets stored in this warehouse belong to. Defaults to {DEFAULT_FABRIC_TYPE} if unspecified",
)
@click.option(
    "-x",
    "--duration_seconds",
    required=False,
    type=int,
    help=f"Expiration duration for temporary credentials used for role. Defaults to {DEFAULT_CREDS_EXPIRY_DURATION_SECONDS} seconds if unspecified",
)
@telemetry.with_telemetry(capture_kwargs=["duration_seconds"])
def update(
    warehouse: str,
    data_root: str,
    description: Optional[str],
    client_id: str,
    client_secret: str,
    region: str,
    role: str,
    env: Optional[str],
    duration_seconds: Optional[int],
) -> None:
    """
    Update iceberg warehouses. Can only update credentials, and role. Cannot update region
    """

    client = get_default_graph(ClientMode.CLI)

    urn = iceberg_data_platform_instance_urn(warehouse)

    if not client.exists(urn):
        raise click.ClickException(f"Warehouse with name {warehouse} does not exist")

    validate_warehouse(data_root)
    storage_client = validate_creds(client_id, client_secret, region)
    validate_role(role, storage_client, duration_seconds)

    client_id_urn, client_secret_urn = update_iceberg_secrets(
        client, warehouse, client_id, client_secret
    )

    if env is None:
        env = DEFAULT_FABRIC_TYPE

    warehouse_aspect = DATA_PLATFORM_INSTANCE_WAREHOUSE_ASPECT
    warehouse_aspect_obj: Dict[str, Any] = {
        "dataRoot": data_root,
        "clientId": client_id_urn,
        "clientSecret": client_secret_urn,
        "region": region,
        "role": role,
        "env": env,
    }
    if duration_seconds:
        warehouse_aspect_obj["tempCredentialExpirationSeconds"] = duration_seconds

    data_platform_instance_properties_aspect_obj = {
        "name": warehouse,
    }

    if description:
        data_platform_instance_properties_aspect_obj["description"] = description

    data_platform_instance_properties_aspect = "dataPlatformInstanceProperties"

    entity_type = "dataPlatformInstance"
    system_metadata: Union[None, SystemMetadataClass] = None

    post_entity(
        client._session,
        client.config.server,
        urn=urn,
        aspect_name=data_platform_instance_properties_aspect,
        entity_type=entity_type,
        aspect_value=data_platform_instance_properties_aspect_obj,
        system_metadata=system_metadata,
    )

    # If status is non 200, post_entity will raise an exception.
    post_entity(
        client._session,
        client.config.server,
        urn=urn,
        aspect_name=warehouse_aspect,
        entity_type=entity_type,
        aspect_value=warehouse_aspect_obj,
        system_metadata=system_metadata,
    )

    click.secho(
        f"✅ Updated warehouse with urn {urn}, clientID: {client_id_urn}, and clientSecret: {client_secret_urn}",
        fg="green",
    )


@iceberg.command()
@telemetry.with_telemetry()
def list() -> None:
    """
    List iceberg warehouses
    """

    client = get_default_graph(ClientMode.CLI)

    for warehouse in get_all_warehouses(client):
        click.echo(warehouse)


@iceberg.command()
@click.option(
    "-w", "--warehouse", required=True, type=str, help="The name of the warehouse"
)
@telemetry.with_telemetry()
def get(warehouse: str) -> None:
    """Fetches the details of the specified iceberg warehouse"""
    client = get_default_graph(ClientMode.CLI)
    urn = iceberg_data_platform_instance_urn(warehouse)

    if client.exists(urn):
        warehouse_aspect = client.get_aspect(
            entity_urn=urn,
            aspect_type=datahub.metadata.schema_classes.IcebergWarehouseInfoClass,
        )
        click.echo(urn)
        if warehouse_aspect:
            click.echo(json.dumps(warehouse_aspect.to_obj(), sort_keys=True, indent=2))
    else:
        raise click.ClickException(f"Iceberg warehouse {warehouse} does not exist")


@iceberg.command()
@click.option(
    "-w", "--warehouse", required=True, type=str, help="The name of the warehouse"
)
@click.option("-n", "--dry-run", required=False, is_flag=True)
@click.option(
    "-f",
    "--force",
    required=False,
    is_flag=True,
    help="force the delete if set without confirmation",
)
@telemetry.with_telemetry(capture_kwargs=["dry_run", "force"])
def delete(warehouse: str, dry_run: bool, force: bool) -> None:
    """
    Delete warehouse
    """

    urn = iceberg_data_platform_instance_urn(warehouse)

    client = get_default_graph(ClientMode.CLI)

    if not client.exists(urn):
        raise click.ClickException(f"urn {urn} not found")

    # Confirm this is a managed warehouse by checking for presence of IcebergWarehouse aspect
    aspect = client.get_aspect(
        entity_urn=urn,
        aspect_type=datahub.metadata.schema_classes.IcebergWarehouseInfoClass,
    )
    if aspect:
        warehouse_aspect: datahub.metadata.schema_classes.IcebergWarehouseInfoClass = (
            aspect
        )

        urns_to_delete: List = []
        resource_names_to_be_deleted: List = []
        for entity in get_related_entities_for_platform_instance(client, urn):
            # Do we really need this double-check?
            if "__typename" in entity and "urn" in entity:
                if entity["__typename"] in ["Container", "Dataset"]:
                    # add the Platform Resource URN to also be deleted for each dataset.
                    # This is not user visible, so no need to show a name to the user and include it in the count. Each
                    # instance corresponds to a dataset whose name is shown.
                    if entity["__typename"] == "Dataset":
                        resource_urn = platform_resource_urn(
                            entity["properties"]["qualifiedName"]
                        )
                        urns_to_delete.append(resource_urn)

                    urns_to_delete.append(entity["urn"])
                    resource_names_to_be_deleted.append(
                        entity.get("name", entity.get("urn"))
                    )

        if dry_run:
            click.echo(
                f"[Dry-run] Would delete warehouse {urn} and the following datasets and namespaces"
            )
            for resource in resource_names_to_be_deleted:
                click.echo(f"   {resource}")
        else:
            if not force:
                click.confirm(
                    f"This will delete {warehouse} warehouse, credentials, and {len(resource_names_to_be_deleted)} datasets and namespaces from DataHub. Do you want to continue?",
                    abort=True,
                )

            # Delete the resources in the warehouse first, so that in case it is interrupted, the warehouse itself is
            # still available to enumerate the resources in it that are not yet deleted.
            for urn_to_delete in urns_to_delete:
                client.hard_delete_entity(urn_to_delete)

            client.hard_delete_entity(urn)
            client.hard_delete_entity(warehouse_aspect.clientId)
            client.hard_delete_entity(warehouse_aspect.clientSecret)

            click.echo(
                f"✅ Successfully deleted iceberg warehouse {warehouse} and associated credentials, {len(resource_names_to_be_deleted)} datasets and namespaces"
            )


def iceberg_data_platform_instance_urn(warehouse: str) -> str:
    return f"urn:li:dataPlatformInstance:({iceberg_data_platform()},{warehouse})"


def platform_resource_urn(dataset_name: str) -> str:
    return f"urn:li:platformResource:iceberg.{dataset_name}"


def iceberg_data_platform() -> str:
    return "urn:li:dataPlatform:iceberg"


def iceberg_client_id_urn(warehouse):
    return f"urn:li:dataHubSecret:{warehouse}-client_id"


def iceberg_client_secret_urn(warehouse):
    return f"urn:li:dataHubSecret:{warehouse}-client_secret"


def create_iceberg_secrets(
    client: DataHubGraph, warehouse: str, client_id: str, client_secret: str
) -> Tuple[str, str]:
    graphql_query = """
        mutation createIcebergSecrets($clientIdName: String!, $clientId: String!, $clientSecretName: String!, $clientSecret: String!) {
            createClientId: createSecret(
                input: {name: $clientIdName, value: $clientId}
            )
            createClientSecret: createSecret(
                input: {name: $clientSecretName, value: $clientSecret}
            )
        }
    """
    variables = {
        "clientIdName": f"{warehouse}-client_id",
        "clientId": client_id,
        "clientSecretName": f"{warehouse}-client_secret",
        "clientSecret": client_secret,
    }
    try:
        response = client.execute_graphql(
            graphql_query, variables=variables, format_exception=False
        )
    except GraphError as graph_error:
        try:
            error = json.loads(str(graph_error).replace('"', '\\"').replace("'", '"'))
            click.secho(
                f"Failed to save Iceberg warehouse credentials :{error[0]['message']}",
                fg="red",
                err=True,
            )
        except Exception:
            click.secho(
                f"Failed to save Iceberg warehouse credentials :\n{graph_error}",
                fg="red",
                err=True,
            )
        sys.exit(1)

    if "createClientId" in response and "createClientSecret" in response:
        return response["createClientId"], response["createClientSecret"]

    click.secho(
        f"Internal error: Unexpected response saving credentials:\n{response}",
        fg="red",
        err=True,
    )
    sys.exit(1)


def update_iceberg_secrets(
    client: DataHubGraph, warehouse: str, client_id: str, client_secret: str
) -> Tuple[str, str]:
    graphql_query = """
        mutation updateIcebergSecrets($clientIdUrn: String!, $clientIdName: String!, $clientId: String!, $clientSecretUrn: String!, $clientSecretName: String!, $clientSecret: String!) {
            updateClientId: updateSecret(
                input: {urn: $clientIdUrn, name: $clientIdName, value: $clientId}
            )
            updateClientSecret: updateSecret(
                input: {urn: $clientSecretUrn, name: $clientSecretName, value: $clientSecret}
            )
        }
    """
    variables = {
        "clientIdUrn": iceberg_client_id_urn(warehouse),
        "clientIdName": f"{warehouse}-client_id",
        "clientId": client_id,
        "clientSecretUrn": iceberg_client_secret_urn(warehouse),
        "clientSecretName": f"{warehouse}-client_secret",
        "clientSecret": client_secret,
    }
    try:
        response = client.execute_graphql(
            graphql_query, variables=variables, format_exception=False
        )
    except GraphError as graph_error:
        try:
            error = json.loads(str(graph_error).replace('"', '\\"').replace("'", '"'))
            click.secho(
                f"Failed to save Iceberg warehouse credentials :{error[0]['message']}",
                fg="red",
                err=True,
            )
        except Exception:
            click.secho(
                f"Failed to save Iceberg warehouse credentials :\n{graph_error}",
                fg="red",
                err=True,
            )
        sys.exit(1)

    if "updateClientId" in response and "updateClientSecret" in response:
        return response["updateClientId"], response["updateClientSecret"]

    click.secho(
        f"Internal error: Unexpected response saving credentials:\n{response}",
        fg="red",
        err=True,
    )
    sys.exit(1)


def get_all_warehouses(client: DataHubGraph) -> Iterator[str]:
    start: int = 0
    total = None
    graph_query = """
        query getIcebergWarehouses($start: Int, $count: Int) {
          search(
            input: {type: DATA_PLATFORM_INSTANCE, query: "dataPlatform:iceberg", start: $start, count: $count}
          ) {
            start
            total
            searchResults {
              entity {
                urn
                ... on DataPlatformInstance {
                  instanceId
                }
              }
            }
          }
        }
        """
    count = 10
    variables = {"start": start, "count": count}
    while total is None or start < total:
        response = client.execute_graphql(
            graph_query, variables=variables, format_exception=True
        )
        if "search" in response and "total" in response["search"]:
            total = response["search"]["total"]
            search_results = response["search"].get("searchResults", [])
            for result in search_results:
                yield result["entity"]["instanceId"]
            start += count
            variables = {"start": start, "count": count}
            # if total is not None and
        else:
            break


def get_related_entities_for_platform_instance(
    client: DataHubGraph, data_platform_instance_urn: str
) -> Iterator[Dict]:
    start: int = 0
    total = None

    graph_query = """
        query getIcebergResources($platformInstanceUrn: String!, $start: Int!, $count: Int!) {
          searchAcrossEntities(
            input: {types: [DATASET, CONTAINER], query: "*", start: $start, count: $count, orFilters: [{and: [{field: "platformInstance", values: [$platformInstanceUrn]}]}]}
          ) {
            start
            total
            searchResults {
              entity {
                __typename
                urn
                ... on Dataset {
                  urn
                  name
                  properties{
                    qualifiedName
                  }
                }
              }
            }
          }
        }
    """
    count = 10
    variables = {
        "start": start,
        "count": count,
        "platformInstanceUrn": data_platform_instance_urn,
    }
    while total is None or start < total:
        response = client.execute_graphql(
            graph_query, variables=variables, format_exception=True
        )
        if (
            "searchAcrossEntities" in response
            and "total" in response["searchAcrossEntities"]
        ):
            total = response["searchAcrossEntities"]["total"]
            search_results = response["searchAcrossEntities"].get("searchResults", [])
            for result in search_results:
                yield result["entity"]
            start += count
            variables["start"] = start
        else:
            break
