import json
from typing import Optional, TypeVar, Union

from datahub.configuration.common import ConnectionModel
from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

# TypeVar to allow ConnectionModel subclasses (e.g., TeamsConnection, SlackConnection)
ConnectionModelT = TypeVar("ConnectionModelT", bound=ConnectionModel)

_connection_urn_prefix = "urn:li:dataHubConnection:"


def _is_connection_urn(urn: str) -> bool:
    return urn.startswith(_connection_urn_prefix)


def _get_id_from_connection_urn(urn: str) -> str:
    assert _is_connection_urn(urn)
    return urn[len(_connection_urn_prefix) :]


def get_connection_json(graph: DataHubGraph, urn: str) -> Optional[dict]:
    res = graph.execute_graphql(
        query="""
query GetConnection($urn: String!) {
  connection(urn: $urn) {
    urn
    details {
      type
      name
      json {
        blob
      }
    }
  }
}
""".strip(),
        variables={
            "urn": urn,
        },
    )

    if not res["connection"]:
        return None

    connection_type = res["connection"]["details"]["type"]
    if connection_type != "JSON":
        logger.error(
            f"Expected connection details type to be 'JSON', but got {connection_type}"
        )
        return None

    blob = res["connection"]["details"]["json"]["blob"]
    obj = json.loads(blob)

    # TODO: Also return the connection name, mainly for debugging purposes?

    return obj


def save_connection_json(
    graph: DataHubGraph,
    *,
    urn: str,
    platform_urn: str,
    config: Union[ConnectionModelT, dict],
    name: Optional[str] = None,
) -> None:
    if isinstance(config, ConnectionModel):
        blob = config.model_dump_json()
    else:
        blob = json.dumps(config)

    id = _get_id_from_connection_urn(urn)

    res = graph.execute_graphql(
        query="""
mutation SetConnection($id: String!, $platformUrn: String!, $blob: String!, $name: String) {
  upsertConnection(
    input: {
      id: $id,
      type: JSON,
      name: $name,
      platformUrn: $platformUrn,
      json: {
        blob: $blob
      }
    }
  ) {
    urn
  }
}
""".strip(),
        variables={
            "id": id,
            "platformUrn": platform_urn,
            "name": name,
            "blob": blob,
        },
    )

    assert res["upsertConnection"]["urn"] == urn
