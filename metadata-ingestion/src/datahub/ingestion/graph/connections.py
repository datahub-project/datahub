_connection_urn_prefix = "urn:li:dataHubConnection:"


def _is_connection_urn(urn: str) -> bool:
    return urn.startswith(_connection_urn_prefix)


def get_id_from_connection_urn(urn: str) -> str:
    assert _is_connection_urn(urn)
    return urn[len(_connection_urn_prefix) :]


connections_gql = """\
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
"""
