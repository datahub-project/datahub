from typing import Type

import pydantic

from datahub.ingestion.api.global_context import get_graph_context


def auto_connection_resolver(
    connection_field: str = "connection",
) -> classmethod:
    def _resolve_connection(cls: Type, values: dict) -> dict:
        if connection_field in values:
            connection_urn = values.pop(connection_field)

            graph = get_graph_context()
            if not graph:
                raise ValueError(
                    "Fetching connection details from the backend requires a DataHub graph client."
                )

            conn = graph.get_connection_json(connection_urn)
            if conn is None:
                raise ValueError(
                    f"Connection {connection_urn} not found using {graph}."
                )

            # TODO: Should this do some additional validation against the config model?

            # Update the config, but don't overwrite existing values.
            for key, value in conn.items():
                if key not in values:
                    values[key] = value

        return values

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple validators do not overwrite each other.
    _resolve_connection.__name__ = f"{_resolve_connection.__name__}_{connection_field}"
    return pydantic.root_validator(pre=True, allow_reuse=True)(_resolve_connection)
