from datahub.ingestion.source.state.entity_removal_state import (
    GenericCheckpointState,
    pydantic_state_migrator,
)


class DbtCheckpointState(GenericCheckpointState):
    """
    Class for representing the checkpoint state for DBT sources.
    Stores all nodes and assertions being ingested and is used to remove any stale entities.
    """

    _migration = pydantic_state_migrator(
        {
            "encoded_node_urns": "dataset",
            "encoded_assertion_urns": "assertion",
        }
    )
