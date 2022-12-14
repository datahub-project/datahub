from datahub.ingestion.source.state.entity_removal_state import (
    GenericCheckpointState,
    pydantic_state_migrator,
)


class BaseSQLAlchemyCheckpointState(GenericCheckpointState):
    """
    Base class for representing the checkpoint state for all SQLAlchemy based sources.
    Stores all tables and views being ingested and is used to remove any stale entities.
    Subclasses can define additional state as appropriate.
    """

    _migration = pydantic_state_migrator(
        {
            "encoded_table_urns": "dataset",
            "encoded_view_urns": "dataset",
            "encoded_container_urns": "container",
            "encoded_assertion_urns": "assertion",
        }
    )
