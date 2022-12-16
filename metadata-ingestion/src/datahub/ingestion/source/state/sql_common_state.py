from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


class BaseSQLAlchemyCheckpointState(GenericCheckpointState):
    """
    Base class for representing the checkpoint state for all SQLAlchemy based sources.
    Stores all tables and views being ingested and is used to remove any stale entities.
    Subclasses can define additional state as appropriate.
    """
