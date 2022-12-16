from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


class DbtCheckpointState(GenericCheckpointState):
    """
    Class for representing the checkpoint state for DBT sources.
    Stores all nodes and assertions being ingested and is used to remove any stale entities.
    """
