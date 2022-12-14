from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


class TableauCheckpointState(GenericCheckpointState):
    """
    Class for representing the checkpoint state for Tableau sources.
    Stores all datasets, charts and dashboards being ingested and is
    used to remove any stale entities.
    """
