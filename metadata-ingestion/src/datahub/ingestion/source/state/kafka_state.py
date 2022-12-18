from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


class KafkaCheckpointState(GenericCheckpointState):
    """
    This class represents the checkpoint state for Kafka based sources.
    Stores all the topics being ingested and it is used to remove any stale entities.
    """
