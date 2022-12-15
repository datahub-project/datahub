from datahub.ingestion.source.state.entity_removal_state import (
    GenericCheckpointState,
    pydantic_state_migrator,
)


class KafkaCheckpointState(GenericCheckpointState):
    """
    This class represents the checkpoint state for Kafka based sources.
    Stores all the topics being ingested and it is used to remove any stale entities.
    """

    _migration = pydantic_state_migrator(
        {
            "encoded_topic_urns": "topic",
        }
    )
