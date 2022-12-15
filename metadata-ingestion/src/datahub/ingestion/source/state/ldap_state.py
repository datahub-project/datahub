from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


class LdapCheckpointState(GenericCheckpointState):
    """
    Base class for representing the checkpoint state for all LDAP based sources.
    Stores all corpuser and corpGroup and being ingested and is used to remove any stale entities.
    """
