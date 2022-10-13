import pytest

from datahub.ingestion.source.state.ldap_state import LdapCheckpointState


@pytest.fixture
def other_checkpoint_state():
    state = LdapCheckpointState()
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user3")
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user5")
    return state


def test_add_checkpoint_urn():
    state = LdapCheckpointState()
    assert len(state.encoded_ldap_users) == 0
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    assert len(state.encoded_ldap_users) == 1
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    assert len(state.encoded_ldap_users) != 1


def test_get_supported_types():
    assert LdapCheckpointState().get_supported_types() == ["corpuser"]


def test_get_urns_not_in(other_checkpoint_state):
    oldstate = LdapCheckpointState()
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user4")
    iterable = oldstate.get_urns_not_in("corpuser", other_checkpoint_state)
    # urn:li:corpuser:user4 has been identified as a user to be deleted
    assert iterable.__next__() == "urn:li:corpuser:user4"


def test_get_percent_entities_changed(other_checkpoint_state):
    oldstate = LdapCheckpointState()
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user4")
    percent = oldstate.get_percent_entities_changed(other_checkpoint_state)
    # new state has 50% of new users (2) out of the total number (4)
    assert percent == 50.0
