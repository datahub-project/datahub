import pytest

from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


@pytest.fixture
def other_checkpoint_state():
    state = GenericCheckpointState()
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user3")
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user5")
    state.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group1")
    state.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group2")
    state.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group3")
    state.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group5")
    return state


def test_add_checkpoint_urn():
    state = GenericCheckpointState()
    assert len(state.urns) == 0
    state.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    assert len(state.urns) == 1
    state.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group2")
    assert len(state.urns) != 1


def test_get_urns_not_in(other_checkpoint_state):
    oldstate = GenericCheckpointState()
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user4")
    oldstate.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group1")
    oldstate.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group2")
    oldstate.add_checkpoint_urn("corpGroup", "urn:li:corpGroup:group4")
    iterable = oldstate.get_urns_not_in("corpuser", other_checkpoint_state)
    # urn:li:corpuser:user4 has been identified as a user to be deleted
    for item in iterable:
        assert item == "urn:li:corpuser:user4"
    iterable = oldstate.get_urns_not_in("corpGroup", other_checkpoint_state)
    for item in iterable:
        assert item == "urn:li:corpGroup:group4"


def test_get_percent_entities_changed(other_checkpoint_state):
    oldstate = GenericCheckpointState()
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user1")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user2")
    oldstate.add_checkpoint_urn("corpuser", "urn:li:corpuser:user4")
    percent = oldstate.get_percent_entities_changed(other_checkpoint_state)
    # two states have 75% of differences (total 8 elements, 6 elements are different (75%) and 2 (25%) are the same)
    assert percent == 75.0
