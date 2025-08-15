import unittest

import pytest

from datahub.utilities.urns.tag_urn import TagUrn


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestTagUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        tag_urn_str = "urn:li:tag:abc"
        tag_urn = TagUrn.create_from_string(tag_urn_str)
        assert tag_urn.get_type() == TagUrn.ENTITY_TYPE

        assert tag_urn.get_entity_id() == ["abc"]
        assert str(tag_urn) == tag_urn_str
        assert tag_urn == TagUrn("abc")
        assert tag_urn == TagUrn.create_from_id("abc")
