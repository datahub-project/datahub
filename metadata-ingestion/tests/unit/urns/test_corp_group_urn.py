import unittest

import pytest

from datahub.utilities.urns.corp_group_urn import CorpGroupUrn
from datahub.utilities.urns.error import InvalidUrnError


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestCorpGroupUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        corp_group_urn_str = "urn:li:corpGroup:abc"
        corp_group_urn = CorpGroupUrn.create_from_string(corp_group_urn_str)
        assert corp_group_urn.get_type() == CorpGroupUrn.ENTITY_TYPE

        assert corp_group_urn.get_entity_id() == ["abc"]
        assert str(corp_group_urn) == corp_group_urn_str
        assert corp_group_urn == CorpGroupUrn(name="abc")
        assert corp_group_urn == CorpGroupUrn.create_from_id("abc")

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            CorpGroupUrn.create_from_string(
                "urn:li:abc:(urn:li:dataPlatform:abc,def,prod)"
            )

        with self.assertRaises(InvalidUrnError):
            CorpGroupUrn.create_from_string("urn:li:corpGroup:(part1,part2)")
