import unittest

import pytest

from datahub.utilities.urns.corpuser_urn import CorpuserUrn
from datahub.utilities.urns.error import InvalidUrnError


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestCorpuserUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        corpuser_urn_str = "urn:li:corpuser:abc"
        corpuser_urn = CorpuserUrn.create_from_string(corpuser_urn_str)
        assert corpuser_urn.get_type() == CorpuserUrn.ENTITY_TYPE

        assert corpuser_urn.get_entity_id() == ["abc"]
        assert str(corpuser_urn) == corpuser_urn_str
        assert corpuser_urn == CorpuserUrn("abc")
        assert corpuser_urn == CorpuserUrn.create_from_id("abc")

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            CorpuserUrn.create_from_string(
                "urn:li:abc:(urn:li:dataPlatform:abc,def,prod)"
            )

        with self.assertRaises(InvalidUrnError):
            CorpuserUrn.create_from_string("urn:li:corpuser:(part1,part2)")
