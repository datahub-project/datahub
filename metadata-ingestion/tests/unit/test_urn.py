import unittest

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class TestUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        simple_urn_str = "urn:li:dataPlatform:abc"
        urn = Urn.create_from_string(simple_urn_str)
        assert urn.get_entity_id_as_string() == "abc"
        assert urn.get_entity_id() == ["abc"]
        assert urn.get_type() == "dataPlatform"
        assert urn.get_domain() == "li"
        assert urn.__str__() == simple_urn_str
        assert urn == Urn("dataPlatform", ["abc"])

        complex_urn_str = "urn:li:dataset:(urn:li:dataPlatform:abc,def,prod)"
        urn = Urn.create_from_string(complex_urn_str)
        assert urn.get_entity_id_as_string() == "(urn:li:dataPlatform:abc,def,prod)"
        assert urn.get_entity_id() == ["urn:li:dataPlatform:abc", "def", "prod"]
        assert urn.get_type() == "dataset"
        assert urn.__str__() == "urn:li:dataset:(urn:li:dataPlatform:abc,def,prod)"

    def test_url_encode_urn(self) -> None:
        urn_with_slash: Urn = Urn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def/ghi,prod)"
        )
        assert (
            Urn.url_encode(str(urn_with_slash))
            == "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Aabc%2Cdef%2Fghi%2Cprod%29"
        )

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            Urn.create_from_string("urn:li:abc")

        with self.assertRaises(InvalidUrnError):
            Urn.create_from_string("urn:li:abc:")

        with self.assertRaises(InvalidUrnError):
            Urn.create_from_string("urn:li:abc:()")

        with self.assertRaises(InvalidUrnError):
            Urn.create_from_string("urn:li:abc:(abc,)")
