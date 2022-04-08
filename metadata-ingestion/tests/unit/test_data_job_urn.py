import unittest

from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.error import InvalidUrnError


class TestDataJobUrnUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        data_job_urn_str = "urn:li:dataJob:abc"
        data_job_urn = DataJobUrn.create_from_string(data_job_urn_str)
        assert data_job_urn.get_type() == DataJobUrn.ENTITY_TYPE

        assert data_job_urn.get_entity_id() == ["abc"]
        assert str(data_job_urn) == data_job_urn_str
        assert data_job_urn == DataJobUrn("dataJob", ["abc"])
        assert data_job_urn == DataJobUrn.create_from_id("abc")

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            DataJobUrn.create_from_string(
                "urn:li:abc:(urn:li:dataPlatform:abc,def,prod)"
            )

        with self.assertRaises(InvalidUrnError):
            DataJobUrn.create_from_string("urn:li:dataJob:(part1,part2)")
