import unittest

import pytest

from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestDatasetUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        dataset_urn_str = "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        dataset_urn = DatasetUrn.create_from_string(dataset_urn_str)
        assert (
            dataset_urn.get_data_platform_urn()
            == DataPlatformUrn.create_from_string("urn:li:dataPlatform:abc")
        )
        assert dataset_urn.get_dataset_name() == "def"
        assert dataset_urn.get_env() == "PROD"
        assert dataset_urn.__str__() == dataset_urn_str
        assert dataset_urn == DatasetUrn("urn:li:dataPlatform:abc", "def", "prod")

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            DatasetUrn.create_from_string(
                "urn:li:abc:(urn:li:dataPlatform:abc,def,prod)"
            )

        with self.assertRaises(InvalidUrnError):
            DatasetUrn.create_from_string(
                "urn:li:dataset:(urn:li:user:abc,dataset,prod)"
            )

        with self.assertRaises(InvalidUrnError):
            DatasetUrn.create_from_string("urn:li:dataset:(urn:li:user:abc,dataset)")

        with self.assertRaises(InvalidUrnError):
            DatasetUrn.create_from_string(
                "urn:li:dataset:(urn:li:user:abc,dataset,invalidEnv)"
            )
