# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import unittest

import pytest

from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn


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
