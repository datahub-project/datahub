# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import unittest

import pytest

from datahub.utilities.urns.corpuser_urn import CorpuserUrn


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
