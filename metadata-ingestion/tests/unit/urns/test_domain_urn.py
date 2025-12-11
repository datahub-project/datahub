# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import unittest

import pytest

from datahub.utilities.urns.domain_urn import DomainUrn


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestDomainUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        domain_urn_str = "urn:li:domain:abc"
        domain_urn = DomainUrn.create_from_string(domain_urn_str)
        assert domain_urn.get_type() == DomainUrn.ENTITY_TYPE

        assert domain_urn.get_entity_id() == ["abc"]
        assert str(domain_urn) == domain_urn_str
        assert domain_urn == DomainUrn("abc")
        assert domain_urn == DomainUrn.create_from_id("abc")
