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
