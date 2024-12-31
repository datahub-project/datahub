import unittest

import pytest

from datahub.utilities.urns.data_process_instance_urn import DataProcessInstanceUrn


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestDataProcessInstanceUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        dataprocessinstance_urn_str = "urn:li:dataProcessInstance:abc"
        dataprocessinstance_urn = DataProcessInstanceUrn.create_from_string(
            dataprocessinstance_urn_str
        )
        assert dataprocessinstance_urn.get_type() == DataProcessInstanceUrn.ENTITY_TYPE

        assert dataprocessinstance_urn.get_entity_id() == ["abc"]
        assert str(dataprocessinstance_urn) == dataprocessinstance_urn_str
        assert dataprocessinstance_urn == DataProcessInstanceUrn("abc")
        assert dataprocessinstance_urn == DataProcessInstanceUrn.create_from_id("abc")
        assert "abc" == dataprocessinstance_urn.get_dataprocessinstance_id()
