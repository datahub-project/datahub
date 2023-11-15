import unittest

from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.error import InvalidUrnError


class TestDataFlowUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        data_flow_urn_str = "urn:li:dataFlow:(airflow,def,prod)"
        data_flow_urn = DataFlowUrn.create_from_string(data_flow_urn_str)
        assert data_flow_urn.get_orchestrator_name() == "airflow"
        assert data_flow_urn.get_flow_id() == "def"
        assert data_flow_urn.get_env() == "prod"
        assert data_flow_urn.__str__() == "urn:li:dataFlow:(airflow,def,prod)"
        assert data_flow_urn == DataFlowUrn("dataFlow", ["airflow", "def", "prod"])

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            DataFlowUrn.create_from_string("urn:li:abc:(airflow,def,prod)")

        with self.assertRaises(InvalidUrnError):
            DataFlowUrn.create_from_string("urn:li:dataFlow:(airflow,flow_id)")

        with self.assertRaises(InvalidUrnError):
            DataFlowUrn.create_from_string(
                "urn:li:dataFlow:(airflow,flow_id,invalidEnv)"
            )
