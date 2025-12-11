# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import unittest

import pytest

from datahub.utilities.urns.data_flow_urn import DataFlowUrn


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestDataFlowUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        data_flow_urn_str = "urn:li:dataFlow:(airflow,def,prod)"
        data_flow_urn = DataFlowUrn.create_from_string(data_flow_urn_str)
        assert data_flow_urn.get_orchestrator_name() == "airflow"
        assert data_flow_urn.get_flow_id() == "def"
        assert data_flow_urn.get_env() == "prod"
        assert data_flow_urn.__str__() == "urn:li:dataFlow:(airflow,def,prod)"
        assert data_flow_urn == DataFlowUrn("airflow", "def", "prod")
