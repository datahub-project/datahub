# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import unittest

import pytest

from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestDataJobUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        data_job_urn_str = (
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow_id,prod),job_id)"
        )
        data_job_urn = DataJobUrn.create_from_string(data_job_urn_str)
        assert data_job_urn.get_data_flow_urn() == DataFlowUrn.create_from_string(
            "urn:li:dataFlow:(airflow,flow_id,prod)"
        )
        assert data_job_urn.get_job_id() == "job_id"
        assert data_job_urn.__str__() == data_job_urn_str
        assert data_job_urn == DataJobUrn(
            "urn:li:dataFlow:(airflow,flow_id,prod)", "job_id"
        )
