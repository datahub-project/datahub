# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utilities.metadata_operations import list_policies


@pytest.mark.read_only
def test_policies_are_accessible(auth_session):
    res_data = list_policies(auth_session)
    assert res_data, f"Received listPolicies were {res_data}"
    assert res_data["total"] > 0, f"Total was {res_data['total']}"
    add_datahub_stats("num-policies", res_data["total"])
