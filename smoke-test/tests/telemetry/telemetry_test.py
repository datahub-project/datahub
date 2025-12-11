# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import json

from datahub.cli.cli_utils import get_aspects_for_entity


def test_no_client_id(graph_client):
    client_id_urn = "urn:li:telemetry:clientId"
    aspect = [
        "clientId"
    ]  # this is checking for the removal of the invalid aspect RemoveClientIdAspectStep.java

    res_data = json.dumps(
        get_aspects_for_entity(
            session=graph_client._session,
            gms_host=graph_client.config.server,
            entity_urn=client_id_urn,
            aspects=aspect,
            typed=False,
        )
    )
    assert res_data == "{}"


def test_no_telemetry_client_id(graph_client):
    client_id_urn = "urn:li:telemetry:clientId"
    aspect = ["telemetryClientId"]  # telemetry expected to be disabled for tests

    res_data = json.dumps(
        get_aspects_for_entity(
            session=graph_client._session,
            gms_host=graph_client.config.server,
            entity_urn=client_id_urn,
            aspects=aspect,
            typed=False,
        )
    )
    assert res_data == "{}"
