import json

from datahub.cli.cli_utils import get_aspects_for_entity


def test_no_clientID():
    client_id_urn = "urn:li:telemetry:clientId"
    aspect = ["clientId"]  # this is checking for the removal of the invalid aspect RemoveClientIdAspectStep.java

    res_data = json.dumps(
        get_aspects_for_entity(entity_urn=client_id_urn, aspects=aspect, typed=False)
    )
    assert res_data == "{}"
