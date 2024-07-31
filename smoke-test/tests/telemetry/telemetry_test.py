import json

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.ingestion.graph.client import get_default_graph


def test_no_client_id():
    client_id_urn = "urn:li:telemetry:clientId"
    aspect = [
        "clientId"
    ]  # this is checking for the removal of the invalid aspect RemoveClientIdAspectStep.java

    client = get_default_graph()

    res_data = json.dumps(
        get_aspects_for_entity(
            session=client._session,
            gms_host=client.config.server,
            entity_urn=client_id_urn,
            aspects=aspect,
            typed=False,
        )
    )
    assert res_data == "{}"


def test_no_telemetry_client_id():
    client_id_urn = "urn:li:telemetry:clientId"
    aspect = ["telemetryClientId"]  # telemetry expected to be disabled for tests

    client = get_default_graph()

    res_data = json.dumps(
        get_aspects_for_entity(
            session=client._session,
            gms_host=client.config.server,
            entity_urn=client_id_urn,
            aspects=aspect,
            typed=False,
        )
    )
    assert res_data == "{}"
