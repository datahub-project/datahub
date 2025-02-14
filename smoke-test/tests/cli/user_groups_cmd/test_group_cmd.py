import json
import sys
import tempfile
from typing import Any, Dict, Iterable, List

import yaml
from click.testing import CliRunner, Result

from datahub.api.entities.corpgroup.corpgroup import CorpGroup
from datahub.entrypoints import datahub
from datahub.ingestion.graph.client import DataHubGraph
from tests.utils import wait_for_writes_to_sync

runner = CliRunner(mix_stderr=False)


def sync_elastic() -> None:
    wait_for_writes_to_sync()


def datahub_upsert_group(auth_session: Any, group: CorpGroup) -> None:
    with tempfile.NamedTemporaryFile("w+t", suffix=".yaml") as group_file:
        yaml.dump(group.dict(), group_file)
        group_file.flush()
        upsert_args: List[str] = [
            "group",
            "upsert",
            "-f",
            group_file.name,
        ]
        group_create_result = runner.invoke(
            datahub,
            upsert_args,
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        assert group_create_result.exit_code == 0


def gen_datahub_groups(num_groups: int) -> Iterable[CorpGroup]:
    for i in range(0, num_groups):
        group = CorpGroup(
            id=f"group_{i}",
            display_name=f"Group {i}",
            email=f"group_{i}@datahubproject.io",
            description=f"The Group {i}",
            picture_link=f"https://images.google.com/group{i}.jpg",
            slack=f"@group{i}",
            owners=["user1"],
            members=["user2"],
        )
        yield group


def datahub_get_group(auth_session, group_urn: str):
    get_args: List[str] = ["get", "--urn", group_urn]
    get_result: Result = runner.invoke(
        datahub,
        get_args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )
    assert get_result.exit_code == 0
    try:
        get_result_output_obj: Dict = json.loads(get_result.stdout)
        return get_result_output_obj
    except json.JSONDecodeError as e:
        print("Failed to decode: " + get_result.stdout, file=sys.stderr)
        raise e


def get_group_ownership(graph_client: DataHubGraph, user_urn: str) -> List[str]:
    entities = graph_client.get_related_entities(
        entity_urn=user_urn,
        relationship_types=["OwnedBy"],
        direction=DataHubGraph.RelationshipDirection.INCOMING,
    )
    return [entity.urn for entity in entities]


def get_group_membership(graph_client: DataHubGraph, user_urn: str) -> List[str]:
    entities = graph_client.get_related_entities(
        entity_urn=user_urn,
        relationship_types=["IsMemberOfGroup"],
        direction=DataHubGraph.RelationshipDirection.OUTGOING,
    )
    return [entity.urn for entity in entities]


def test_group_upsert(auth_session: Any, graph_client: DataHubGraph) -> None:
    num_groups: int = 10
    for i, datahub_group in enumerate(gen_datahub_groups(num_groups)):
        datahub_upsert_group(auth_session, datahub_group)
        group_dict = datahub_get_group(auth_session, f"urn:li:corpGroup:group_{i}")
        assert group_dict == {
            "corpGroupEditableInfo": {
                "description": f"The Group {i}",
                "email": f"group_{i}@datahubproject.io",
                "pictureLink": f"https://images.google.com/group{i}.jpg",
                "slack": f"@group{i}",
            },
            "corpGroupInfo": {
                "admins": ["urn:li:corpuser:user1"],
                "description": f"The Group {i}",
                "displayName": f"Group {i}",
                "email": f"group_{i}@datahubproject.io",
                "groups": [],
                "members": ["urn:li:corpuser:user2"],
                "slack": f"@group{i}",
            },
            "corpGroupKey": {"name": f"group_{i}"},
            "ownership": {
                "lastModified": {"actor": "urn:li:corpuser:unknown", "time": 0},
                "owners": [
                    {"owner": "urn:li:corpuser:user1", "type": "TECHNICAL_OWNER"}
                ],
                "ownerTypes": {},
            },
            "status": {"removed": False},
        }

    sync_elastic()
    groups_owned = get_group_ownership(graph_client, "urn:li:corpuser:user1")
    groups_partof = get_group_membership(graph_client, "urn:li:corpuser:user2")

    all_groups = sorted([f"urn:li:corpGroup:group_{i}" for i in range(0, num_groups)])

    assert sorted(groups_owned) == all_groups
    assert sorted(groups_partof) == all_groups
