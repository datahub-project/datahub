import json
import sys
import tempfile
from typing import Any, Dict, Iterable, List
from click.testing import CliRunner, Result
import time
from datahub.api.entities.corpgroup.corpgroup import CorpGroup
import yaml
from datahub.entrypoints import datahub

runner = CliRunner(mix_stderr=False)


def datahub_upsert_group(group: CorpGroup) -> None:
    with tempfile.NamedTemporaryFile("w+t", suffix=".yaml") as group_file:
        yaml.dump(group.dict(), group_file)
        group_file.flush()
        upsert_args: List[str] = [
            "group",
            "upsert",
            "-f",
            group_file.name,
        ]
        group_create_result = runner.invoke(datahub, upsert_args)
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
            admins=[
            "user1"
            ],
            members=[
            "user2"
            ]
        )
        yield group

def datahub_get_group(group_urn: str):
    get_args: List[str] = ["get", "--urn", group_urn]
    get_result: Result = runner.invoke(datahub, get_args)
    assert get_result.exit_code == 0
    try:
        get_result_output_obj: Dict = json.loads(get_result.stdout)
        return get_result_output_obj
    except json.JSONDecodeError as e:
        print("Failed to decode: " + get_result.stdout, file=sys.stderr)
        raise e

def test_group_upsert(wait_for_healthchecks: Any) -> None:
    num_groups: int = 10
    for i, datahub_group in enumerate(gen_datahub_groups(num_groups)):
        datahub_upsert_group(datahub_group)
        # Validate against all ingested values once every verification_batch_size to reduce overall test time. Since we
        # are ingesting  the aspects in the ascending order of timestampMillis, get should return the one just put.
        #sync_elastic()
        group_dict = datahub_get_group(f"urn:li:corpGroup:group_{i}")
        assert group_dict =={'corpGroupEditableInfo': {'description': f'The Group {i}', 'email': f'group_{i}@datahubproject.io', 'pictureLink': f'https://images.google.com/group{i}.jpg', 'slack': f'@group{i}'}, 'corpGroupInfo': {'admins': ['urn:li:corpuser:user1'], 'description': f'The Group {i}', 'displayName': f'Group {i}', 'email': f'group_{i}@datahubproject.io', 'groups': [], 'members': ['urn:li:corpuser:user2'], 'slack': f'@group{i}'}, 'corpGroupKey': {'name': f'group_{i}'}, 'ownership': {'lastModified': {'actor': 'urn:li:corpuser:unknown', 'time': 0}, 'owners': [{'owner': 'urn:li:corpuser:user1', 'type': 'TECHNICAL_OWNER'}]}}
        