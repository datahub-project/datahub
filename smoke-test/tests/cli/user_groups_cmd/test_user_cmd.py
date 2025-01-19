import json
import sys
import tempfile
from typing import Any, Dict, Iterable, List

import yaml
from click.testing import CliRunner, Result

from datahub.api.entities.corpuser.corpuser import CorpUser
from datahub.entrypoints import datahub

runner = CliRunner(mix_stderr=False)


def datahub_upsert_user(auth_session, user: CorpUser) -> None:
    with tempfile.NamedTemporaryFile("w+t", suffix=".yaml") as user_file:
        yaml.dump(user.dict(), user_file)
        user_file.flush()
        upsert_args: List[str] = [
            "user",
            "upsert",
            "-f",
            user_file.name,
        ]
        user_create_result = runner.invoke(
            datahub,
            upsert_args,
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        assert user_create_result.exit_code == 0


def gen_datahub_users(num_users: int) -> Iterable[CorpUser]:
    for i in range(0, num_users):
        user = CorpUser(
            id=f"user_{i}",
            display_name=f"User {i}",
            email=f"user_{i}@datahubproject.io",
            title=f"User {i}",
            first_name="User",
            last_name=f"{i}",
            groups=[f"urn:li:corpGroup:group_{i}"],
            description=f"The User {i}",
            slack=f"@user{i}",
            picture_link=f"https://images.google.com/user{i}.jpg",
            phone=f"1-800-USER-{i}",
        )
        yield user


def datahub_get_user(auth_session: Any, user_urn: str):
    get_args: List[str] = ["get", "--urn", user_urn]
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


def test_user_upsert(auth_session: Any) -> None:
    num_user_profiles: int = 10
    for i, datahub_user in enumerate(gen_datahub_users(num_user_profiles)):
        datahub_upsert_user(auth_session, datahub_user)
        user_dict = datahub_get_user(auth_session, f"urn:li:corpuser:user_{i}")
        assert user_dict == {
            "corpUserEditableInfo": {
                "aboutMe": f"The User {i}",
                "displayName": f"User {i}",
                "email": f"user_{i}@datahubproject.io",
                "phone": f"1-800-USER-{i}",
                "pictureLink": f"https://images.google.com/user{i}.jpg",
                "skills": [],
                "slack": f"@user{i}",
                "teams": [],
            },
            "corpUserKey": {"username": f"user_{i}"},
            "groupMembership": {"groups": [f"urn:li:corpGroup:group_{i}"]},
            "status": {"removed": False},
        }
