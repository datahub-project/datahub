import json
import sys
import tempfile
import time
from typing import Any, Dict, Iterable, List

import yaml

from datahub.api.entities.corpuser.corpuser import CorpUser
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import run_datahub_cmd


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
        user_create_result = run_datahub_cmd(
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
    get_result = run_datahub_cmd(
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
    max_retries: int = 3
    backoff_seconds: float = 1.0

    # Create users first
    for _i, datahub_user in enumerate(gen_datahub_users(num_user_profiles)):
        datahub_upsert_user(auth_session, datahub_user)

    # Initial wait for writes
    wait_for_writes_to_sync()

    # Retry logic for getting all users and assertion
    for attempt in range(max_retries):
        try:
            actual = []
            expected = []

            # Collect all user data
            for _i in range(num_user_profiles):
                user_dict = datahub_get_user(auth_session, f"urn:li:corpuser:user_{_i}")
                actual.append(user_dict)
                expected.append(
                    {
                        "corpUserEditableInfo": {
                            "aboutMe": f"The User {_i}",
                            "displayName": f"User {_i}",
                            "email": f"user_{_i}@datahubproject.io",
                            "phone": f"1-800-USER-{_i}",
                            "pictureLink": f"https://images.google.com/user{_i}.jpg",
                            "skills": [],
                            "slack": f"@user{_i}",
                            "teams": [],
                        },
                        "corpUserKey": {"username": f"user_{_i}"},
                        "groupMembership": {"groups": [f"urn:li:corpGroup:group_{_i}"]},
                        "status": {"removed": False},
                    }
                )

            # Validate core user data (without corpUserSettings)
            for i, (actual_user, expected_user) in enumerate(
                zip(actual, expected, strict=False)
            ):
                # Create a copy to avoid mutating original data
                actual_user_copy = dict(actual_user)
                # Pop out the settings, we'll validate them separately.
                actual_settings = actual_user_copy.pop("corpUserSettings", None)

                # Compare the rest of the user data
                assert actual_user_copy == expected_user, f"User {i} core data mismatch"

                # Validate corpUserSettings structure exists and has key components
                assert actual_settings is not None, f"User {i} missing corpUserSettings"
                assert "appearance" in actual_settings, (
                    f"User {i} missing appearance settings"
                )
                assert "notificationSettings" in actual_settings, (
                    f"User {i} missing notification settings"
                )

                # Validate notification settings structure
                notification_settings = actual_settings["notificationSettings"]
                assert "emailSettings" in notification_settings, (
                    f"User {i} missing email settings"
                )
                assert "settings" in notification_settings, (
                    f"User {i} missing notification settings map"
                )
                assert "sinkTypes" in notification_settings, (
                    f"User {i} missing sink types"
                )
                assert "slackSettings" in notification_settings, (
                    f"User {i} missing slack settings"
                )

                # Validate that expected notification types exist (without caring about order)
                settings_map = notification_settings["settings"]
                expected_notification_types = [
                    "DATA_HUB_COMMUNITY_UPDATES",
                    "NEW_ACTION_WORKFLOW_FORM_REQUEST",
                    "NEW_PROPOSAL",
                    "PROPOSAL_STATUS_CHANGE",
                    "PROPOSER_PROPOSAL_STATUS_CHANGE",
                    "REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
                ]

                for notification_type in expected_notification_types:
                    assert notification_type in settings_map, (
                        f"User {i} missing {notification_type} notification setting"
                    )
                    setting = settings_map[notification_type]
                    assert "value" in setting and setting["value"] == "ENABLED", (
                        f"User {i} {notification_type} not enabled"
                    )
                    assert "params" in setting, (
                        f"User {i} {notification_type} missing params"
                    )

            # If we get here, all validations passed
            return

        except AssertionError:
            if attempt < max_retries - 1:
                wait_time = backoff_seconds * (2**attempt)
                print(
                    f"Assertion failed on attempt {attempt + 1}, waiting {wait_time} seconds before retrying..."
                )
                # Wait before retrying
                time.sleep(wait_time)
                # Additional sync wait
                wait_for_writes_to_sync()
            else:
                # On last attempt, let the assertion error propagate
                raise
