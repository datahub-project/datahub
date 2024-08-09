from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import get_admin_credentials, get_frontend_url, login_as


def set_base_platform_privileges_policy_status(status, session):
    base_platform_privileges = {
        "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
            "urn": "urn:li:dataHubPolicy:7",
            "input": {
                "type": "PLATFORM",
                "state": status,
                "name": "All Users - Base Platform Privileges",
                "description": "Grants base platform privileges to ALL users of DataHub. Change this policy to alter that behavior.",
                "privileges": [
                    "MANAGE_INGESTION",
                    "MANAGE_SECRETS",
                    "MANAGE_USERS_AND_GROUPS",
                    "VIEW_ANALYTICS",
                    "GENERATE_PERSONAL_ACCESS_TOKENS",
                    "MANAGE_DOMAINS",
                    "MANAGE_GLOBAL_ANNOUNCEMENTS",
                    "MANAGE_TESTS",
                    "MANAGE_GLOSSARIES",
                    "MANAGE_TAGS",
                    "MANAGE_GLOBAL_VIEWS",
                    "MANAGE_GLOBAL_OWNERSHIP_TYPES",
                ],
                "actors": {
                    "users": [],
                    "groups": None,
                    "resourceOwners": False,
                    "allUsers": True,
                    "allGroups": False,
                    "resourceOwnersTypes": None,
                },
            },
        },
    }
    base_privileges_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=base_platform_privileges
    )
    base_privileges_response.raise_for_status()
    base_res_data = base_privileges_response.json()
    assert base_res_data["data"]["updatePolicy"] == "urn:li:dataHubPolicy:7"


def set_view_dataset_sensitive_info_policy_status(status, session):
    dataset_sensitive_information = {
        "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
            "urn": "urn:li:dataHubPolicy:view-dataset-sensitive",
            "input": {
                "type": "METADATA",
                "state": status,
                "name": "All Users - View Dataset Sensitive Information",
                "description": "Grants viewing privileges of usage and profile information of all datasets for all users",
                "privileges": ["VIEW_DATASET_USAGE", "VIEW_DATASET_PROFILE"],
                "actors": {
                    "users": [],
                    "groups": None,
                    "resourceOwners": False,
                    "allUsers": True,
                    "allGroups": False,
                    "resourceOwnersTypes": None,
                },
            },
        },
    }
    sensitive_info_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=dataset_sensitive_information
    )
    sensitive_info_response.raise_for_status()
    sens_info_data = sensitive_info_response.json()
    assert (
        sens_info_data["data"]["updatePolicy"]
        == "urn:li:dataHubPolicy:view-dataset-sensitive"
    )


def set_view_entity_profile_privileges_policy_status(status, session):
    view_entity_page = {
        "query": """mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
            "urn": "urn:li:dataHubPolicy:view-entity-page-all",
            "input": {
                "type": "METADATA",
                "state": status,
                "name": "All Users - View Entity Page",
                "description": "Grants entity view to all users",
                "privileges": [
                    "VIEW_ENTITY_PAGE",
                    "SEARCH_PRIVILEGE",
                    "GET_COUNTS_PRIVILEGE",
                    "GET_TIMESERIES_ASPECT_PRIVILEGE",
                    "GET_ENTITY_PRIVILEGE",
                    "GET_TIMELINE_PRIVILEGE",
                ],
                "actors": {
                    "users": [],
                    "groups": None,
                    "resourceOwners": False,
                    "allUsers": True,
                    "allGroups": False,
                    "resourceOwnersTypes": None,
                },
            },
        },
    }
    view_entity_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=view_entity_page
    )
    view_entity_response.raise_for_status()
    view_entity_data = view_entity_response.json()
    assert (
        view_entity_data["data"]["updatePolicy"]
        == "urn:li:dataHubPolicy:view-entity-page-all"
    )


def create_user(session, email, password):
    # Remove user if exists
    res_data = remove_user(session, f"urn:li:corpuser:{email}")
    assert res_data
    assert "error" not in res_data
    # Get the invite token
    get_invite_token_json = {
        "query": """query getInviteToken($input: GetInviteTokenInput!) {\n
            getInviteToken(input: $input){\n
              inviteToken\n
            }\n
        }""",
        "variables": {"input": {}},
    }
    get_invite_token_response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_invite_token_json
    )
    get_invite_token_response.raise_for_status()
    get_invite_token_res_data = get_invite_token_response.json()
    invite_token = get_invite_token_res_data["data"]["getInviteToken"]["inviteToken"]
    assert invite_token is not None
    assert "error" not in invite_token
    # Create a new user using the invite token
    sign_up_json = {
        "fullName": "Test User",
        "email": email,
        "password": password,
        "title": "Data Engineer",
        "inviteToken": invite_token,
    }
    sign_up_response = session.post(f"{get_frontend_url()}/signUp", json=sign_up_json)
    sign_up_response.raise_for_status()
    assert sign_up_response
    assert "error" not in sign_up_response
    wait_for_writes_to_sync()
    session.cookies.clear()
    (admin_user, admin_pass) = get_admin_credentials()
    admin_session = login_as(admin_user, admin_pass)
    return admin_session


def remove_user(session, urn):
    json = {
        "query": """mutation removeUser($urn: String!) {\n
            removeUser(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    return response.json()


def create_group(session, name):
    json = {
        "query": """mutation createGroup($input: CreateGroupInput!) {\n
            createGroup(input: $input)
        }""",
        "variables": {"input": {"name": name}},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createGroup"]
    return res_data["data"]["createGroup"]


def remove_group(session, urn):
    json = {
        "query": """mutation removeGroup($urn: String!) {\n
            removeGroup(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeGroup"]
    return res_data["data"]["removeGroup"]


def assign_user_to_group(session, group_urn, user_urns):
    json = {
        "query": """mutation addGroupMembers($groupUrn: String!, $userUrns: [String!]!) {\n
            addGroupMembers(input: { groupUrn: $groupUrn, userUrns: $userUrns })
        }""",
        "variables": {"groupUrn": group_urn, "userUrns": user_urns},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addGroupMembers"]
    return res_data["data"]["addGroupMembers"]


def assign_role(session, role_urn, actor_urns):
    json = {
        "query": """mutation batchAssignRole($input: BatchAssignRoleInput!) {\n
            batchAssignRole(input: $input)
        }""",
        "variables": {"input": {"roleUrn": role_urn, "actors": actor_urns}},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["batchAssignRole"]
    return res_data["data"]["batchAssignRole"]


def create_user_policy(user_urn, privileges, session):
    policy = {
        "query": """mutation createPolicy($input: PolicyUpdateInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "PLATFORM",
                "name": "Test Policy Name",
                "description": "Test Policy Description",
                "state": "ACTIVE",
                "resources": {"filter": {"criteria": []}},
                "privileges": privileges,
                "actors": {
                    "users": [user_urn],
                    "resourceOwners": False,
                    "allUsers": False,
                    "allGroups": False,
                },
            }
        },
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=policy)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]
    return res_data["data"]["createPolicy"]


def remove_policy(urn, session):
    remove_policy_json = {
        "query": """mutation deletePolicy($urn: String!) {\n
            deletePolicy(urn: $urn) }""",
        "variables": {"urn": urn},
    }

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=remove_policy_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deletePolicy"]
    assert res_data["data"]["deletePolicy"] == urn


def clear_polices(session):
    list_policy_json = {
        "query": """query listPolicies($input: ListPoliciesInput!) {
                      listPolicies(input: $input) {
                        start
                        count
                        total
                        policies {
                          urn
                          editable
                          name
                          description
                          __typename
                        }
                        __typename
                      }
                    }""",
        "variables": {
            "input": {
                "count": 100,
                "start": 0,
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "state",
                                "values": ["ACTIVE"],
                                "condition": "EQUAL",
                            },
                            {
                                "field": "editable",
                                "values": ["true"],
                                "condition": "EQUAL",
                            },
                        ]
                    }
                ],
            }
        },
    }

    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_policy_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]
    for policy in res_data["data"]["listPolicies"]["policies"]:
        if "test" in policy["name"].lower() or "test" in policy["description"].lower():
            remove_policy(policy["urn"], session)


def remove_secret(session, urn):
    remove_secret = {
        "query": """mutation deleteSecret($urn: String!) {\n
            deleteSecret(urn: $urn)\n}""",
        "variables": {"urn": urn},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=remove_secret)
    response.raise_for_status()
