import json
import pprint

with open(
    "./metadata-service/war/src/main/resources/boot/policies.json"
) as policies_file:
    all_policies = json.loads(policies_file.read())

metadata_policies = []
platform_policies = []
other_policies = []
without_info = []

metadata_privileges = set()
platform_privileges = set()
for policy in all_policies:
    urn = policy["urn"]
    if urn == "urn:li:dataHubPolicy:0":
        root_user_platform_policy_privileges = policy["info"]["privileges"]
    elif urn == "urn:li:dataHubPolicy:editor-platform-policy":
        editor_platform_policy_privileges = policy["info"]["privileges"]
    elif urn == "urn:li:dataHubPolicy:7":
        all_user_platform_policy_privileges = policy["info"]["privileges"]
    try:
        doc_type = policy["info"]["type"]
        privileges = policy["info"]["privileges"]
        if doc_type == "METADATA":
            metadata_policies.append(policy)
            metadata_privileges.update(privileges)
        elif doc_type == "PLATFORM":
            platform_policies.append(policy)
            platform_privileges.update(privileges)
        else:
            other_policies.append(policy)
    except:
        without_info.append(policy)
        pprint.pprint(policy)

print(
    f"""
    Number of policies is {len(all_policies)}
    Number of metadata_policies is {len(metadata_policies)}
    Number of platform_policies is {len(platform_policies)}
    Number of other is {len(other_policies)}
    Number without info is {len(without_info)}

    Number metadata privileges are {len(metadata_privileges)}
    Number platform privileges are {len(platform_privileges)}
"""
)

diff_policies = set(platform_privileges).difference(
    set(root_user_platform_policy_privileges)
)
assert len(diff_policies) == 0, f"Missing privileges for root user are {diff_policies}"

# All users privileges checks
assert "MANAGE_POLICIES" not in all_user_platform_policy_privileges
assert "MANAGE_USERS_AND_GROUPS" not in all_user_platform_policy_privileges
assert "MANAGE_SECRETS" not in all_user_platform_policy_privileges
assert "MANAGE_USER_CREDENTIALS" not in all_user_platform_policy_privileges
assert "MANAGE_ACCESS_TOKENS" not in all_user_platform_policy_privileges
assert "EDIT_ENTITY" not in all_user_platform_policy_privileges
assert "DELETE_ENTITY" not in all_user_platform_policy_privileges

# Editor checks
assert "MANAGE_POLICIES" not in editor_platform_policy_privileges
assert "MANAGE_USERS_AND_GROUPS" not in editor_platform_policy_privileges
assert "MANAGE_SECRETS" not in editor_platform_policy_privileges
assert "MANAGE_USER_CREDENTIALS" not in editor_platform_policy_privileges
assert "MANAGE_ACCESS_TOKENS" not in editor_platform_policy_privileges
# These don't prevent a user from modifying entities they are an asset owner of, i.e. their own profile info
assert "EDIT_CONTACT_INFO" not in editor_platform_policy_privileges
assert "EDIT_USER_PROFILE" not in editor_platform_policy_privileges
assert "EDIT_ENTITY_OWNERS" not in editor_platform_policy_privileges
