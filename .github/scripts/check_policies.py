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
root_user_platform_policy_privileges = set()
root_user_all_privileges = set()
admin_role_platform_privileges = set()
admin_role_all_privileges = set()
reader_role_all_privileges = set()
editor_role_all_privileges = set()
for policy in all_policies:
    urn = policy["urn"]
    if urn == "urn:li:dataHubPolicy:0":
        root_user_platform_policy_privileges = policy["info"]["privileges"]
        root_user_all_privileges.update(set(root_user_platform_policy_privileges))
    elif urn == "urn:li:dataHubPolicy:1":
        root_user_all_privileges.update(set(policy["info"]["privileges"]))
    elif urn == "urn:li:dataHubPolicy:admin-platform-policy":
        admin_role_platform_privileges = policy["info"]["privileges"]
        admin_role_all_privileges.update(set(admin_role_platform_privileges))
    elif urn == "urn:li:dataHubPolicy:admin-metadata-policy":
        admin_role_all_privileges.update(set(policy["info"]["privileges"]))
    elif urn == "urn:li:dataHubPolicy:editor-platform-policy":
        editor_platform_policy_privileges = policy["info"]["privileges"]
    elif urn == "urn:li:dataHubPolicy:7":
        all_user_platform_policy_privileges = policy["info"]["privileges"]
    elif urn.startswith("urn:li:dataHubPolicy:reader-"):
        reader_role_all_privileges.update(set(policy["info"]["privileges"]))
    elif urn.startswith("urn:li:dataHubPolicy:editor-"):
        editor_role_all_privileges.update(set(policy["info"]["privileges"]))
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

# Root user has all privileges
diff_policies = set(platform_privileges).difference(
    set(root_user_platform_policy_privileges)
)
assert len(diff_policies) == 0, f"Missing privileges for root user are {diff_policies}"

# admin role and root user have same platform privileges
diff_root_missing_from_admin = set(root_user_platform_policy_privileges).difference(set(admin_role_platform_privileges))
diff_admin_missing_from_root = set(admin_role_platform_privileges).difference(set(root_user_platform_policy_privileges))

assert len(diff_root_missing_from_admin) == 0, f"Admin role missing: {diff_root_missing_from_admin}"
assert len(diff_admin_missing_from_root) == 0, f"Root user missing: {diff_admin_missing_from_root}"

# admin role and root user have same privileges
diff_root_missing_from_admin_all = set(root_user_all_privileges).difference(set(admin_role_all_privileges))
diff_admin_missing_from_root_all = set(admin_role_all_privileges).difference(set(root_user_all_privileges))
## Admin user has EDIT_ENTITY privilege which is super privilege for editing entities
diff_admin_missing_from_root_all_new = set()
for privilege in diff_admin_missing_from_root_all:
    if privilege.startswith("EDIT_"):
        continue
    diff_admin_missing_from_root_all_new.add(privilege)
diff_admin_missing_from_root_all = diff_admin_missing_from_root_all_new

assert len(diff_root_missing_from_admin_all) == 0, f"Admin role missing: {diff_root_missing_from_admin_all}"
assert len(diff_admin_missing_from_root_all) == 0, f"Root user missing: {diff_admin_missing_from_root_all}"

# Editor role has all privielges of Reader
diff_reader_missing_from_editor = set(reader_role_all_privileges).difference(set(editor_role_all_privileges))
assert len(diff_reader_missing_from_editor) == 0, f"Editor role missing: {diff_reader_missing_from_editor}"

# Admin role has all privileges of editor
diff_editor_missing_from_admin = set(editor_role_all_privileges).difference(set(admin_role_all_privileges))
assert len(diff_editor_missing_from_admin) == 0, f"Admin role missing: {diff_editor_missing_from_admin}"

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
