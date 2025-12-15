import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Configuring LDAP Authentication

The DataHub React application supports LDAP authentication built on top of the [JaaS (Java Authentication and Authorization Service)](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html) framework.
This enables operators of DataHub to integrate with LDAP/Active Directory servers to authenticate their users using existing corporate credentials.

:::info
LDAP authentication in DataHub uses the same internal architecture as OIDC authentication, ensuring consistent behavior for user provisioning, group membership synchronization, and audit trail generation across all authentication methods.
:::

## 1. Configure your LDAP Server

Before you configure DataHub, ensure you have the following information from your LDAP/Active Directory administrator:

1. _LDAP Server URL_ - The URL of your LDAP server (e.g., `ldap://ldap.example.com:389` or `ldaps://ldap.example.com:636` for SSL)
2. _Base DN_ - The base Distinguished Name for searches (e.g., `DC=example,DC=com`)
3. _User Base DN_ - The base DN specifically for user searches (can be same as Base DN)
4. _User Filter_ - The LDAP filter to search for users (e.g., `(uid={0})`, `(sAMAccountName={0})`, or `(&(userPrincipalName={0}@domain.com)(objectClass=person))`)
5. _Group Base DN_ - The base DN for group searches (if group extraction is enabled)
6. _Group Filter_ - The LDAP filter to search for groups (e.g., `(member={0})` or `(&(objectClass=group)(member={0}))`)

:::info User Authentication
DataHub uses the **user's own credentials** (username and password entered during login) to authenticate to LDAP and extract user/group information. No service account credentials need to be stored in the configuration. This follows the principle of least privilege - each user can only access their own LDAP data based on LDAP ACLs.
:::

## 2. Configure DataHub Frontend Server

### Docker

The next step to enabling LDAP involves configuring `datahub-frontend` to enable LDAP authentication with your LDAP/Active Directory server.

To do so, you must update the `datahub-frontend` [docker.env](../../../../docker/datahub-frontend/env/docker.env) file with the
values for your LDAP server:

```
# Required Configuration Values:
AUTH_JAAS_ENABLED=true
AUTH_LDAP_ENABLED=true
AUTH_LDAP_SERVER=ldaps://ldap.example.com:636
AUTH_LDAP_BASE_DN=DC=example,DC=com
AUTH_LDAP_USER_BASE_DN=DC=example,DC=com
AUTH_LDAP_USER_FILTER=(&(userPrincipalName={0}@example.com)(objectClass=person))

# JIT Provisioning (enabled by default)
AUTH_LDAP_JIT_PROVISIONING_ENABLED=true

# Group Extraction (optional - enable to sync LDAP groups)
AUTH_LDAP_EXTRACT_GROUPS_ENABLED=true
AUTH_LDAP_GROUP_BASE_DN=DC=example,DC=com
AUTH_LDAP_GROUP_FILTER=(&(objectClass=group)(member={0}))
AUTH_LDAP_GROUP_NAME_ATTRIBUTE=cn
```

:::info No Service Account Required
Note that **no bind DN or bind password** is required in the configuration. DataHub uses the user's own credentials (entered during login) to authenticate to LDAP and extract user/group information. This is more secure as it follows the principle of least privilege.
:::

| Configuration                | Description                                                                                                                                                                                                                                                              | Default |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| AUTH_JAAS_ENABLED            | Enable JAAS authentication framework (required for LDAP)                                                                                                                                                                                                                 | true    |
| AUTH_LDAP_ENABLED            | Enable LDAP authentication                                                                                                                                                                                                                                               | false   |
| AUTH_LDAP_SERVER             | The URL of your LDAP server. Use `ldap://` for non-SSL or `ldaps://` for SSL connections                                                                                                                                                                                |         |
| AUTH_LDAP_BASE_DN            | The base Distinguished Name (DN) for all LDAP searches                                                                                                                                                                                                                   |         |
| AUTH_LDAP_USER_BASE_DN       | The base DN specifically for user searches. Can be same as BASE_DN or more specific (e.g., `OU=Users,DC=example,DC=com`)                                                                                                                                                 | Same as BASE_DN |
| AUTH_LDAP_USER_FILTER        | LDAP filter for user search. Use `{0}` as username placeholder. Examples: `(uid={0})` for OpenLDAP, `(sAMAccountName={0})` for AD, `(&(userPrincipalName={0}@domain.com)(objectClass=person))` for AD with UPN                                                         |         |
| AUTH_LDAP_JIT_PROVISIONING_ENABLED | Whether to automatically create users in DataHub on first login                                                                                                                                                                                                    | true    |
| AUTH_LDAP_EXTRACT_GROUPS_ENABLED | Whether to extract and sync LDAP groups to DataHub. When enabled, user's group memberships are synced from LDAP on each login                                                                                                                                     | false   |
| AUTH_LDAP_GROUP_BASE_DN      | The base DN for group searches (required if group extraction is enabled)                                                                                                                                                                                                 |         |
| AUTH_LDAP_GROUP_FILTER       | LDAP filter for group search. Use `{0}` as user DN placeholder. Examples: `(member={0})`, `(&(objectClass=group)(member={0}))` for AD                                                                                                                                    |         |
| AUTH_LDAP_GROUP_NAME_ATTRIBUTE | The LDAP attribute containing the group name                                                                                                                                                                                                                           | cn      |
| AUTH_SESSION_TTL_HOURS       | The length of time in hours before a user will be prompted to login again. Controls the actor cookie expiration time in the browser. Numeric value converted to hours.                                                                                                   | 24      |
| MAX_SESSION_TOKEN_AGE        | Determines the expiration time of a session token. Session tokens are stateless so this determines at what time a session token may no longer be used and a valid session token can be used until this time has passed. Accepts a valid relative Java date style String. | 24h     |

Providing these configs will cause DataHub to delegate authentication to your LDAP server, validating user credentials against the LDAP directory.

### Kubernetes

In Kubernetes, you can add the above env variables in the `values.yaml` as follows.

```yaml
datahub-frontend:
  ...
  extraEnvs:
    - name: AUTH_LDAP_ENABLED
      value: "true"
    - name: AUTH_LDAP_SERVER
      value: ldap://ldap.example.com:389
    - name: AUTH_LDAP_BASE_DN
      value: ou=users,dc=example,dc=com
    - name: AUTH_LDAP_BIND_DN
      value: cn=admin,dc=example,dc=com
    - name: AUTH_LDAP_BIND_PASSWORD
      value: your-bind-password
    - name: AUTH_LDAP_USER_FILTER
      value: "(uid={0})"
    - name: AUTH_LDAP_USER_ID_ATTRIBUTE
      value: uid
```

You can also package LDAP credentials into a k8s secret by running

```
kubectl create secret generic datahub-ldap-secret --from-literal=bind-password=<<LDAP BIND PASSWORD>>
```

Then set the secret env as follows.

```yaml
- name: AUTH_LDAP_BIND_PASSWORD
  valueFrom:
    secretKeyRef:
      name: datahub-ldap-secret
      key: bind-password
```

### Advanced LDAP Configurations

You can optionally customize the LDAP authentication flow further using advanced configurations. These allow
you to specify additional LDAP settings such as SSL/TLS configuration, connection pooling, and user attribute mapping.

```
# Optional Configuration Values:
AUTH_LDAP_USE_SSL=true
AUTH_LDAP_START_TLS=false
AUTH_LDAP_TRUST_ALL_CERTS=false
AUTH_LDAP_CONNECTION_TIMEOUT=5000
AUTH_LDAP_READ_TIMEOUT=5000
AUTH_LDAP_USER_FIRST_NAME_ATTRIBUTE=givenName
AUTH_LDAP_USER_LAST_NAME_ATTRIBUTE=sn
AUTH_LDAP_USER_EMAIL_ATTRIBUTE=mail
AUTH_LDAP_USER_DISPLAY_NAME_ATTRIBUTE=displayName
```

| Configuration                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                         | Default     |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| AUTH_LDAP_USE_SSL                      | Whether to use SSL (LDAPS) for the LDAP connection. If true, ensure your LDAP server URL uses `ldaps://`                                                                                                                                                                                                                                                                                                                            | false       |
| AUTH_LDAP_START_TLS                    | Whether to use StartTLS for the LDAP connection. This upgrades a non-SSL connection to SSL. Cannot be used with `AUTH_LDAP_USE_SSL`                                                                                                                                                                                                                                                                                                 | false       |
| AUTH_LDAP_TRUST_ALL_CERTS              | Whether to trust all SSL certificates. This should only be used for testing purposes and not in production                                                                                                                                                                                                                                                                                                                          | false       |
| AUTH_LDAP_CONNECTION_TIMEOUT           | The timeout in milliseconds for establishing an LDAP connection                                                                                                                                                                                                                                                                                                                                                                     | 5000        |
| AUTH_LDAP_READ_TIMEOUT                 | The timeout in milliseconds for LDAP read operations                                                                                                                                                                                                                                                                                                                                                                                | 5000        |
| AUTH_LDAP_USER_FIRST_NAME_ATTRIBUTE    | The LDAP attribute that contains the user's first name                                                                                                                                                                                                                                                                                                                                                                              | givenName   |
| AUTH_LDAP_USER_LAST_NAME_ATTRIBUTE     | The LDAP attribute that contains the user's last name                                                                                                                                                                                                                                                                                                                                                                               | sn          |
| AUTH_LDAP_USER_EMAIL_ATTRIBUTE         | The LDAP attribute that contains the user's email address                                                                                                                                                                                                                                                                                                                                                                           | mail        |
| AUTH_LDAP_USER_DISPLAY_NAME_ATTRIBUTE  | The LDAP attribute that contains the user's display name                                                                                                                                                                                                                                                                                                                                                                            | displayName |

### User & Group Provisioning (JIT Provisioning)

By default, DataHub will optimistically attempt to provision users that do not already exist at the time of login.
For users, we extract information like first name, last name, display name, & email from LDAP attributes to construct a basic user profile.

The default provisioning behavior can be customized using the following configs.

```
# User and groups provisioning
AUTH_LDAP_JIT_PROVISIONING_ENABLED=true
AUTH_LDAP_PRE_PROVISIONING_REQUIRED=false
AUTH_LDAP_EXTRACT_GROUPS_ENABLED=false
AUTH_LDAP_GROUP_BASE_DN=ou=groups,dc=example,dc=com
AUTH_LDAP_GROUP_FILTER=(member={0})
AUTH_LDAP_GROUP_NAME_ATTRIBUTE=cn
```

| Configuration                          | Description                                                                                                                                                                                                                                                                                                                                                      | Default |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| AUTH_LDAP_JIT_PROVISIONING_ENABLED     | Whether DataHub users should be provisioned on login if they do not exist.                                                                                                                                                                                                                                                                                       | true    |
| AUTH_LDAP_PRE_PROVISIONING_REQUIRED    | Whether the user should already exist in DataHub when they login, failing login if they are not. This is appropriate for situations in which users are batch ingested and tightly controlled inside your environment.                                                                                                                                            | false   |
| AUTH_LDAP_EXTRACT_GROUPS_ENABLED       | Only applies if `AUTH_LDAP_JIT_PROVISIONING_ENABLED` is set to true. This determines whether we should attempt to extract a list of groups from LDAP. Note that if this is enabled, each login will re-sync group membership with the groups in your LDAP directory, clearing the group membership that has been assigned through the DataHub UI. Enable with care! | false   |
| AUTH_LDAP_GROUP_BASE_DN                | Only applies if `AUTH_LDAP_EXTRACT_GROUPS_ENABLED` is set to true. The base DN for searching groups in the LDAP directory                                                                                                                                                                                                                                        |         |
| AUTH_LDAP_GROUP_FILTER                 | Only applies if `AUTH_LDAP_EXTRACT_GROUPS_ENABLED` is set to true. The LDAP filter used to search for groups. Use `{0}` as a placeholder for the user's DN. Common examples: `(member={0})` or `(uniqueMember={0})`                                                                                                                                              |         |
| AUTH_LDAP_GROUP_NAME_ATTRIBUTE         | Only applies if `AUTH_LDAP_EXTRACT_GROUPS_ENABLED` is set to true. The LDAP attribute that contains the group name                                                                                                                                                                                                                                               | cn      |

## 3. Restart datahub-frontend-react

Once configured, restarting the `datahub-frontend-react` container will enable LDAP authentication in which DataHub validates user credentials against the specified LDAP server.

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml  up datahub-frontend-react
```

Navigate to your DataHub domain to see LDAP authentication in action.

:::caution
By default, enabling LDAP will _not_ disable the dummy JAAS authentication path, which can be reached at the `/login`
route of the React app. To disable the default file-based authentication, additionally specify the following config:
`AUTH_JAAS_ENABLED=false`
:::

## Summary

Once a user is authenticated by the LDAP server, DataHub will extract user information from LDAP attributes
and grant DataHub access to the user by setting a pair of session cookies.

A brief summary of the steps that occur when the user navigates to the React app are as follows:

1. A `GET` to login `datahub-frontend` server is initiated
2. The login attempts to authenticate the request via session cookies
3. If auth fails, the server redirects to the DataHub login page
4. The user enters their LDAP username and password
5. DataHub connects to the LDAP server using the configured bind DN and password
6. DataHub searches for the user in the LDAP directory using the configured user filter
7. DataHub attempts to bind as the user with the provided credentials
8. If authentication succeeds, DataHub extracts user attributes (name, email, etc.) from LDAP
9. DataHub provisions the user in DataHub if JIT provisioning is enabled
10. DataHub sets session cookies for the newly authenticated user
11. DataHub redirects the user to the homepage ("/")

## Troubleshooting

<details>
<summary>No users can log in. Instead, I get an error message on the login page. What do I do?</summary>

This can occur for a variety of reasons, but most often it is due to misconfiguration of LDAP authentication, either on the DataHub
side or on the LDAP server side.

- Verify that all LDAP configuration values are correct (server URL, base DN, bind DN, bind password, user filter).
- Verify that the LDAP server is accessible from the `datahub-frontend` container. You can test this by running `ldapsearch` from within the container.
- Verify that the bind DN and password are correct and have sufficient permissions to search for users in the LDAP directory.
- Verify that the user filter is correct and matches the structure of your LDAP directory.
- Check the container logs for the `datahub-frontend` container. This should hopefully provide some additional context around why exactly the LDAP authentication is not working.

If all else fails, feel free to reach out to the DataHub Community on Slack for real-time support.

</details>

<details>
<summary>
I'm seeing an error in the `datahub-frontend` logs when a user tries to login: LDAP connection timeout or connection refused
</summary>

This indicates that the `datahub-frontend` container cannot reach your LDAP server.

To fix this, you may need to:

1. Verify that the LDAP server URL is correct and includes the proper protocol (`ldap://` or `ldaps://`)
2. Verify that the LDAP server is accessible from the network where the `datahub-frontend` container is running
3. Check firewall rules to ensure that the LDAP port (typically 389 for LDAP or 636 for LDAPS) is open
4. If using Docker, ensure that the container has network access to the LDAP server

</details>

<details>
<summary>
I'm seeing an error: "Invalid credentials" or "Bind failed" even though I know the credentials are correct
</summary>

This can happen for several reasons:

1. The user filter may not be matching the user correctly. Verify that the `AUTH_LDAP_USER_FILTER` is correct for your LDAP schema.
2. The user ID attribute may be incorrect. Verify that `AUTH_LDAP_USER_ID_ATTRIBUTE` matches the attribute used in your LDAP directory.
3. The base DN may be incorrect or too broad/narrow. Verify that `AUTH_LDAP_BASE_DN` includes the organizational unit where your users are located.
4. For Active Directory, ensure you're using the correct format for the username (e.g., `sAMAccountName` instead of `uid`).

</details>

<details>
<summary>
Users can log in but their profile information (name, email) is not populated correctly
</summary>

This indicates that the LDAP attribute mapping may be incorrect.

To fix this, you may need to:

1. Verify that the LDAP attributes specified in `AUTH_LDAP_USER_FIRST_NAME_ATTRIBUTE`, `AUTH_LDAP_USER_LAST_NAME_ATTRIBUTE`, `AUTH_LDAP_USER_EMAIL_ATTRIBUTE`, and `AUTH_LDAP_USER_DISPLAY_NAME_ATTRIBUTE` exist in your LDAP schema and are populated for your users.
2. Use an LDAP browser tool to inspect the actual attributes available for your users and adjust the configuration accordingly.
3. Check the `datahub-frontend` logs to see which attributes are being retrieved from LDAP.

</details>

## Reference

### Common LDAP Configurations

#### OpenLDAP

```
AUTH_LDAP_ENABLED=true
AUTH_LDAP_SERVER=ldap://ldap.example.com:389
AUTH_LDAP_BASE_DN=ou=users,dc=example,dc=com
AUTH_LDAP_BIND_DN=cn=admin,dc=example,dc=com
AUTH_LDAP_BIND_PASSWORD=admin-password
AUTH_LDAP_USER_FILTER=(uid={0})
AUTH_LDAP_USER_ID_ATTRIBUTE=uid
```

#### Active Directory

```
AUTH_LDAP_ENABLED=true
AUTH_LDAP_SERVER=ldap://ad.example.com:389
AUTH_LDAP_BASE_DN=CN=Users,DC=example,DC=com
AUTH_LDAP_BIND_DN=CN=Service Account,CN=Users,DC=example,DC=com
AUTH_LDAP_BIND_PASSWORD=service-account-password
AUTH_LDAP_USER_FILTER=(sAMAccountName={0})
AUTH_LDAP_USER_ID_ATTRIBUTE=sAMAccountName
```

#### Active Directory with SSL

```
AUTH_LDAP_ENABLED=true
AUTH_LDAP_SERVER=ldaps://ad.example.com:636
AUTH_LDAP_USE_SSL=true
AUTH_LDAP_BASE_DN=CN=Users,DC=example,DC=com
AUTH_LDAP_BIND_DN=CN=Service Account,CN=Users,DC=example,DC=com
AUTH_LDAP_BIND_PASSWORD=service-account-password
AUTH_LDAP_USER_FILTER=(sAMAccountName={0})
AUTH_LDAP_USER_ID_ATTRIBUTE=sAMAccountName
```

#### Active Directory with SSL and Group Extraction (Recommended)

```
AUTH_JAAS_ENABLED=true
AUTH_LDAP_ENABLED=true
AUTH_LDAP_SERVER=ldaps://ad.example.com:636
AUTH_LDAP_BASE_DN=DC=example,DC=com
AUTH_LDAP_USER_BASE_DN=DC=example,DC=com
AUTH_LDAP_USER_FILTER=(&(userPrincipalName={0}@example.com)(objectClass=person))
AUTH_LDAP_JIT_PROVISIONING_ENABLED=true
AUTH_LDAP_EXTRACT_GROUPS_ENABLED=true
AUTH_LDAP_GROUP_BASE_DN=DC=example,DC=com
AUTH_LDAP_GROUP_FILTER=(&(objectClass=group)(member={0}))
AUTH_LDAP_GROUP_NAME_ATTRIBUTE=cn
```

:::info
This configuration uses **user credentials** (no bind DN/password required). Users authenticate with their own LDAP credentials, which are then used to extract user and group information. This is more secure than storing service account credentials.
:::

### application.conf Configuration

For direct configuration in `application.conf` (typically used in development or when not using environment variables):

```hocon
auth {
  jaas {
    enabled = true
  }
  ldap {
    enabled = true
    jitProvisioningEnabled = true
    extractGroupsEnabled = true
    server = "ldaps://ldap.example.com:636"
    baseDn = "DC=example,DC=com"
    userBaseDn = "DC=example,DC=com"
    userFilter = "(&(userPrincipalName={0}@example.com)(objectClass=person))"
    groupBaseDn = "DC=example,DC=com"
    groupFilter = "(&(objectClass=group)(member={0}))"
    groupNameAttribute = "cn"
  }
}
```

**Note:** User credentials (username and password) are obtained during login and used to authenticate to LDAP. No service account credentials are stored in configuration.

### Additional Resources

- [LDAP Authentication in Java](https://docs.oracle.com/javase/jndi/tutorial/ldap/security/ldap.html)
- [Active Directory LDAP Syntax](https://docs.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax)
- [OpenLDAP Documentation](https://www.openldap.org/doc/)
