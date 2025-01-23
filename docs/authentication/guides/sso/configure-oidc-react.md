import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Configuring OIDC Authentication

The DataHub React application supports OIDC authentication built on top of the [Pac4j Play](https://github.com/pac4j/play-pac4j) library.
This enables operators of DataHub to integrate with 3rd party identity providers like Okta, Google, Keycloak, & more to authenticate their users.

## 1. Get your credentials

Before you do anything, you'll want to set up DataHub with your SSO provider, and get prerequisite credentials:
1. _Client ID_ - A unique identifier for your application with the identity provider
2. _Client Secret_ - A shared secret to use for exchange between you and your identity provider.
3. _Discovery URL_ - A URL where the OIDC API of your identity provider can be discovered.

[👉 See the steps here](./initialize-oidc.md)

## 2. Configure DataHub Frontend Server

### Docker

The next step to enabling OIDC involves configuring `datahub-frontend` to enable OIDC authentication with your Identity Provider.

To do so, you must update the `datahub-frontend` [docker.env](../../../../docker/datahub-frontend/env/docker.env) file with the
values received from your identity provider:

```
# Required Configuration Values:
AUTH_OIDC_ENABLED=true
AUTH_OIDC_CLIENT_ID=your-client-id
AUTH_OIDC_CLIENT_SECRET=your-client-secret
AUTH_OIDC_DISCOVERY_URI=your-provider-discovery-url
AUTH_OIDC_BASE_URL=your-datahub-url
```

| Configuration           | Description                                                                                                                                                                                                                                                              | Default |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| AUTH_OIDC_ENABLED       | Enable delegating authentication to OIDC identity provider                                                                                                                                                                                                               |         |
| AUTH_OIDC_CLIENT_ID     | Unique client id received from identity provider                                                                                                                                                                                                                         |         |
| AUTH_OIDC_CLIENT_SECRET | Unique client secret received from identity provider                                                                                                                                                                                                                     |         |
| AUTH_OIDC_DISCOVERY_URI | Location of the identity provider OIDC discovery API. Suffixed with `.well-known/openid-configuration`                                                                                                                                                                   |         |
| AUTH_OIDC_BASE_URL      | The base URL of your DataHub deployment, e.g. https://yourorgdatahub.com (prod) or http://localhost:9002 (testing)                                                                                                                                                       |         |
| AUTH_SESSION_TTL_HOURS  | The length of time in hours before a user will be prompted to login again. Controls the actor cookie expiration time in the browser. Numeric value converted to hours.                                                                                                   | 24      |
| MAX_SESSION_TOKEN_AGE   | Determines the expiration time of a session token. Session tokens are stateless so this determines at what time a session token may no longer be used and a valid session token can be used until this time has passed. Accepts a valid relative Java date style String. | 24h     |

Providing these configs will cause DataHub to delegate authentication to your identity
provider, requesting the "oidc email profile" scopes and parsing the "preferred_username" claim from
the authenticated profile as the DataHub CorpUser identity.

:::note

By default, the login callback endpoint exposed by DataHub will be located at `${AUTH_OIDC_BASE_URL}/callback/oidc`. This must **exactly** match the login redirect URL you've registered with your identity provider in step 1.

:::

### Kubernetes

In Kubernetes, you can add the above env variables in the `values.yaml` as follows.

```yaml
datahub-frontend:
  ...
  extraEnvs:
    - name: AUTH_OIDC_ENABLED
      value: "true"
    - name: AUTH_OIDC_CLIENT_ID
      value: your-client-id
    - name: AUTH_OIDC_CLIENT_SECRET
      value: your-client-secret
    - name: AUTH_OIDC_DISCOVERY_URI
      value: your-provider-discovery-url
    - name: AUTH_OIDC_BASE_URL
      value: your-datahub-url
```

You can also package OIDC client secrets into a k8s secret by running

```
kubectl create secret generic datahub-oidc-secret --from-literal=secret=<<OIDC SECRET>>
```

Then set the secret env as follows.

```yaml
- name: AUTH_OIDC_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: datahub-oidc-secret
      key: secret
```

### Advanced OIDC Configurations

You can optionally customize the flow further using advanced configurations. These allow
you to specify the OIDC scopes requested, how the DataHub username is parsed from the claims returned by the identity provider, and how users and groups are extracted and provisioned from the OIDC claim set.

```
# Optional Configuration Values:
AUTH_OIDC_USER_NAME_CLAIM=your-custom-claim
AUTH_OIDC_USER_NAME_CLAIM_REGEX=your-custom-regex
AUTH_OIDC_SCOPE=your-custom-scope
AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD=authentication-method
```

| Configuration                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                         | Default             |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| AUTH_OIDC_USER_NAME_CLAIM              | The attribute that will contain the username used on the DataHub platform. By default, this is "email" providedas part of the standard `email` scope.                                                                                                                                                                                                                                                                               |                     |
| AUTH_OIDC_USER_NAME_CLAIM_REGEX        | A regex string used for extracting the username from the userNameClaim attribute. For example, if the userNameClaim field will contain an email address, and we want to omit the domain name suffix of the email, we can specify a customregex to do so. (e.g. `([^@]+)`)                                                                                                                                                           |                     |
| AUTH_OIDC_SCOPE                        | A string representing the scopes to be requested from the identity provider, granted by the end user. For more info, see [OpenID Connect Scopes](https://auth0.com/docs/scopes/openid-connect-scopes).                                                                                                                                                                                                                              |                     |
| AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD | a string representing the token authentication method to use with the identity provider. Default value is `client_secret_basic`, which uses HTTP Basic authentication. Another option is `client_secret_post`, which includes the client_id and secret_id as form parameters in the HTTP POST request. For more info, see [OAuth 2.0 Client Authentication](https://darutk.medium.com/oauth-2-0-client-authentication-4b5f929305d4) | client_secret_basic |
| AUTH_OIDC_PREFERRED_JWS_ALGORITHM      | Can be used to select a preferred signing algorithm for id tokens. Examples include: `RS256` or `HS256`. If your IdP includes `none` before `RS256`/`HS256` in the list of signing algorithms, then this value **MUST** be set.                                                                                                                                                                                                     |                     |

### User & Group Provisioning (JIT Provisioning)

By default, DataHub will optimistically attempt to provision users and groups that do not already exist at the time of login.
For users, we extract information like first name, last name, display name, & email to construct a basic user profile. If a groups claim is present,
we simply extract their names.

The default provisioning behavior can be customized using the following configs.

```
# User and groups provisioning
AUTH_OIDC_JIT_PROVISIONING_ENABLED=true
AUTH_OIDC_PRE_PROVISIONING_REQUIRED=false
AUTH_OIDC_EXTRACT_GROUPS_ENABLED=false
AUTH_OIDC_GROUPS_CLAIM=<your-groups-claim-name>
```

| Configuration                       | Description                                                                                                                                                                                                                                                                                                                                                                                                      | Default |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| AUTH_OIDC_JIT_PROVISIONING_ENABLED  | Whether DataHub users & groups should be provisioned on login if they do not exist.                                                                                                                                                                                                                                                                                                                              | true    |
| AUTH_OIDC_PRE_PROVISIONING_REQUIRED | Whether the user should already exist in DataHub when they login, failing login if they are not. This is appropriate for situations in which users and groups are batch ingested and tightly controlled inside your environment.                                                                                                                                                                                 | false   |
| AUTH_OIDC_EXTRACT_GROUPS_ENABLED    | Only applies if `AUTH_OIDC_JIT_PROVISIONING_ENABLED` is set to true. This determines whether we should attempt to extract a list of group names from a particular claim in the OIDC attributes. Note that if this is enabled, each login will re-sync group membership with the groups in your Identity Provider, clearing the group membership that has been assigned through the DataHub UI. Enable with care! | false   |
| AUTH_OIDC_GROUPS_CLAIM              | Only applies if `AUTH_OIDC_EXTRACT_GROUPS_ENABLED` is set to true. This determines which OIDC claims will contain a list of string group names. Accepts multiple claim names with comma-separated values. I.e: `groups, teams, departments`.                                                                                                                                                                     | groups  |

## 3. Restart datahub-frontend-react

Once configured, restarting the `datahub-frontend-react` container will enable an indirect authentication flow in which DataHub delegates authentication to the specified identity provider.

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml  up datahub-frontend-react
```

Navigate to your DataHub domain to see SSO in action.

:::caution
By default, enabling OIDC will _not_ disable the dummy JAAS authentication path, which can be reached at the `/login`
route of the React app. To disable this authentication path, additionally specify the following config:
`AUTH_JAAS_ENABLED=false`
:::

## Summary

Once a user is authenticated by the identity provider, DataHub will extract a username from the provided claims
and grant DataHub access to the user by setting a pair of session cookies.

A brief summary of the steps that occur when the user navigates to the React app are as follows:

1. A `GET` to the `/authenticate` endpoint in `datahub-frontend` server is initiated
2. The `/authenticate` attempts to authenticate the request via session cookies
3. If auth fails, the server issues a redirect to the Identity Provider's login experience
4. The user logs in with the Identity Provider
5. The Identity Provider authenticates the user and redirects back to DataHub's registered login redirect URL, providing an authorization code which
   can be used to retrieve information on behalf of the authenticated user
6. DataHub fetches the authenticated user's profile and extracts a username to identify the user on DataHub (eg. urn:li:corpuser:username)
7. DataHub sets session cookies for the newly authenticated user
8. DataHub redirects the user to the homepage ("/")

## Troubleshooting

<details>
<summary>No users can log in. Instead, I get redirected to the login page with an error. What do I do?</summary>

This can occur for a variety of reasons, but most often it is due to misconfiguration of Single-Sign On, either on the DataHub
side or on the Identity Provider side.

- Verify that all values are consistent across them (e.g. the host URL where DataHub is deployed), and that no values are misspelled (client id, client secret).
- Verify that the scopes requested are supported by your Identity Provider and that the claim (i.e. attribute) DataHub uses for uniquely identifying the user is supported by your Identity Provider (refer to Identity Provider OpenID Connect documentation). By default, this claim is `email`.
- Make sure the Discovery URI you've configured (`AUTH_OIDC_DISCOVERY_URI`) is accessible where the datahub-frontend container is running. You can do this by issuing a basic CURL to the address (**Pro-Tip**: you may also visit the address in your browser to check more specific details about your Identity Provider).
- Check the container logs for the `datahub-frontend` container. This should hopefully provide some additional context around why exactly the login handoff is not working.

If all else fails, feel free to reach out to the DataHub Community on Slack for real-time support.

</details>

<details>
<summary>
I'm seeing an error in the `datahub-frontend` logs when a user tries to login: Caused by: java.lang.RuntimeException: Failed to resolve user name claim from profile provided by Identity Provider. Missing attribute. Attribute: 'email', Regex: '(.*)', Profile: &#123; ....
</summary>

This indicates that your Identity Provider does not provide the claim with name 'email', which DataHub
uses by default to uniquely identify users within your organization.

To fix this, you may need to

1. Change the claim that is used as the unique user identifier to something else by changing the `AUTH_OIDC_USER_NAME_CLAIM` (e.g. to "name" or "preferred*username") \_OR*
2. Change the environment variable `AUTH_OIDC_SCOPE` to include the scope required to retrieve the claim with name "email"

For the `datahub-frontend` container / pod.

</details>

## Reference

Check the documentation for your Identity Provider to learn more about the scope claims supported.

- [Registering an App in Okta](https://developer.okta.com/docs/guides/add-an-external-idp/openidconnect/main/)
- [OpenID Connect in Google Identity](https://developers.google.com/identity/protocols/oauth2/openid-connect)
- [OpenID Connect authentication with Azure Active Directory](https://docs.microsoft.com/en-us/azure/active-directory/fundamentals/auth-oidc)
- [Keycloak - Securing Applications and Services Guide](https://www.keycloak.org/docs/latest/securing_apps/)
