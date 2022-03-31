# OIDC Authentication

The DataHub React application supports OIDC authentication built on top of the [Pac4j Play](https://github.com/pac4j/play-pac4j) library.
This enables operators of DataHub to integrate with 3rd party identity providers like Okta, Google, Keycloak, & more to authenticate their users.

When configured, OIDC auth will be enabled between clients of the DataHub UI & `datahub-frontend` server. Beyond this point is considered
to be a secure environment and as such authentication is validated & enforced only at the "front door" inside datahub-frontend.

## Provider-Specific Guides

1. [Configuring OIDC using Google](configure-oidc-react-google.md)
2. [Configuring OIDC using Okta](configure-oidc-react-okta.md)
3. [Configuring OIDC using Azure](configure-oidc-react-azure.md)

## Configuring OIDC in React

### 1. Register an app with your Identity Provider

To configure OIDC in React, you will most often need to register yourself as a client with your identity provider (Google, Okta, etc). Each provider may
have their own instructions. Provided below are links to examples for Okta, Google, Azure AD, & Keycloak.

- [Registering an App in Okta](https://developer.okta.com/docs/guides/add-an-external-idp/apple/register-app-in-okta/)
- [OpenID Connect in Google Identity](https://developers.google.com/identity/protocols/oauth2/openid-connect)
- [OpenID Connect authentication with Azure Active Directory](https://docs.microsoft.com/en-us/azure/active-directory/fundamentals/auth-oidc)
- [Keycloak - Securing Applications and Services Guide](https://www.keycloak.org/docs/latest/securing_apps/)

During the registration process, you'll need to provide a login redirect URI to the identity provider. This tells the identity provider
where to redirect to once they've authenticated the end user.

By default, the URL will be constructed as follows:

> "http://your-datahub-domain.com/callback/oidc"

For example, if you're hosted DataHub at `datahub.myorg.com`, this
value would be `http://datahub.myorg.com/callback/oidc`. For testing purposes you can also specify localhost as the domain name
directly: `http://localhost:9002/callback/oidc`

The goal of this step should be to obtain the following values, which will need to be configured before deploying DataHub:

1. **Client ID** - A unique identifier for your application with the identity provider
2. **Client Secret** - A shared secret to use for exchange between you and your identity provider
3. **Discovery URL** - A URL where the OIDC API of your identity provider can be discovered. This should suffixed by
   `.well-known/openid-configuration`. Sometimes, identity providers will not explicitly include this URL in their setup guides, though
   this endpoint *will* exist as per the OIDC specification. For more info see http://openid.net/specs/openid-connect-discovery-1_0.html.


### 2. Configure DataHub Frontend Server

The second step to enabling OIDC involves configuring `datahub-frontend` to enable OIDC authentication with your Identity Provider.

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

- `AUTH_OIDC_ENABLED`: Enable delegating authentication to OIDC identity provider
- `AUTH_OIDC_CLIENT_ID`: Unique client id received from identity provider
- `AUTH_OIDC_CLIENT_SECRET`: Unique client secret received from identity provider
- `AUTH_OIDC_DISCOVERY_URI`: Location of the identity provider OIDC discovery API. Suffixed with `.well-known/openid-configuration`
- `AUTH_OIDC_BASE_URL`: The base URL of your DataHub deployment, e.g. https://yourorgdatahub.com (prod) or http://localhost:9002 (testing)

Providing these configs will cause DataHub to delegate authentication to your identity
provider, requesting the "oidc email profile" scopes and parsing the "preferred_username" claim from
the authenticated profile as the DataHub CorpUser identity.


> By default, the login callback endpoint exposed by DataHub will be located at `${AUTH_OIDC_BASE_URL}/callback/oidc`. This must **exactly** match the login redirect URL you've registered with your identity provider in step 1.

In kubernetes, you can add the above env variables in the values.yaml as follows.

```
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

```kubectl create secret generic datahub-oidc-secret --from-literal=secret=<<OIDC SECRET>>```

Then set the secret env as follows.

```
    - name: AUTH_OIDC_CLIENT_SECRET
      valueFrom:
        secretKeyRef:
          name: datahub-oidc-secret
          key: secret
```


#### Advanced

You can optionally customize the flow further using advanced configurations. These allow
you to specify the OIDC scopes requested, how the DataHub username is parsed from the claims returned by the identity provider, and how users and groups are extracted and provisioned from the OIDC claim set.

```
# Optional Configuration Values:
AUTH_OIDC_USER_NAME_CLAIM=your-custom-claim
AUTH_OIDC_USER_NAME_CLAIM_REGEX=your-custom-regex
AUTH_OIDC_SCOPE=your-custom-scope
AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD=authentication-method
```

- `AUTH_OIDC_USER_NAME_CLAIM`: The attribute that will contain the username used on the DataHub platform. By default, this is "preferred_username" provided
  as part of the standard `profile` scope.
- `AUTH_OIDC_USER_NAME_CLAIM_REGEX`: A regex string used for extracting the username from the userNameClaim attribute. For example, if
  the userNameClaim field will contain an email address, and we want to omit the domain name suffix of the email, we can specify a custom
  regex to do so. (e.g. `([^@]+)`)
- `AUTH_OIDC_SCOPE`: a string representing the scopes to be requested from the identity provider, granted by the end user. For more info,
  see [OpenID Connect Scopes](https://auth0.com/docs/scopes/openid-connect-scopes).
- `AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD`: a string representing the token authentication method to use with the identity provider. Default value
  is `client_secret_basic`, which uses HTTP Basic authentication. Another option is `client_secret_post`, which includes the client_id and secret_id
  as form parameters in the HTTP POST request. For more info, see [OAuth 2.0 Client Authentication](https://darutk.medium.com/oauth-2-0-client-authentication-4b5f929305d4)


##### User & Group Provisioning (JIT Provisioning)

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

- `AUTH_OIDC_JIT_PROVISIONING_ENABLED`: Whether DataHub users & groups should be provisioned on login if they do not exist. Defaults to true.
- `AUTH_OIDC_PRE_PROVISIONING_REQUIRED`: Whether the user should already exist in DataHub when they login, failing login if they are not. This is appropriate for situations in which users and groups are batch ingested and tightly controlled inside your environment. Defaults to false.
- `AUTH_OIDC_EXTRACT_GROUPS_ENABLED`: Only applies if `AUTH_OIDC_JIT_PROVISIONING_ENABLED` is set to true. This determines whether we should attempt to extract a list of group names from a particular claim in the OIDC attributes. Note that if this is enabled, each login will re-sync group membership with the groups in your Identity Provider, clearing the group membership that has been assigned through the DataHub UI. Enable with care! Defaults to false.
- `AUTH_OIDC_GROUPS_CLAIM`: Only applies if `AUTH_OIDC_EXTRACT_GROUPS_ENABLED` is set to true. This determines which OIDC claims will contain a list of string group names. Accepts multiple claim names with comma-separated values. I.e: `groups, teams, departments`. Defaults to 'groups'.


Once configuration has been updated, `datahub-frontend-react` will need to be restarted to pick up the new environment variables:

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml  up datahub-frontend-react
```

>Note that by default, enabling OIDC will *not* disable the dummy JAAS authentication path, which can be reached at the `/login`
route of the React app. To disable this authentication path, additionally specify the following config:
> `AUTH_JAAS_ENABLED=false`

### Summary

Once configured, deploying the `datahub-frontend-react` container will enable an indirect authentication flow in which DataHub delegates
authentication to the specified identity provider.

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

### Root user

Even if OIDC is configured the root user can still login without OIDC by going to `/login` URL endpoint. It is recommended that you don't use the default credentials by mounting a different file in the front end container. To do this please see (jaas)[../jaas.md] - "Mount a custom user.props file".