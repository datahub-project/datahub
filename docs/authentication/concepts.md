# Concepts & Key Components

We introduced a few important concepts to the Metadata Service to make authentication work:

1. Actor
2. Authenticator
3. AuthenticatorChain
4. Two-Tier Authentication System
5. AuthenticationContext
6. DataHub Access Token
7. DataHub Token Service

In following sections, we'll take a closer look at each individually.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-service-auth.png"/>
</p>

_High level overview of Metadata Service Authentication_

## What is an Actor?

An **Actor** is a concept within the new Authentication subsystem to represent a unique identity / principal that is initiating actions (e.g. read & write requests)
on the platform.

An actor can be characterized by 2 attributes:

1. **Type**: The "type" of the actor making a request. The purpose is to for example distinguish between a "user" & "service" actor. Currently, the "user" actor type is the only one
   formally supported.
2. **Id**: A unique identifier for the actor within DataHub. This is commonly known as a "principal" in other systems. In the case of users, this
   represents a unique "username". This username is in turn used when converting from the "Actor" concept into a Metadata Entity Urn (e.g. CorpUserUrn).

For example, the root "datahub" super user would have the following attributes:

```
{
   "type": "USER",
   "id": "datahub"
}
```

Which is mapped to the CorpUser urn:

```
urn:li:corpuser:datahub
```

for Metadata retrieval.

## What is an Authenticator?

An **Authenticator** is a pluggable component inside the Metadata Service that is responsible for authenticating an inbound request provided context about the request (currently, the request headers).
Authentication boils down to successfully resolving an **Actor** to associate with the inbound request.

There can be many types of Authenticator. For example, there can be Authenticators that

- Verify the authenticity of access tokens (ie. issued by either DataHub itself or a 3rd-party IdP)
- Authenticate username / password credentials against a remote database (ie. LDAP)

and more! A key goal of the abstraction is _extensibility_: a custom Authenticator can be developed to authenticate requests
based on an organization's unique needs.

DataHub ships with 3 Authenticators by default:

- **DataHubSystemAuthenticator**: Verifies that inbound requests have originated from inside DataHub itself using a shared system identifier
  and secret. This authenticator is always present.

- **DataHubTokenAuthenticator**: Verifies that inbound requests contain a DataHub-issued Access Token (discussed further in the "DataHub Access Token" section below) in their
  'Authorization' header. This authenticator is required if Metadata Service Authentication is enabled.

- **DataHubGuestAuthenticator**: Verifies if guest authentication is enabled with a guest user configured and allows unauthenticated users to perform operations as the designated
  guest user. By default, this Authenticator is disabled. If this is required, it needs to be explicitly enabled and requires a restart of the datahub GMS service.
-

## What is an AuthenticatorChain?

An **AuthenticatorChain** is a series of **Authenticators** that are configured to run one-after-another. This allows
for configuring multiple ways to authenticate a given request, for example via LDAP OR via local key file.

Only if each Authenticator within the chain fails to authenticate a request will it be rejected.

The Authenticator Chain can be configured in the `application.yaml` file under `authentication.authenticators`:

```
authentication:
  ....
  authenticators:
    # Configure the Authenticators in the chain
    - type: com.datahub.authentication.Authenticator1
      ...
    - type: com.datahub.authentication.Authenticator2
    ....
```

## What is the Two-Tier Authentication System?

DataHub uses a **two-tier authentication system** that decouples authentication extraction from enforcement:

### Tier 1: Authentication Extraction

The **AuthenticationExtractionFilter** is the foundation [servlet filter](http://tutorials.jenkov.com/java-servlets/servlet-filters.html) that runs for **every request** to the Metadata Service. Its single responsibility:

- **Extract Authentication Information**: Constructs and invokes an **AuthenticatorChain** to process credentials
- **Set Universal Context**: Always establishes an **AuthenticationContext** (see below) for every request
- **Never Enforce**: Never blocks requests - if authentication fails, it sets an anonymous context and continues

### Tier 2: Authentication Enforcement

The second tier consists of **enforcement mechanisms** that can be implemented in multiple ways:

#### AuthenticationEnforcementFilter

The default enforcement filter that:

- **Selective Processing**: Only processes endpoints requiring authentication (excludes paths like `/health`, `/config`)
- **Context-Based Decisions**: Reads the **AuthenticationContext** set by the extraction tier
- **Request Blocking**: Returns 401 unauthorized when authentication is required but not present

#### Additional Enforcement Options

The decoupled design enables flexible enforcement strategies:

- **Multiple Enforcement Filters**: Different areas can have specialized filters (e.g., admin area filter with additional privilege checks)
- **Controller-Level Enforcement**: Individual controllers can examine the **AuthenticationContext** and enforce their own rules
- **Custom Authorization Logic**: Business logic can make authentication decisions based on the established context

### Benefits of Two-Tier Architecture

This separation of concerns provides several advantages:

1. **Decoupled Responsibilities**: Authentication extraction is separate from enforcement decisions
2. **Performance**: Authentication processing happens once per request, regardless of enforcement complexity
3. **Flexibility**: Multiple enforcement strategies can coexist (filters, controllers, custom logic)
4. **Extensibility**: New enforcement mechanisms can be added without changing authentication extraction
5. **Consistency**: All parts of the system have access to the same authentication context
6. **Progressive Disclosure**: As a side benefit, endpoints can provide different responses based on user authentication status

## What is AuthenticationContext?

The **AuthenticationContext** is a thread-local storage mechanism that bridges the extraction and enforcement tiers. It serves as the **universal authentication state** for the entire request lifecycle:

- **Authentication Object**: Contains the result of the authentication extraction process (Actor + credentials)
- **Anonymous Support**: Set to anonymous actor when authentication extraction yields no valid credentials
- **Request Lifecycle**: Automatically established by the extraction tier and cleaned up after each request
- **Universal Access**: Available to all enforcement mechanisms - filters, controllers, or custom business logic

This context enables **consistent authentication decisions** across all parts of the system. Whether enforcement happens in a servlet filter, a controller method, or custom business logic, they all work with the same authentication information established during the extraction phase.

## What is a DataHub Token Service? What are Access Tokens?

Along with Metadata Service Authentication comes an important new component called the **DataHub Token Service**. The purpose of this
component is twofold:

1. Generate Access Tokens that grant access to the Metadata Service
2. Verify the validity of Access Tokens presented to the Metadata Service

**Access Tokens** granted by the Token Service take the form of [Json Web Tokens](https://jwt.io/introduction), a type of stateless token which
has a finite lifespan & is verified using a unique signature. JWTs can also contain a set of claims embedded within them. Tokens issued by the Token
Service contain the following claims:

- exp: the expiration time of the token
- version: version of the DataHub Access Token for purposes of evolvability (currently 1)
- type: The type of token, currently SESSION (used for UI-based sessions) or PERSONAL (used for personal access tokens)
- actorType: The type of the **Actor** associated with the token. Currently, USER is the only type supported.
- actorId: The id of the **Actor** associated with the token.

Today, Access Tokens are granted by the Token Service under two scenarios:

1. **UI Login**: When a user logs into the DataHub UI, for example via [JaaS](guides/jaas.md) or
   [OIDC](guides/sso/configure-oidc-react.md), the `datahub-frontend` service issues an
   request to the Metadata Service to generate a SESSION token _on behalf of_ of the user logging in. (\*Only the frontend service is authorized to perform this action).
2. **Generating Personal Access Tokens**: When a user requests to generate a Personal Access Token (described below) from the UI.

> At present, the Token Service supports the symmetric signing method `HS256` to generate and verify tokens.

Now that we're familiar with the concepts, we will talk concretely about what new capabilities have been built on top
of Metadata Service Authentication.

## How do I enable Guest Authentication

The Guest Authentication configuration is present in two configuration files - the `application.conf` for DataHub frontend, and
`application.yaml` for GMS. To enable Guest Authentication, set the environment variable `GUEST_AUTHENTICATION_ENABLED` to `true`
for both the GMS and the frontend service and restart those services.
If enabled, the default user designated as guest is called `guest`. This user must be explicitly created and privileges assigned
to control the guest user privileges.

A recommended approach to operationalize guest access is, first, create a designated guest user account with login credentials,
but keep guest access disabled. This allows you to configure and test the exact permissions this user should have. Once you've
confirmed the privileges are set correctly, you can then enable guest access, which removes the need for login/credentials
while maintaining the verified permission settings.

The name of the designated guest user can be changed by defining the env var `GUEST_AUTHENTICATION_USER`.
The entry URL to authenticate as the guest user is `/public` and can be changed via the env var `GUEST_AUTHENTICATION_PATH`

Here are the relevant portions of the two configs

For the Frontend

```yaml
#application.conf
...
auth.guest.enabled = ${?GUEST_AUTHENTICATION_ENABLED}
# The name of the guest user id
auth.guest.user = ${?GUEST_AUTHENTICATION_USER}
# The path to bypass login page and get logged in as guest
auth.guest.path = ${?GUEST_AUTHENTICATION_PATH}
...
```

and for GMS

```yaml
#application.yaml
# Required if enabled is true! A configurable chain of Authenticators
...
authenticators:
  ...
  - type: com.datahub.authentication.authenticator.DataHubGuestAuthenticator
    configs:
      guestUser: ${GUEST_AUTHENTICATION_USER:guest}
      enabled: ${GUEST_AUTHENTICATION_ENABLED:false}
...
```
