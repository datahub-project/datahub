# Concepts & Key Components

We introduced a few important concepts to the Metadata Service to make authentication work:

1. Actor
2. Authenticator
3. AuthenticatorChain
4. AuthenticationFilter
5. DataHub Access Token
6. DataHub Token Service

In following sections, we'll take a closer look at each individually.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-service-auth.png"/>
</p>

*High level overview of Metadata Service Authentication*

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

and more! A key goal of the abstraction is *extensibility*: a custom Authenticator can be developed to authenticate requests
based on an organization's unique needs.

DataHub ships with 2 Authenticators by default:

- **DataHubSystemAuthenticator**: Verifies that inbound requests have originated from inside DataHub itself using a shared system identifier
  and secret. This authenticator is always present.

- **DataHubTokenAuthenticator**: Verifies that inbound requests contain a DataHub-issued Access Token (discussed further in the "DataHub Access Token" section below) in their
  'Authorization' header. This authenticator is required if Metadata Service Authentication is enabled.

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

## What is the AuthenticationFilter?

The **AuthenticationFilter** is a [servlet filter](http://tutorials.jenkov.com/java-servlets/servlet-filters.html) that authenticates each and requests to the Metadata Service.
It does so by constructing and invoking an **AuthenticatorChain**, described above.

If an Actor is unable to be resolved by the AuthenticatorChain, then a 401 unauthorized exception will be returned by the filter.


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
   request to the Metadata Service to generate a SESSION token *on behalf of* of the user logging in. (*Only the frontend service is authorized to perform this action).
2. **Generating Personal Access Tokens**: When a user requests to generate a Personal Access Token (described below) from the UI.

> At present, the Token Service supports the symmetric signing method `HS256` to generate and verify tokens.

Now that we're familiar with the concepts, we will talk concretely about what new capabilities have been built on top
of Metadata Service Authentication. 