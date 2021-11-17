# Metadata Service Authentication

## Introduction

We've recently introduced Authentication in the Metadata Service layer. This document will provide a technical overview of the feature aimed at developers
evaluating or operating DataHub for their companies. It will include a characterization of the motivations forthe feature, the key components in its design, the new capabilities it provides, & instructions on configuring Authentication in the Metadata Service. 

## Background

Let's recall 2 critical components of DataHub's architecture: 

- **DataHub Frontend Proxy** (datahub-frontend) - Resource server that routes requests to downstream Metadata Service
- **DataHub Metadata Service** (datahub-gms) - Source of truth for storing and serving DataHub Metadata Graph. 

Previously, Authentication was exclusively handled by the frontend proxy. This service would perform the following steps 
when a user navigated to `http://localhost:9002/`, where the UI app was served:

a. Check for the presence of a special PLAY_SESSION cookie.
b. If cookie was present + valid, redirect to the home page
c. If cookie was invalid, redirect to either a) the DataHub login screen (for [JAAS authentication](https://datahubproject.io/docs/how/auth/jaas/) or b) a [configured OIDC Identity Provider](https://datahubproject.io/docs/how/auth/sso/configure-oidc-react/) to perform authentication.

Once authentication had succeeded at the frontend proxy layer, a stateless (token-based) session cookie (PLAY_SESSION) would be set in the users browser.
All subsequent requests, including the GraphQL requests issued by the React UI, would be authenticated using this session cookie. Once a request had made it beyond
the frontend service layer, it was assumed to have been already authenticated. Hence, there was **no native authentication inside of the Metadata Service**. 

### Problems with this approach

The major challenge with this situation is that requests to the backend Metadata Service were completely unauthenticated. There were 2 options for folks who required authentication at the Metadata Service layer:

1. Set up a proxy in front of Metadata Service that performed authentication
2. (A more recent possibility) Route requests to Metadata Service through DataHub Frontend Proxy, including the PLAY_SESSION
Cookie with every request.
   
Neither of which are ideal. Setting up a proxy to do authentication takes time & expertise. Extracting and setting a session cookie for programmatic is
clunky & unscalable. On top of that, extending the authentication system was difficult, requiring implementing a new Play module within DataHub Frontend.

## Introducing Authentication in DataHub Metadata Service

To address these problems, we've introduced configurable Authentication inside the **Metadata Service** itself, 
meaning that requests are no longer considered authenticated until they reach the Metadata Service.

Why push Authentication down? In addition to the problems described in the previous section, the rationale for pushing authentication deeper in the stack, as opposed to keeping it at the proxy layer, 
was in part locality (keeping shared components in the same place) and in part planning for a future where incoming Kafka writes 
can be validated using the same mechanisms as normal Rest API requests.

Next, we'll cover the new components introduced to support Authentication inside the Metadata Service. 

### Key Components

So how does this work? We've introduced a few important concepts to the Metadata Service to make things work:

1. Authenticator
2. AuthenticationChain
3. AuthenticationFilter
4. DataHub Access Token
5. DataHub Token Service
 
We'll take a closer look at each individually. 

![](./imgs/metadata-service-auth.png)
*High level overview of Metadata Service Authentication*

#### What is an Authenticator? 

An **Authenticator** is an abstract, pluggable component inside the Metadata Service that is responsible for authenticating an inbound request given the request context (e.g. headers, etc). 
Authenticating basically boils down to resolving a 1) user type (e.g. the normal "corp user") and 2) unique user identifier used to identify a given actor 
within the DataHub ecosystem. 

There can be any number of different flavors of Authenticator. You could have one that verifies LDAP credentials provided in an HTTP header, or another that uses a remote Database to verify a username + password combination, and many more.

A few concrete examples that ship with DataHub by default are

- **DataHubTokenAuthenticator**: Verifies that inbound requests contain a DataHub-issued Access Token (discussed below) in their 
'Authorization' header.
  
- **DataHubSystemAuthenticator**: Verifies that inbound requests have originated from inside DataHub itself using a shared system identifier
and secret. 

#### What is an AuthenticationChain?

An **AuthenticationChain** is a series of **Authenticators** that are configured to run one-after-another. This abstractions allows
for configuring multiple ways to authenticate an inbound request, for example via LDAP and via local key file. 

DataHub supports configuring (described below) arbitrary authenticators to be executing for each inbound request received by the 
Metadata Service. 


#### What is the AuthenticationFilter?

The **AuthenticationFilter** is a servlet filter that authenticates each and every inbound request to the Metadata Service, aside from the normal /health and /config utility endpoints. 
It does so by invoking a


#### What is a DataHub Access Token? 


#### What is the DataHub Token Service? 


### New Capability: Personal Access Tokens

With these changes, we've introduced a way to generate a "Personal Access Token" suitable for programmatic use with both the DataHub GraphQL
and DataHub Rest.li (Ingestion) APIs. 

Personal Access Tokens have a finite lifespan (default 3 months) and currently cannot be revoked without changing the signing key that
DataHub uses to generate these tokens (via the TokenService described above). Most importantly, they inherit the permissions
granted to the user who generates them. 

#### Generating a Personal Access Token

To generate a personal access token, users must have been granted the "Generate Personal Access Token" (GENERATE_PERSONAL_ACCESS_TOKEN) Privilege via a [DataHub Policy](./policies.md). Once
they have this permission, they can navigate to 'Settings' > 'Access Tokens' > 'Generate Personal Access Token' to generate a token.

![](./imgs/generate-personal-access-token.png)


#### Using a Personal Access Token

The user will subsequently be able to make authenticated requests to DataHub frontend proxy or DataHub GMS directly by providing
the generated Access Token as a Bearer token in the `Authorization` header:

```
Authorization: Bearer <generated-access-token> 
```

For example, using a curl to the frontend proxy (preferred in production):

`curl 'http://localhost:9002/api/gms/entities/urn:li:corpuser:datahub' -H 'Authorization: Bearer <access-token>`

or to Metadata Service directly:

`curl 'http://localhost:8080/entities/urn:li:corpuser:datahub' -H 'Authorization: Bearer <access-token>`

Without an access token, making programmatic requests will result in a 401 result from the server if Metadata Service Authentication
is enabled.

### Configuring Metadata Service Authentication



### The Role of DataHub Frontend Proxy Going Forward

With these changes, DataHub Frontend will continue to play a vital part in the complex dance of Authentication. It will serve as the place
where UI-based session authentication, i.e. "login", originates and will continue to support 3rd Party SSO configuration (OIDC)
and JAAS configuration as it does today. The major improvement is that the Frontend Service will exchange credentials provided at "login" time
for a *DataHub Access Token* (described below), standing in replacement of the traditional session cookie (which will continue to work as well).

To accomplish this, DataHub Frontend issues requests to the Metadata Service to generate an Access Token *on behalf of* a
user who has logged in with the DataHub UI.

## The Future is Bright for Authentication 

These changes represent the first milestone in Metadata Service Authentication. They will serve as a foundation upon which we can build new features as requested by the community:

1. **Custom Authenticator Plugins**: Configure + register custom Authenticator implementations, without forking DataHub. 
2. **Service Accounts**: Create service accounts and generate Access tokens on their behalf. 
3. **Kafka Ingestion Authentication**: Authenticate ingestion requests coming from the Kafka ingestion sink inside the Metadata Service.
4. **Access Token Management**: Ability to view, manage, and revoke access tokens that have been generated. (Currently, access tokens inlcude no server side state, and thus cannot be revoked once granted)

...and more! To request prioritization of these authentication features or others, let us know on [Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email). 

## Q&As 

### What if I don't want to use Metadata Service Authentication?

That's perfectly fine, for now. Metadata Service Authentication is disabled by default, only enabled if you provide the 
environment variable `METADATA_SERVICE_AUTH_ENABLED` to the `datahub-gms` container or change the `authentication.enabled` to "true"
inside your DataHub Metadata Service configuration (`application.yml`). 

That being said, we will be recommending that you enable Authentication for production use cases, to prevent
arbitrary actors from ingesting metadata into DataHub. 

### If I enable Metadata Service Authentication, will ingestion stop working? 

If you enable Metadata Service Authentication, you will want to provide a value for the "token" configuration value
when using the `datahub-rest` sink in your [Ingestion Recipes](https://datahubproject.io/docs/metadata-ingestion/#recipes). See
the [Rest Sink Docs](https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub#config-details) for configuration details.

We'd recommend generating a Personal Access Token (described above) from a trusted DataHub Account (e.g. root 'datahub' user) when configuring
your Ingestion sources.

Note that you can also provide the "extraHeaders" configuration in `datahub-rest` sink to specify a custom header to
pass with each request. This can be used in conjunction to authenticate using a custom Authenticator, for example. 

### How do I generate an Access Token for a service account?

There is no formal concept of "service account" or "bot" on DataHub (yet). For now, we recommend you configure any
programmatic clients of DataHub to use a Personal Access Token generated from a user with the correct privileges, for example
the root "datahub" user account. 

### I want to authenticate requests using a custom Authenticator? How do I do this? 

You can configure DataHub to add your custom **Authenticator** to the **Authentication Chain** by changing the `application.yml` configuration file for the Metadata Service:

```yml
authentication:
  enabled: true # Enable Metadata Service Authentication 
  ....
  authenticators: # Configure an Authenticator Chain 
    - type: <fully-qualified-authenticator-class-name> # E.g. com.linkedin.datahub.authentication.CustomAuthenticator
      configs: # Specific configs that should be passed into 'init' method of Authenticator
        customConfig1: <value> 
```

Notice that you will need to have a class that implements the `Authenticator` interface with a zero-argument constructor available on the classpath
of the Metadata Service java process.

We love contributions! Feel free to raise a PR to contribute an Authenticator back if it's generally useful. 

### Now that I can make authenticated requests to either DataHub Proxy Service and DataHub Metadata Service, which should I use?

Previously, we were recommending that folks contact the Metadata Service directly when doing things like

- ingesting Metadata via recipes
- issuing programmatic requests to the Rest.li APIs 
- issuing programmatic requests to the GraphQL APIs 

With these changes, we will be shifting to the recommendation that folks direct all traffic, whether it's programmatic or not, 
to the **DataHub Frontend Proxy**, as routing to Metadata Service endpoints is currently available at the path `/api/gms`. 
This recommendation is in effort to minimize the exposed surface area of DataHub to make securing, operating, maintaining, and developing
the platform simpler.

In practice, this will require migrating Metadata Ingestion Recipes use the `datahub-rest` sink to pointing at a slightly different
host + path: 

Example recipe that proxies through DataHub Frontend 

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    ...
    token: <your-personal-access-token> 
```

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on [Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email)!