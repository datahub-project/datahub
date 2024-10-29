# Metadata Service Authentication

## Introduction

This document provides a technical overview of the how authentication works in DataHub's backend aimed at developers evaluating or operating DataHub. 
It includes a characterization of the motivations for the feature, the key components in its design, the new capabilities it provides, & configuration instructions. 

## Background

Let's recall 2 critical components of DataHub's architecture: 

- **DataHub Frontend Proxy** (datahub-frontend) - Resource server that routes requests to downstream Metadata Service
- **DataHub Metadata Service** (datahub-gms) - Source of truth for storing and serving DataHub Metadata Graph. 

Previously, Authentication was exclusively handled by the Frontend Proxy. This service would perform the following steps 
when a user navigated to `http://localhost:9002/`:

  a. Check for the presence of a special `PLAY_SESSION` cookie.

  b. If cookie was present + valid, redirect to the home page

  c. If cookie was invalid, redirect to either a) the DataHub login screen (for [JAAS authentication](guides/jaas.md) or b) a [configured OIDC Identity Provider](guides/sso/configure-oidc-react.md) to perform authentication.

Once authentication had succeeded at the frontend proxy layer, a stateless (token-based) session cookie (PLAY_SESSION) would be set in the users browser.
All subsequent requests, including the GraphQL requests issued by the React UI, would be authenticated using this session cookie. Once a request had made it beyond
the frontend service layer, it was assumed to have been already authenticated. Hence, there was **no native authentication inside of the Metadata Service**. 

### Problems with this approach

The major challenge with this situation is that requests to the backend Metadata Service were completely unauthenticated. There were 2 options for folks who required authentication at the Metadata Service layer:

1. Set up a proxy in front of Metadata Service that performed authentication
2. [A more recent possibility] Route requests to Metadata Service through DataHub Frontend Proxy, including the PLAY_SESSION
Cookie with every request.
   
Neither of which are ideal. Setting up a proxy to do authentication takes time & expertise. Extracting and setting a session cookie from the browser for programmatic is
clunky & unscalable. On top of that, extending the authentication system was difficult, requiring implementing a new [Play module](https://www.playframework.com/documentation/2.8.8/api/java/play/mvc/Security.Authenticator.html) within DataHub Frontend.

## Introducing Authentication in DataHub Metadata Service

To address these problems, we introduced configurable Authentication inside the **Metadata Service** itself, 
meaning that requests are no longer considered trusted until they are authenticated by the Metadata Service.

Why push Authentication down? In addition to the problems described above, we wanted to plan for a future
where Authentication of Kafka-based-writes could be performed in the same manner as Rest writes.

## Configuring Metadata Service Authentication

Metadata Service Authentication is currently **opt-in**. This means that you may continue to use DataHub without Metadata Service Authentication without interruption.
To enable Metadata Service Authentication:

- set the `METADATA_SERVICE_AUTH_ENABLED` environment variable to "true" for the `datahub-gms` AND `datahub-frontend` containers / pods. 
  
OR

- change the Metadata Service `application.yaml` configuration file to set `authentication.enabled` to "true" AND
- change the Frontend Proxy Service `application.config` configuration file to set `metadataService.auth.enabled` to "true"

After setting the configuration flag, simply restart the Metadata Service to start enforcing Authentication. 

Once enabled, all requests to the Metadata Service will need to be authenticated; if you're using the default Authenticators
that ship with DataHub, this means that all requests will need to present an Access Token in the Authorization Header as follows:

```
Authorization: Bearer <access-token> 
```

For users logging into the UI, this process will be handled for you. When logging in, a cookie will be set in your browser that internally
contains a valid Access Token for the Metadata Service. When browsing the UI, this token will be extracted and sent to the Metadata Service
to authenticate each request.

For users who want to access the Metadata Service programmatically, i.e. for running ingestion, the current recommendation is to generate
a **Personal Access Token** (described above) from the root "datahub" user account, and using this token when configuring your [Ingestion Recipes](../../metadata-ingestion/README.md#recipes). 
To configure the token for use in ingestion, simply populate the "token" configuration for the `datahub-rest` sink:

```
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    ...
    token: <your-personal-access-token-here!> 
```

> Note that ingestion occurring via `datahub-kafka` sink will continue to be Unauthenticated *for now*. Soon, we will be introducing
> support for providing an access token in the event payload itself to authenticate ingestion requests over Kafka. 


### The Role of DataHub Frontend Proxy Going Forward

With these changes, DataHub Frontend Proxy will continue to play a vital part in the complex dance of Authentication. It will serve as the place
where UI-based session authentication originates and will continue to support 3rd Party SSO configuration (OIDC)
and JAAS configuration as it does today. 

The major improvement is that the Frontend Service will validate credentials provided at UI login time
and generate a DataHub **Access Token**, embedding it into traditional session cookie (which will continue to work).

In summary, DataHub Frontend Service will continue to play a vital role to Authentication. It's scope, however, will likely
remain limited to concerns specific to the React UI.

## Where to go from here

These changes represent the first milestone in Metadata Service Authentication. They will serve as a foundation upon which we can build new features, prioritized based on Community demand:

1. **Dynamic Authenticator Plugins**: Configure + register custom Authenticator implementations, without forking DataHub. 
2. **Service Accounts**: Create service accounts and generate Access tokens on their behalf. 
3. **Kafka Ingestion Authentication**: Authenticate ingestion requests coming from the Kafka ingestion sink inside the Metadata Service.
4. **Access Token Management**: Ability to view, manage, and revoke access tokens that have been generated. (Currently, access tokens inlcude no server side state, and thus cannot be revoked once granted)

...and more! To advocate for these features or others, reach out on [Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email). 

## Q&As 

### What if I don't want to use Metadata Service Authentication?

That's perfectly fine, for now. Metadata Service Authentication is disabled by default, only enabled if you provide the 
environment variable `METADATA_SERVICE_AUTH_ENABLED` to the `datahub-gms` container or change the `authentication.enabled` to "true"
inside your DataHub Metadata Service configuration (`application.yaml`). 

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

You can configure DataHub to add your custom **Authenticator** to the **Authentication Chain** by changing the `application.yaml` configuration file for the Metadata Service:

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

In practice, this will require migrating Metadata [Ingestion Recipes](../../metadata-ingestion/README.md#recipes) use the `datahub-rest` sink to pointing at a slightly different
host + path.

Example recipe that proxies through DataHub Frontend 

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    ...
    token: <your-personal-access-token-here!> 
```

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on [Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email)!