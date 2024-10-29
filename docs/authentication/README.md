# Overview

Authentication is the process of verifying the identity of a user or service. There are two
places where Authentication occurs inside DataHub:

1. DataHub frontend service when a user attempts to log in to the DataHub application.
2. DataHub backend service when making API requests to DataHub.

In this document, we'll tak a closer look at both. 

### Authentication in the Frontend

Authentication of normal users of DataHub takes place in two phases. 

At login time, authentication is performed by either DataHub itself (via username / password entry) or a third-party Identity Provider. Once the identity 
of the user has been established, and credentials validated, a persistent session token is generated for the user and stored
in a browser-side session cookie. 

DataHub provides 3 mechanisms for authentication at login time:

- **Native Authentication** which uses username and password combinations natively stored and managed by DataHub, with users invited via an invite link.
- [Single Sign-On with OpenID Connect](guides/sso/configure-oidc-react.md) to delegate authentication responsibility to third party systems like Okta or Google/Azure Authentication. This is the recommended approach for production systems.
- [JaaS Authentication](guides/jaas.md) for simple deployments where authenticated users are part of some known list or invited as a [Native DataHub User](guides/add-users.md).

In subsequent requests, the session token is used to represent the authenticated identity of the user, and is validated by DataHub's backend service (discussed below).
Eventually, the session token is expired (24 hours by default), at which point the end user is required to log in again.

### Authentication in the Backend (Metadata Service)

When a user makes a request for Data within DataHub, the request is authenticated by DataHub's Backend (Metadata Service) via a JSON Web Token. This applies to both requests originating from the DataHub application,
and programmatic calls to DataHub APIs. There are two types of tokens that are important:

1. **Session Tokens**: Generated for users of the DataHub web application. By default, having a duration of 24 hours. 
These tokens are encoded and stored inside browser-side session cookies. The duration a session token is valid for is configurable via the `MAX_SESSION_TOKEN_AGE` environment variable
on the datahub-frontend deployment. Additionally, the `AUTH_SESSION_TTL_HOURS` configures the expiration time of the actor cookie on the user's browser which will also prompt a user login. The difference between these is that the actor cookie expiration only affects the browser session and can still be used programmatically,
but when the session expires it can no longer be used programmatically either as it is created as a JWT with an expiration claim.
2. **Personal Access Tokens**: These are tokens generated via the DataHub settings panel useful for interacting
with DataHub APIs. They can be used to automate processes like enriching documentation, ownership, tags, and more on DataHub. Learn
more about Personal Access Tokens [here](personal-access-tokens.md). 

To learn more about DataHub's backend authentication, check out [Introducing Metadata Service Authentication](introducing-metadata-service-authentication.md).

Credentials must be provided as Bearer Tokens inside of the **Authorization** header in any request made to DataHub's API layer. To learn 

```shell
Authorization: Bearer <your-token>
```

Note that in DataHub local quickstarts, Authentication at the backend layer is disabled for convenience. This leaves the backend
vulnerable to unauthenticated requests and should not be used in production. To enable
backend (token-based) authentication, simply set the `METADATA_SERVICE_AUTH_ENABLED=true` environment variable
for the datahub-gms container or pod. 

### References

For a quick video on the topic of users and groups within DataHub, have a look at [DataHub Basics — Users, Groups, & Authentication 101
](https://youtu.be/8Osw6p9vDYY)