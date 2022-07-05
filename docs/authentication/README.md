# Overview

Authentication is the process of verifying the identity of a user or service. In DataHub this can be split into 2 main components:
 - How to login into DataHub.
 - How to make some action withing DataHub on **behalf** of a user/service.

:::note

Authentication in DataHub does not necessarily mean that the user/service being authenticated will be part of the metadata graph within DataHub itself other concepts like Datasets or Dashboards.
In other words, a user called `john.smith` logging into DataHub does not mean that john.smith appears as a CorpUser Entity within DataHub.

For a quick video on that subject, have a look at our video on [DataHub Basics — Users, Groups, & Authentication 101
](https://youtu.be/8Osw6p9vDYY)

:::

### Authentication in the Frontend

Authentication in DataHub happens at 2 possible moments, if enabled.

The first happens in the **DataHub Frontend** component when you access the UI.
You will be prompted with a login screen, upon which you must supply a username/password combo or OIDC login to access DataHub's UI.
This is typical scenario for a human interacting with DataHub.

DataHub provides 2 methods of authentication:
 - [JaaS Authentication](guides/jaas.md) for simple deployments where authenticated users are part of some known list or invited as a [Native DataHub User](guides/add-users.md).
 - [OIDC Authentication](guides/sso/configure-oidc-react.md) to delegate authentication responsibility to third party systems like Okta or Google/Azure Authentication. This is the recommended approach for production systems.

Upon validation of a user's credentials through one of these authentication systems, DataHub will generate a session token with which all subsequent requests will be made.

### Authentication in the Backend

The second way in which authentication occurs, is within DataHub's Backend (Metadata Service) when a user makes a request either through the UI or through APIs.
In this case DataHub makes use of Personal Access Tokens or session HTTP headers to apply actions on behalf of some user.
To learn more about DataHub's backend authentication have a look at our docs on [Introducing Metadata Service Authentication](introducing-metadata-service-authentication.md).

Note, while authentication can happen on both the frontend or backend components of DataHub, they are separate, related processes.
The first is to authenticate users/services by a third party system (Open-ID connect or Java based authentication) and the latter to only permit identified requests to be accepted by DataHub via access tokens or bearer cookies.

If you only want some users to interact with DataHub's UI, enable authentication in the Frontend and manage who is allowed either through JaaS or OIDC login methods.
If you want users to be able to access DataHub's backend directly without going through the UI in an authenticated manner, then enable authentication in the backend and generate access tokens for them.
