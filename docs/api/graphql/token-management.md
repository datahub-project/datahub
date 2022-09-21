# Access Token Management

DataHub provides the following GraphQL endpoints for managing Access Tokens. In this page you will see examples as well
as explanations as to how to administrate access tokens within the project whether for yourself or others, depending on the caller's privileges.

*Note*: This API makes use of DataHub Policies to safeguard against improper use. By default, a user will not be able to interact with it at all unless they have at least `Generate Personal Access Tokens` privileges.

### Generating Access Tokens

To generate an access token, simply use the `createAccessToken(input: GetAccessTokenInput!)` GraphQL Query.
This endpoint will return an `AccessToken` object, containing the access token string itself alongside with metadata
which will allow you to identify said access token later on.

For example, to generate an access token for the `datahub` corp user, you can issue the following GraphQL Query:

*As GraphQL*

```graphql
mutation {
  createAccessToken(input: {type: PERSONAL, actorUrn: "urn:li:corpuser:datahub", duration: ONE_HOUR, name: "my personal token"}) {
    accessToken
    metadata {
      id
      name
      description
    }
  }
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ createAccessToken(input: { type: PERSONAL, actorUrn: \"urn:li:corpuser:datahub\", duration: ONE_HOUR, name: \"my personal token\" } ) { accessToken metadata { id name description} } }", "variables":{}}'
```

### Listing Access Tokens

Listing tokens is a powerful endpoint that allows you to list the tokens owned by a particular user (ie. YOU). 
To list all tokens that you own, you must specify a filter with: `{field: "actorUrn", value: "<your user urn>"}` configuration. 

*As GraphQL*

```graphql
{
  listAccessTokens(input: {start: 0, count: 100, filters: [{field: "ownerUrn", value: "urn:li:corpuser:datahub"}]}) {
    start
    count
    total
    tokens {
      urn
      id
      actorUrn
    }
  }
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ listAccessTokens(input: {start: 0, count: 100, filters: [{field: \"ownerUrn\", value: \"urn:li:corpuser:datahub\"}]}) { start count total tokens {urn id actorUrn} } }", "variables":{}}'
```

Admin users can also list tokens owned by other users of the platform. To list tokens belonging to other users, you must have the `Manage All Access Tokens` Platform privilege. 

*As GraphQL*

```graphql
{
  listAccessTokens(input: {start: 0, count: 100, filters: []}) {
    start
    count
    total
    tokens {
      urn
      id
      actorUrn
    }
  }
}
```

*As CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ listAccessTokens(input: {start: 0, count: 100, filters: []}) { start count total tokens {urn id actorUrn} } }", "variables":{}}'
```

Other filters besides `actorUrn=<some value>` are possible. You can filter by property in the `DataHubAccessTokenInfo` aspect which you can find in the Entities documentation.

### Revoking Access Tokens

To revoke an existing access token, you can use the `revokeAccessToken` mutation. 

*As GraphQL*

```graphql
mutation {
  revokeAccessToken(tokenId: "HnMJylxuowJ1FKN74BbGogLvXCS4w+fsd3MZdI35+8A=")
}
```

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{"query":"mutation {revokeAccessToken(tokenId: \"HnMJylxuowJ1FKN74BbGogLvXCS4w+fsd3MZdI35+8A=\")}","variables":{}}}'
```

This endpoint will return a boolean detailing whether the operation was successful. In case of failure, an error message will appear explaining what went wrong.

## Feedback, Feature Requests, & Support

Visit our [Slack channel](https://slack.datahubproject.io) to ask questions, tell us what we can do better, & make requests for what you'd like to see in the future. Or just
stop by to say 'Hi'. 
