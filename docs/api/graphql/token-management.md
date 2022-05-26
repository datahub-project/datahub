# Access Token Management

DataHub provides the following GraphQL endpoints for managing access tokens. In this page you will see examples as well
as explanations as to how to administrate access tokens within the project whether for yourself or others, depending on the caller's privileges.

Note: This API makes use of policies to safeguard against improper use. By default, a user will not be able to interact with it at all unless they have at least `Generate Personal Access Tokens` privileges.
This will allow a user to generate/list & revoke their tokens, but no more.
In order for a user to work with tokens for other users they must have `Manage All Access Tokens`.
It is HIGHLY recommended that only admins of the DataHub system have manage level privileges.

### Generate tokens
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

### List Tokens

Listing tokens is a powerful endpoint, as such there are rules to using.

List your own personal token (assuming you are the datahub corp user). Notice that to list your own tokens you must
specify a filter with: `{field: "actorUrn", value: "<your user urn>"}` configuration. Unless you specify this filter, the endpoint will fail with a not authorized exception.

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

Listing all access tokens in the system (your user must have `Manage All Access Tokens` privileges for this to work)

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

### Revoke Tokens
Revoking a token is a mutation, as such bear in mind the need to specify `mutation` keyword when submitting the GraphQL command.

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
