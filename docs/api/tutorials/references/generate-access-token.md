# Generate Access Token
With CURL, you need to provide tokens. To generate token, run the following comand. 

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"mutation { createAccessToken(input: { type: PERSONAL, actorUrn: \"urn:li:corpuser:datahub\", duration: ONE_HOUR, name: \"my personal token\" } ) { accessToken metadata { id name description} } }", "variables":{}}'
```

Expected Response: 
```json
{
  "data": {
    "createAccessToken": {
      "accessToken": <my-access-token>,
      "metadata": {
        "id": <my-id>,
        "name": "my personal token",
        "description": null
      }
    }
  },
  "extensions": {}
}
```

You can now copy `accessToken` and pass it to header.
