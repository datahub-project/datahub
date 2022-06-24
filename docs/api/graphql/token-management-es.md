# Administración de tokens de acceso

DataHub proporciona los siguientes extremos de GraphQL para administrar tokens de acceso. En esta página también verás ejemplos
como explicaciones sobre cómo administrar tokens de acceso dentro del proyecto, ya sea para usted u otros, dependiendo de los privilegios de la persona que llama.

Nota: Esta API hace uso de políticas para protegerse contra el uso indebido. De forma predeterminada, un usuario no podrá interactuar con él en absoluto a menos que tenga al menos `Generate Personal Access Tokens` Privilegios.
Esto permitirá a un usuario generar / listar y revocar sus tokens, pero no más.
Para que un usuario trabaje con tokens para otros usuarios, debe tener `Manage All Access Tokens`.
Se recomienda ENCARECIDAMENTE que solo los administradores del sistema DataHub tengan privilegios de nivel de administración.

### Generar tokens

Para generar un token de acceso, simplemente use el botón `createAccessToken(input: GetAccessTokenInput!)` Consulta GraphQL.
Este extremo devolverá un `AccessToken` objeto, que contiene la propia cadena de token de acceso junto con los metadatos
lo que le permitirá identificar dicho token de acceso más adelante.

Por ejemplo, para generar un token de acceso para el `datahub` corp user, puede emitir la siguiente consulta GraphQL:

*Como GraphQL*

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

*Como CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ createAccessToken(input: { type: PERSONAL, actorUrn: \"urn:li:corpuser:datahub\", duration: ONE_HOUR, name: \"my personal token\" } ) { accessToken metadata { id name description} } }", "variables":{}}'
```

### Lista de tokens

Listar tokens es un punto final poderoso, como tal, hay reglas para usar.

Enumere su propio token personal (suponiendo que usted es el usuario de datahub corp). Tenga en cuenta que para enumerar sus propios tokens debe
especifique un filtro con: `{field: "actorUrn", value: "<your user urn>"}` configuración. A menos que especifique este filtro, se producirá un error en el extremo con una excepción no autorizada.

*Como GraphQL*

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

*Como CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ listAccessTokens(input: {start: 0, count: 100, filters: [{field: \"ownerUrn\", value: \"urn:li:corpuser:datahub\"}]}) { start count total tokens {urn id actorUrn} } }", "variables":{}}'
```

Listado de todos los tokens de acceso en el sistema (su usuario debe tener `Manage All Access Tokens` privilegios para que esto funcione)

*Como GraphQL*

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

*Como CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ listAccessTokens(input: {start: 0, count: 100, filters: []}) { start count total tokens {urn id actorUrn} } }", "variables":{}}'
```

Otros filtros además `actorUrn=<some value>` son posibles. Puede filtrar por propiedad en el cuadro `DataHubAccessTokenInfo` aspecto que puede encontrar en la documentación de Entidades.

### Revocar tokens

Revocar un token es una mutación, como tal hay que tener en cuenta la necesidad de especificar `mutation` al enviar el comando GraphQL.

*Como GraphQL*

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

Este extremo devolverá un valor booleano que detalla si la operación se realizó correctamente. En caso de fallo, aparecerá un mensaje de error explicando qué salió mal.
