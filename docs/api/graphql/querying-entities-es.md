# Consulta de entidades de metadatos

### Aprenda a obtener y actualizar entidades en su gráfico de metadatos mediante programación

## Consultas: Lectura de una entidad

DataHub proporciona las siguientes consultas de GraphQL para recuperar entidades en su gráfico de metadatos.

### Obtención de una entidad de metadatos

Para recuperar una entidad de metadatos por clave principal (urna), simplemente use el botón `<entityName>(urn: String!)` Consulta GraphQL.

Por ejemplo, para recuperar una entidad de usuario, puede emitir la siguiente consulta de GraphQL:

*Como GraphQL*

```graphql
{
  corpUser(urn: "urn:li:corpuser:datahub") {
    username
    urn
  }
}
```

*Como CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ corpUser(urn: \"urn:li:corpuser:datahub\") { username urn } }", "variables":{}}'
```

### Búsqueda de una entidad de metadatos

Para realizar una búsqueda de texto completo en una entidad de un tipo determinado, utilice el botón `search(input: SearchInput!)` Consulta GraphQL.

Como GraphQL:

```graphql
{
  search(input: { type: DATASET, query: "my sql dataset", start: 0, count: 10 }) {
    start
    count
    total
    searchResults {
      entity {
         urn
         type
         ...on Dataset {
            name
         }
      }
    }
  }
}
```

Como CURL:

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query":"{ search(input: { type: DATASET, query: \"my sql dataset\", start: 0, count: 10 }) { start count total searchResults { entity { urn type ...on Dataset { name } } } } }", "variables":{}}'
```

Tenga en cuenta que también se pueden proporcionar criterios de filtrado por campo.

### Consulta de propietarios de un conjunto de datos

Como GraphQL:

```graphql
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
    ownership {
      owners {
        owner {
          ... on CorpUser {
            urn
            type
          }
          ... on CorpGroup {
            urn
            type
          }
        }
      }
    }
  }
}
```

### Consulta de etiquetas de un conjunto de datos

Como GraphQL:

```graphql
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)") {
    tags {
      tags {
        tag {
          name
        }
      }
    }
  }
}
```

### Próximamente

¡Enumere las entidades de metadatos! listDatasets, listDashboards, listCharts, listDataFlows, listDataJobs, listTags

## Mutaciones: Modificación de una entidad

DataHub proporciona las siguientes mutaciones de GraphQL para actualizar entidades en su gráfico de metadatos.

### Autorización

Mutaciones que cambian los metadatos de la entidad están sujetas a [Políticas de acceso a DataHub](../../../docs/policies.md). Esto significa que el servidor de DataHub
comprobará si el actor solicitante está autorizado para realizar la acción. Si está consultando el punto de enlace de GraphQL a través de DataHub
Servidor proxy, que se analiza más en [Empezar](./getting-started.md), luego la cookie de sesión proporcionada llevará la información del actor.
Si está consultando la API del servicio de metadatos directamente, deberá proporcionarla a través de un mensaje especial `X-DataHub-Actor` Encabezado HTTP, que debería
contener la URN (clave principal) del actor que realiza la solicitud. Por ejemplo `X-DataHub-Actor: urn:li:corpuser:datahub`.

### Actualización de una entidad de metadatos

Para actualizar una entidad de metadatos existente, simplemente utilice el botón `update<entityName>(urn: String!, input: EntityUpdateInput!)` Consulta GraphQL.

Por ejemplo, para actualizar una entidad Dashboard, puede emitir la siguiente mutación graphQL:

*Como GraphQL*

```graphql
mutation updateDashboard {
    updateDashboard(
        urn: "urn:li:dashboard:(looker,baz)",
        input: {
            editableProperties: {
                description: "My new desription"
            }
        }
    ) {
        urn
    }
}
```

*Como CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDashboard { updateDashboard(urn:\"urn:li:dashboard:(looker,baz)\", input: { editableProperties: { description: \"My new desription\" } } ) { urn } }", "variables":{}}'
```

**Ten cuidado**: estas API le permiten realizar cambios significativos en una entidad de metadatos, que a menudo incluyen
actualizando todo el conjunto de Owners & Tags.

### Agregar y eliminar etiquetas / términos del glosario

Para adjuntar una etiqueta o un término de glosario a una entidad de metadatos, puede utilizar el comando `addTag` y `addTerm` Mutaciones.
Para eliminarlos, puede utilizar el botón `removeTag` y `removeTerm` Mutaciones.

Por ejemplo, para agregar una etiqueta a una entidad DataFlow, puede emitir la siguiente mutación graphQL:

*Como GraphQL*

```graphql
mutation addTag {
    addTag(input: { tagUrn: "urn:li:tag:NewTag", resourceUrn: "urn:li:dataFlow:(airflow,dag_abc,PROD)" })
}
```

*Como CURL*

```curl
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'X-DataHub-Actor: urn:li:corpuser:datahub' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTag { addTag(input: { tagUrn: \"urn:li:tag:NewTag\", resourceUrn: \"urn:li:dataFlow:(airflow,dag_abc,PROD)\" }) }", "variables":{}}'
```

### Próximamente

**Creación de entidades**: createDataset, createDashboard, createChart, etc.
**Eliminación de entidades**: removeDataset, removeDashboard, removeChart, etc.

## Manejo de errores

En GraphQL, las solicitudes que tienen errores no siempre dan como resultado un cuerpo de respuesta HTTP que no sea 200. En su lugar, los errores serán
presente en el cuerpo de respuesta dentro de un nivel superior `errors` campo.

Esto permite situaciones en las que el cliente es capaz de tratar con gracia los datos parciales devueltos por el servidor de aplicaciones.
Para comprobar que no se ha devuelto ningún error después de realizar una solicitud de GraphQL, asegúrese de comprobar *ambos* el `data` y `errors` campos que se devuelven.

## Comentarios, solicitudes de funciones y soporte

Visita nuestro [Canal de Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email) para hacer preguntas, decirnos qué podemos hacer mejor y hacer solicitudes de lo que le gustaría ver en el futuro. O simplemente
pásate a decir 'Hola'.
