***

## título: "Serving Tier"

# Arquitectura de servicio de DataHub

La siguiente figura muestra el diagrama de sistemas de alto nivel para el nivel de servicio de DataHub.

![datahub-serving](../imgs/datahub-serving.png)

El componente principal se llama [el servicio de metadatos](../../metadata-service) y expone una API REST y una API GraphQL para realizar operaciones CRUD en metadatos. El servicio también expone API-s de consulta de búsqueda y gráficos para admitir consultas de estilo de índice secundario, consultas de búsqueda de texto completo, así como consultas de relación como linaje. Además, el [datahub-frontend](../../datahub-frontend) exponer una API de GraphQL sobre el gráfico de metadatos.

## Componentes de nivel de servicio de DataHub

### Almacenamiento de metadatos

El servicio de metadatos de DataHub conserva los metadatos en un almacén de documentos (un RDBMS como MySQL, Postgres o Cassandra, etc.).

### Flujo de registro de cambios de metadatos (MCL)

El nivel de servicio de DataHub también emite un evento de confirmación [Registro de cambios de metadatos][Metadata Change Log] cuando un cambio de metadatos se ha confirmado correctamente en el almacenamiento persistente. Este evento se envía a través de Kafka.

El flujo MCL es una API pública y puede ser suscrito por sistemas externos (por ejemplo, el Marco de Acciones) que proporciona una forma extremadamente poderosa de reaccionar en tiempo real a los cambios que ocurren en los metadatos. Por ejemplo, podría crear un ejecutor de control de acceso que reaccione al cambio en los metadatos (por ejemplo, un conjunto de datos previamente legible en todo el mundo ahora tiene un campo pii) para bloquear inmediatamente el conjunto de datos en cuestión.
Tenga en cuenta que no todos los MCP-s darán como resultado un MCL, ya que el nivel de servicio de DataHub ignorará cualquier cambio duplicado en los metadatos.

### Aplicador de índice de metadatos (mae-consumer-job)

\[Los registros de cambio de metadatos]s son consumidos por otro trabajo de Kafka Streams, [mae-consumidor-trabajo][mae-consumer-job], que aplica los cambios a la [gráfico][graph] y [índice de búsqueda][search index] en consecuencia.
El trabajo es independiente de la entidad y ejecutará los correspondientes generadores de gráficos e índices de búsqueda, que serán invocados por el trabajo cuando se cambie un aspecto específico de los metadatos.
El constructor debe indicar al trabajo cómo actualizar el gráfico y el índice de búsqueda en función del cambio de metadatos.

Para garantizar que los cambios de metadatos se procesan en el orden cronológico correcto, la entidad introduce MCL [URNA][URN] — lo que significa que todos los MAE de una entidad en particular serán procesados secuencialmente por un único hilo de flujos de Kafka.

### Servicio de consulta de metadatos

Lecturas basadas en claves primarias (por ejemplo, obtener metadatos de esquema para un conjunto de datos basado en el `dataset-urn`) en los metadatos se enrutan al almacén de documentos. Las lecturas basadas en índices secundarios en metadatos se enrutan al índice de búsqueda (o alternativamente pueden usar la compatibilidad con índices secundarios muy coherente descrita [aquí]()). Las consultas de texto completo y de búsqueda avanzada se enrutan al índice de búsqueda. Las consultas de grafos complejos, como el linaje, se enrutan al índice de grafos.

[RecordTemplate]: https://github.com/linkedin/rest.li/blob/master/data/src/main/java/com/linkedin/data/template/RecordTemplate.java

[GenericRecord]: https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/generic/GenericRecord.java

[Pegasus]: https://linkedin.github.io/rest.li/DATA-Data-Schema-and-Templates

[relationship]: ../what/relationship.md

[entity]: ../what/entity.md

[aspect]: ../what/aspect.md

[GMS]: ../what/gms.md

[Metadata Change Log]: ../what/mxe.md#metadata-change-log-mcl

[rest.li]: https://rest.li

[Metadata Change Proposal (MCP)]: ../what/mxe.md#metadata-change-proposal-mcp

[Metadata Change Log (MCL)]: ../what/mxe.md#metadata-change-log-mcl

[MCP]: ../what/mxe.md#metadata-change-proposal-mcp

[MCL]: ../what/mxe.md#metadata-change-log-mcl

[equivalent Pegasus format]: https://linkedin.github.io/rest.li/how_data_is_represented_in_memory#the-data-template-layer

[graph]: ../what/graph.md

[search index]: ../what/search-index.md

[mce-consumer-job]: ../../metadata-jobs/mce-consumer-job

[mae-consumer-job]: ../../metadata-jobs/mae-consumer-job

[Remote DAO]: ../architecture/metadata-serving.md#remote-dao

[URN]: ../what/urn.md

[Metadata Modelling]: ../modeling/metadata-model.md

[Entity]: ../what/entity.md

[Relationship]: ../what/relationship.md

[Search Document]: ../what/search-document.md

[metadata aspect]: ../what/aspect.md

[Python emitters]: https://datahubproject.io/docs/metadata-ingestion/#using-as-a-library
