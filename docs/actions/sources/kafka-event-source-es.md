# Origen del evento Kafka

## Visión general

El origen de eventos de Kafka es el origen de eventos predeterminado que se utiliza en el marco de acciones de DataHub.

Bajo el capó, la fuente de eventos de Kafka utiliza un consumidor de Kafka para suscribirse a la transmisión de temas
fuera de DataHub (MetadataChangeLog_v1, PlatformEvent_v1). Cada acción se coloca automáticamente en una acción única
[grupo de consumidores](https://docs.confluent.io/platform/current/clients/consumer.html#consumer-groups) basado en
el único `name` proporcionado dentro del archivo de configuración action.

Esto significa que puede escalar fácilmente el procesamiento de acciones compartiendo el mismo archivo de configuración de acciones en todas partes.
múltiples nodos o procesos. Siempre y cuando el `name` de la Acción es la misma, cada instancia del marco de Acciones se suscribirá como miembro en el mismo Grupo de Consumidores de Kafka, lo que permite equilibrar la carga
Tráfico de temas entre consumidores que cada uno consume independientemente [Particiones](https://developer.confluent.io/learn-kafka/apache-kafka/partitions/#kafka-partitioning).

Dado que el origen de eventos de Kafka utiliza grupos de consumidores de forma predeterminada, las acciones que utilizan este origen serán **Stateful**.
Esto significa que Actions realizará un seguimiento de sus compensaciones de procesamiento de los temas de Kafka aguas arriba. Si usted
Detenga una acción y reinícielo algún tiempo después, primero se "pondrá al día" procesando los mensajes que el tema
ha recibido desde la última ejecución de la Acción. Tenga en cuenta esto: si su acción es computacionalmente costosa, puede ser preferible comenzar a consumir desde el final del registro, en lugar de ponerse al día. La forma más fácil de lograr esto es simplemente cambiar el nombre de la Acción dentro del archivo de configuración de Acción: esto creará un nuevo Grupo de consumidores de Kafka que comenzará a procesar nuevos mensajes al final del registro (última política).

### Garantías de procesamiento

Este origen de eventos implementa una función "ack" que se invoca si y sólo si un evento se procesa correctamente
por el marco de Acciones, lo que significa que el evento llegó a través de los Transformadores y en la Acción sin
cualquier error. Bajo el capó, el método "ack" compromete sincrónicamente Kafka Consumer Offsets en nombre de la Acción. Esto significa que, de forma predeterminada, el marco proporciona *al menos una vez* semántica de procesamiento. Es decir, en el caso inusual de que se produzca un error al intentar confirmar compensaciones de nuevo a Kafka, ese evento puede reproducirse al reiniciar la acción.

Si ha configurado la canalización de acciones `failure_mode` ser `CONTINUE` (el valor predeterminado), luego eventos que
Si no se procesa, simplemente se registrará en un `failed_events.log` archivo para una mayor investigación (cola de letra muerta). El origen de eventos de Kafka continuará progresando en relación con los temas subyacentes y continuará confirmando compensaciones incluso en el caso de mensajes fallidos.

Si ha configurado la canalización de acciones `failure_mode` ser `THROW`y, a continuación, los eventos que no se procesan producen un error de canalización de acciones. Esto, a su vez, termina la tubería antes de comprometer las compensaciones de regreso a Kafka. Por lo tanto, el mensaje no será marcado como "procesado" por el consumidor de Action.

## Eventos admitidos

La fuente de eventos de Kafka produce

*   [Evento de cambio de entidad V1](../events/entity-change-event.md)
*   [Registro de cambios de metadatos V1](../events/metadata-change-log-event.md)

## Configurar el origen de eventos

Utilice las siguientes configuraciones para comenzar con el origen de eventos de Kafka.

```yml
name: "pipeline-name"
source:
  type: "kafka"
  config:
    # Connection-related configuration
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
      # Dictionary of freeform consumer configs propagated to underlying Kafka Consumer 
      consumer_config: 
          #security.protocol: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}
          #ssl.keystore.location: ${KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION:-/mnt/certs/keystore}
          #ssl.truststore.location: ${KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION:-/mnt/certs/truststore}
          #ssl.keystore.password: ${KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD:-keystore_password}
          #ssl.key.password: ${KAFKA_PROPERTIES_SSL_KEY_PASSWORD:-keystore_password}
          #ssl.truststore.password: ${KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD:-truststore_password}
    # Topic Routing - which topics to read from.
    topic_routes:
      mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1} # Topic name for MetadataChangeLog_v1 events. 
      pe: ${PLATFORM_EVENT_TOPIC_NAME:-PlatformEvent_v1} # Topic name for PlatformEvent_v1 events. 
action:
  # action configs
```

<details>
  <summary>View All Configuration Options</summary>

| | de campo | requerido | predeterminada Descripción |
| --- | :-: | :-: | --- |
| `connection.bootstrap` | ✅ | N/A | El URI de arranque de Kafka, por ejemplo, `localhost:9092`. |
| `connection.schema_registry_url` | ✅ | N/A | La dirección URL del registro de esquemas de Kafka, por ejemplo, `http://localhost:8081` |
| `connection.consumer_config` | ❌ | {} | Conjunto de pares clave-valor que representa configuraciones arbitrarias de Kafka Consumer |
| `topic_routes.mcl` | ❌  | `MetadataChangeLog_v1` | El nombre del tema que contiene los eventos MetadataChangeLog |
| `topic_routes.pe` | ❌ | `PlatformEvent_v1` | El nombre del tema que contiene los eventos PlatformEvent |

</details>

## PREGUNTAS MÁS FRECUENTES

1.  ¿Hay alguna manera de comenzar siempre a procesar desde el final de los temas en Inicio de acciones?

Actualmente, la única forma es cambiar el `name` de la Acción en su archivo de configuración. En el futuro,
Esperamos agregar soporte de primera clase para configurar la acción para que sea "sin estado", es decir, solo el proceso
mensajes que se reciben mientras se ejecuta la acción.

2.  ¿Hay alguna manera de confirmar de forma asincrónica las compensaciones a Kafka?

Actualmente, todas las confirmaciones de desplazamiento del consumidor se realizan de forma sincrónica para cada mensaje recibido. Por ahora, hemos optimizado la corrección sobre el rendimiento. Si esta política de confirmación no se adapta a las necesidades de su organización, sin duda comuníquese con [Flojo](https://slack.datahubproject.io/).
