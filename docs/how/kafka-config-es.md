***

## título: "Configuración de Kafka"&#xD;&#xA;hide_title: verdadero

# Configuración de Kafka en DataHub

DataHub requiere kafka para funcionar. Kafka se utiliza como un registro duradero que se puede utilizar para almacenar entradas
solicitudes para actualizar el gráfico de metadatos (propuesta de cambio de metadatos) o como un registro de cambios que detalla las actualizaciones
que se han realizado en el Gráfico de Metadatos (Registro de Cambios de Metadatos).

## Variables de entorno

Las siguientes variables de entorno se pueden utilizar para personalizar la conexión de DataHub a Kafka para los siguientes componentes de DataHub,
cada uno de los cuales requiere una conexión con Kafka:

*   `metadata-service` (contenedor datahub-gms)
*   (Avanzado: si se implementan consumidores independientes) `mce-consumer-job` (contenedor datahub-mce-consumer)
*   (Avanzado: si se implementan consumidores independientes) `mae-consumer-job` (contenedor datahub-mae-consumer)
*   (Avanzado: si el análisis de productos está habilitado) datahub-frontend

### Configuración de la conexión

Con la excepción de `KAFKA_BOOTSTRAP_SERVER` y `KAFKA_SCHEMAREGISTRY_URL`, Kafka se configura a través de [maletero](https://spring.io/projects/spring-boot), específicamente con [KafkaPropiedades](https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/autoconfigure/kafka/KafkaProperties.html). Ver [Propiedades de integración](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties) prefijado con `spring.kafka`.

A continuación se muestra un ejemplo de cómo se pueden configurar las propiedades SASL/GSSAPI a través de variables de entorno:

```bash
export KAFKA_BOOTSTRAP_SERVER=broker:29092
export KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
export SPRING_KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
export SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
export SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

#### Ejemplo: Conexión mediante AWS IAM (MSK)

Aquí hay otro ejemplo de cómo se puede configurar SASL_SSL para AWS_MSK_IAM al conectarse a MSK mediante IAM a través de variables de entorno

```bash
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION=/tmp/kafka.client.truststore.jks
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=AWS_MSK_IAM
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=software.amazon.msk.auth.iam.IAMLoginModule required;
SPRING_KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Para obtener más información sobre la configuración de estas variables, consulte Spring's [Configuración externalizada](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config) para ver cómo funciona esto.
Ver también [Seguridad de Kafka Connect](https://docs.confluent.io/current/connect/security.html) para más formas de conectarse.

### Configuración del tema

De forma predeterminada, DataHub se basa en un conjunto de temas de Kafka para operar. De forma predeterminada, tienen los siguientes nombres:

*   **MetadataChangeProposal_v1**
*   **FailedMetadataChangeProposal_v1**
*   **MetadataChangeLog_Versioned_v1**
*   **MetadataChangeLog_Timeseries_v1**
*   **DataHubUsageEvent_v1**: Evento de seguimiento del comportamiento del usuario para la interfaz de usuario

6.  (Obsoleto) **MetadataChangeEvent_v4**: Mensajes de propuesta de cambio de metadatos
7.  (Obsoleto) **MetadataAuditEvent_v4**: Mensajes de registro de cambio de metadatos
8.  (Obsoleto) **FailedMetadataChangeEvent_v4**: Error al procesar el evento #1

Estos temas se discuten con más detalle en [Eventos de metadatos](../what/mxe.md).

Hemos incluido variables de entorno para personalizar el nombre de cada uno de estos temas, para los casos en que una organización tiene reglas de nomenclatura para sus temas.

### Servicio de metadatos (datahub-gms)

Las siguientes son variables de entorno que puede usar para configurar los nombres de tema utilizados en el contenedor del servicio de metadatos:

*   `METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: nombre del tema propuestas de cambio de metadatos emitidas por el marco de ingesta.
*   `FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: nombre del tema Propuestas de cambio de metadatos emitidas cuando los MCP no se procesan.
*   `METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME`: nombre del tema de los registros de cambios de metadatos que se producen para los aspectos versionados.
*   `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: nombre del tema de los registros de cambio de metadatos que se producen para los aspectos de la serie de tiempo.
*   `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: el nombre del tema de Platform Events (eventos semánticos de alto nivel).
*   `DATAHUB_USAGE_EVENT_NAME`: el nombre del tema para los eventos de análisis de productos.
*   (Obsoleto) `METADATA_CHANGE_EVENT_NAME`: el nombre del tema del evento de cambio de metadatos.
*   (Obsoleto) `METADATA_AUDIT_EVENT_NAME`: nombre del tema del evento de auditoría de metadatos.
*   (Obsoleto) `FAILED_METADATA_CHANGE_EVENT_NAME`: el nombre del tema del evento de cambio de metadatos con errores.

### Consumidor de MCE (datahub-mce-consumer)

*   `METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: nombre del tema propuestas de cambio de metadatos emitidas por el marco de ingesta.
*   `FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: nombre del tema Propuestas de cambio de metadatos emitidas cuando los MCP no se procesan.
*   (Obsoleto) `METADATA_CHANGE_EVENT_NAME`: el nombre del tema obsoleto del que consumirá un consumidor de MCE incrustado.
*   (Obsoleto) `FAILED_METADATA_CHANGE_EVENT_NAME`: Se escribirá el nombre del tema obsoleto en el que se han producido los MCE fallidos.

### CONSUMIDOR MAE (datahub-mae-consumer)

*   `METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME`: nombre del tema de los registros de cambios de metadatos que se producen para los aspectos versionados.
*   `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: nombre del tema de los registros de cambio de metadatos que se producen para los aspectos de la serie de tiempo.
*   `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: el nombre del tema de Platform Events (eventos semánticos de alto nivel).
*   `DATAHUB_USAGE_EVENT_NAME`: el nombre del tema para los eventos de análisis de productos.
*   (Obsoleto) `METADATA_AUDIT_EVENT_NAME`: nombre del tema del evento de auditoría de metadatos obsoleto.

### Frontend de DataHub (datahub-frontend-react)

*   `DATAHUB_TRACKING_TOPIC`: nombre del tema utilizado para almacenar eventos de uso de DataHub.
    Debe contener el mismo valor que `DATAHUB_USAGE_EVENT_NAME` en el contenedor Servicio de metadatos.

Asegúrese de que estas variables de entorno se establezcan de manera coherente en todo el ecosistema. DataHub tiene algunas aplicaciones diferentes en ejecución que se comunican con Kafka (ver arriba).

## Configuración del identificador de grupo de consumidores

Los consumidores de Kafka en primavera se configuran utilizando oyentes de Kafka. De forma predeterminada, el identificador de grupo de consumidores es el mismo que el identificador de escucha.

Hemos incluido una variable de entorno para personalizar el identificador del grupo de consumidores, si su empresa u organización tiene reglas de nomenclatura específicas.

### datahub-mce-consumer y datahub-mae-consumer

*   `KAFKA_CONSUMER_GROUP_ID`: El nombre del identificador del grupo del consumidor kafka.

## Aplicación de configuraciones

### Estibador

Simplemente agregue las variables de entorno anteriores a las requeridas `docker.env` para los contenedores. Estos pueden
se encuentran dentro de la `docker` del repositorio.

### Timón

En Helm, deberá configurar estas variables de entorno mediante el `extraEnvs` secciones del contenedor específico
configuraciones dentro de su `values.yaml` archivo.

    datahub-gms: 
        ...
        extraEnvs:
          - name: METADATA_CHANGE_PROPOSAL_TOPIC_NAME
            value: "CustomMetadataChangeProposal_v1"
          - name: METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME
            value: "CustomMetadataChangeLogVersioned_v1"
          - name: FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME
            value: "CustomFailedMetadataChangeProposal_v1"
          - name: KAFKA_CONSUMER_GROUP_ID
            value: "my-apps-mae-consumer"
            ....
            
    datahub-frontend:
        ...
        extraEnvs:
            - name: DATAHUB_TRACKING_TOPIC
              value: "MyCustomTrackingEvent"
          
    # If standalone consumers are enabled
    datahub-mae-consumer; 
        extraEnvs:
            - name: METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME
              value: "CustomMetadataChangeLogVersioned_v1"
              ....
            - name: METADATA_AUDIT_EVENT_NAME
              value: "MetadataAuditEvent"
    datahub-mce-consumer; 
        extraEnvs:
            - name: METADATA_CHANGE_PROPOSAL_TOPIC_NAME
              value: "CustomMetadataChangeLogVersioned_v1"
              ....
            - name: METADATA_CHANGE_EVENT_NAME
              value: "MetadataChangeEvent"
            ....

## Otros componentes que utilizan Kafka se pueden configurar utilizando variables de entorno:

*   kafka-setup
*   esquema-registro

## Propiedades SASL/GSSAPI para kafka-setup y datahub-frontend a través de variables de entorno

```bash
KAFKA_BOOTSTRAP_SERVER=broker:29092
KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

## Propiedades SASL/GSSAPI para el registro de esquemas a través de variables de entorno

```bash
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=broker:29092
SCHEMA_REGISTRY_KAFKASTORE_SASL_KERBEROS_SERVICE_NAME=kafka
SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SASL_PLAINTEXT
SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

## SSL

### Kafka

Estamos utilizando el marco de Spring Boot para iniciar nuestras aplicaciones, incluida la configuración de Kafka. Puedes
[Utilizar variables de entorno para establecer las propiedades del sistema](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config-relaxed-binding-from-environment-variables),
Incluido [Propiedades de Kafka](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties).
Desde allí puede configurar su configuración SSL para Kafka.

### Registro de esquemas

Si Schema Registry está configurado para usar seguridad (SSL), también debe establecer valores adicionales.

El [MCE](../../metadata-jobs/mce-consumer-job) y [MAE](../../metadata-jobs/mae-consumer-job) los consumidores pueden establecer
Valores predeterminados del entorno de Spring Kafka, por ejemplo:

*   `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SECURITY_PROTOCOL`
*   `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`
*   `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`
*   `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`
*   `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD`

[FM2](../what/gms.md) Puede establecer las siguientes variables de entorno que se pasarán como propiedades al crear el Registro de esquemas
Cliente.

*   `KAFKA_SCHEMA_REGISTRY_SECURITY_PROTOCOL`
*   `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`
*   `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`
*   `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`
*   `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD`

> **Nota** En los registros es posible que vea algo como
> `The configuration 'kafkastore.ssl.truststore.password' was supplied but isn't a known config.` La configuración es
> no es una configuración requerida para el productor. Estos mensajes WARN se pueden ignorar de forma segura. Cada uno de los servicios de Datahub son
> pasó un conjunto completo de configuración, pero es posible que no requiera todas las configuraciones que se les pasan. Estos advierten
> los mensajes indican que al servicio se le pasó una configuración que no es relevante para él y se puede omitir de forma segura.

> Otros errores: `Failed to start bean 'org.springframework.kafka.config.internalKafkaListenerEndpointRegistry'; nested exception is org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [DataHubUsageEvent_v1]`. Por favor, compruebe los permisos de guardabosques o los registros de kafka broker.
