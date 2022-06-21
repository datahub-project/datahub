# Guía de depuración

## ¿Cómo puedo confirmar si todos los contenedores de Docker se están ejecutando como se esperaba después de un inicio rápido?

Si configura el `datahub` Herramienta CLI (consulte [aquí](../metadata-ingestion/README.md)), puede utilizar la utilidad de comprobación integrada:

```shell
datahub docker check
```

Puede enumerar todos los contenedores de Docker en su local ejecutando `docker container ls`. Debe esperar ver un registro similar al siguiente:

    CONTAINER ID        IMAGE                                                 COMMAND                  CREATED             STATUS              PORTS                                                      NAMES
    979830a342ce        linkedin/datahub-mce-consumer:latest                "bash -c 'while ping…"   10 hours ago        Up 10 hours                                                                    datahub-mce-consumer
    3abfc72e205d        linkedin/datahub-frontend-react:latest              "datahub-frontend…"   10 hours ago        Up 10 hours         0.0.0.0:9002->9002/tcp                                     datahub-frontend
    50b2308a8efd        linkedin/datahub-mae-consumer:latest                "bash -c 'while ping…"   10 hours ago        Up 10 hours                                                                    datahub-mae-consumer
    4d6b03d77113        linkedin/datahub-gms:latest                         "bash -c 'dockerize …"   10 hours ago        Up 10 hours         0.0.0.0:8080->8080/tcp                                     datahub-gms
    c267c287a235        landoop/schema-registry-ui:latest                     "/run.sh"                10 hours ago        Up 10 hours         0.0.0.0:8000->8000/tcp                                     schema-registry-ui
    4b38899cc29a        confluentinc/cp-schema-registry:5.2.1                 "/etc/confluent/dock…"   10 hours ago        Up 10 hours         0.0.0.0:8081->8081/tcp                                     schema-registry
    37c29781a263        confluentinc/cp-kafka:5.2.1                           "/etc/confluent/dock…"   10 hours ago        Up 10 hours         0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp           broker
    15440d99a510        docker.elastic.co/kibana/kibana:5.6.8                 "/bin/bash /usr/loca…"   10 hours ago        Up 10 hours         0.0.0.0:5601->5601/tcp                                     kibana
    943e60f9b4d0        neo4j:4.0.6                                           "/sbin/tini -g -- /d…"   10 hours ago        Up 10 hours         0.0.0.0:7474->7474/tcp, 7473/tcp, 0.0.0.0:7687->7687/tcp   neo4j
    6d79b6f02735        confluentinc/cp-zookeeper:5.2.1                       "/etc/confluent/dock…"   10 hours ago        Up 10 hours         2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                 zookeeper
    491d9f2b2e9e        docker.elastic.co/elasticsearch/elasticsearch:5.6.8   "/bin/bash bin/es-do…"   10 hours ago        Up 10 hours         0.0.0.0:9200->9200/tcp, 9300/tcp                           elasticsearch
    ce14b9758eb3        mysql:5.7

También puede comprobar los registros de contenedores individuales de Docker ejecutando `docker logs <<container_name>>`. Para `datahub-gms`, debería ver un registro similar a este al final de la inicialización:

    2020-02-06 09:20:54.870:INFO:oejs.Server:main: Started @18807ms

Para `datahub-frontend-react`, debería ver un registro similar a este al final de la inicialización:

    09:20:22 [main] INFO  play.core.server.AkkaHttpServer - Listening for HTTP on /0.0.0.0:9002

## Mi contenedor de elasticsearch o broker salió con un error o se atascó para siempre

Si está viendo errores como los siguientes, es probable que no haya dado suficientes recursos para docker. Asegúrese de asignar al menos 8 GB de RAM + 2 GB de espacio de intercambio.

    datahub-gms             | 2020/04/03 14:34:26 Problem with request: Get http://elasticsearch:9200: dial tcp 172.19.0.5:9200: connect: connection refused. Sleeping 1s
    broker                  | [2020-04-03 14:34:42,398] INFO Client session timed out, have not heard from server in 6874ms for sessionid 0x10000023fa60002, closing socket connection and attempting reconnect (org.apache.zookeeper.ClientCnxn)
    schema-registry         | [2020-04-03 14:34:48,518] WARN Client session timed out, have not heard from server in 20459ms for sessionid 0x10000023fa60007 (org.apache.zookeeper.ClientCnxn)

## ¿Cómo puedo comprobar si [MXE](what/mxe.md) ¿Se crean temas de Kafka?

Puede usar una utilidad como [kafkacat](https://github.com/edenhill/kafkacat) para enumerar todos los temas.
Puede ejecutar el siguiente comando para ver los temas de Kafka creados en su corredor de Kafka.

```bash
kafkacat -L -b localhost:9092
```

Confirme que `MetadataChangeEvent`, `MetadataAuditEvent`, `MetadataChangeProposal_v1` y `MetadataChangeLog_v1` existen temas además de los predeterminados.

## ¿Cómo puedo comprobar si los índices de búsqueda se crean en Elasticsearch?

Puede ejecutar el siguiente comando para ver los índices de búsqueda creados en su Elasticsearch.

```bash
curl http://localhost:9200/_cat/indices
```

Confirme que `datasetindex_v2` & `corpuserindex_v2` existen índices además de los predeterminados. Ejemplo de respuesta como se muestra a continuación:

```bash
yellow open dataset_datasetprofileaspect_v1         HnfYZgyvS9uPebEQDjA1jg 1 1   0  0   208b   208b
yellow open datajobindex_v2                         A561PfNsSFmSg1SiR0Y0qQ 1 1   2  9 34.1kb 34.1kb
yellow open mlmodelindex_v2                         WRJpdj2zT4ePLSAuEvFlyQ 1 1   1 12 24.2kb 24.2kb
yellow open dataflowindex_v2                        FusYIc1VQE-5NaF12uS8dA 1 1   1  3 23.3kb 23.3kb
yellow open mlmodelgroupindex_v2                    QOzAaVx7RJ2ovt-eC0hg1w 1 1   0  0   208b   208b
yellow open datahubpolicyindex_v2                   luXfXRlSRoS2-S_tvfLjHA 1 1   0  0   208b   208b
yellow open corpuserindex_v2                        gbNXtnIJTzqh3vHSZS0Fwg 1 1   2  2 18.4kb 18.4kb
yellow open dataprocessindex_v2                     9fL_4iCNTLyFv8MkDc6nIg 1 1   0  0   208b   208b
yellow open chartindex_v2                           wYKlG5ylQe2dVKHOaswTww 1 1   2  7 29.4kb 29.4kb
yellow open tagindex_v2                             GBQSZEvuRy62kpnh2cu1-w 1 1   2  2 19.7kb 19.7kb
yellow open mlmodeldeploymentindex_v2               UWA2ltxrSDyev7Tmu5OLmQ 1 1   0  0   208b   208b
yellow open dashboardindex_v2                       lUjGAVkRRbuwz2NOvMWfMg 1 1   1  0  9.4kb  9.4kb
yellow open .ds-datahub_usage_event-000001          Q6NZEv1UQ4asNHYRywxy3A 1 1  36  0 54.8kb 54.8kb
yellow open datasetindex_v2                         bWE3mN7IRy2Uj0QzeCt1KQ 1 1   7 47 93.7kb 93.7kb
yellow open mlfeatureindex_v2                       fvjML5xoQpy8oxPIwltm8A 1 1  20 39 59.3kb 59.3kb
yellow open dataplatformindex_v2                    GihumZfvRo27vt9yRpoE_w 1 1   0  0   208b   208b
yellow open glossarynodeindex_v2                    ABKeekWTQ2urPWfGDsS4NQ 1 1   1  1 18.1kb 18.1kb
yellow open graph_service_v1                        k6q7xV8OTIaRIkCjrzdufA 1 1 116 25 77.1kb 77.1kb
yellow open system_metadata_service_v1              9-FKAqp7TY2hs3RQuAtVMw 1 1 303  0 55.9kb 55.9kb
yellow open schemafieldindex_v2                     Mi_lqA-yQnKWSleKEXSWeg 1 1   0  0   208b   208b
yellow open mlfeaturetableindex_v2                  pk98zrSOQhGr5gPYUQwvvQ 1 1   5 14 36.4kb 36.4kb
yellow open glossarytermindex_v2                    NIyi3WWiT0SZr8PtECo0xQ 1 1   3  8 23.1kb 23.1kb
yellow open mlprimarykeyindex_v2                    R1WFxD9sQiapIZcXnDtqMA 1 1   7  6 35.5kb 35.5kb
yellow open corpgroupindex_v2                       AYxVtFAEQ02BsJdahYYvlA 1 1   2  1 13.3kb 13.3kb
yellow open dataset_datasetusagestatisticsaspect_v1 WqPpDCKZRLaMIcYAAkS_1Q 1 1   0  0   208b   208b
```

## ¿Cómo puedo comprobar si los datos se han cargado correctamente en MySQL?

Una vez que el contenedor mysql esté en funcionamiento, debería poder conectarse a él directamente en `localhost:3306` uso de herramientas como [Banco de trabajo MySQL](https://www.mysql.com/products/workbench/). También puede ejecutar el siguiente comando para invocar [Cliente de línea de comandos MySQL](https://dev.mysql.com/doc/refman/8.0/en/mysql.html) dentro del contenedor mysql.

    docker exec -it mysql /usr/bin/mysql datahub --user=datahub --password=datahub

Inspeccionar el contenido de `metadata_aspect_v2` , que contiene los aspectos ingeridos para todas las entidades.

## Error al iniciar contenedores de Docker

Puede haber diferentes razones por las que un contenedor falla durante la inicialización. A continuación se presentan las razones más comunes:

### `bind: address already in use`

Este error significa que el puerto de red (que se supone que debe ser utilizado por el contenedor fallido) ya está en uso en su sistema. Debe encontrar y eliminar el proceso que está utilizando este puerto específico antes de iniciar el contenedor Docker correspondiente. Si no desea eliminar el proceso que está utilizando ese puerto, otra opción es cambiar el número de puerto para el contenedor docker. Necesitas encontrar y cambiar el [Puertos](https://docs.docker.com/compose/compose-file/#ports) para el contenedor de Docker específico en el `docker-compose.yml` archivo de configuración.

    Example : On macOS

    ERROR: for mysql  Cannot start service mysql: driver failed programming external connectivity on endpoint mysql (5abc99513affe527299514cea433503c6ead9e2423eeb09f127f87e2045db2ca): Error starting userland proxy: listen tcp 0.0.0.0:3306: bind: address already in use

       1) sudo lsof -i :3306
       2) kill -15 <PID found in step1>

### `OCI runtime create failed`

Si ve un mensaje de error como el siguiente, asegúrese de actualizar su repositorio local a HEAD.

    ERROR: for datahub-mae-consumer  Cannot start service datahub-mae-consumer: OCI runtime create failed: container_linux.go:349: starting container process caused "exec: \"bash\": executable file not found in $PATH": unknown

### `failed to register layer: devmapper: Unknown device`

Esto significa que no tiene espacio en disco (consulte [#1879](https://github.com/datahub-project/datahub/issues/1879)).

### `ERROR: for kafka-rest-proxy  Get https://registry-1.docker.io/v2/confluentinc/cp-kafka-rest/manifests/5.4.0: EOF`

Lo más probable es que se trate de un problema transitorio con [Registro de Docker](https://docs.docker.com/registry/). Vuelva a intentarlo más tarde.

## toomanyrequests: demasiados intentos fallidos de inicio de sesión para el nombre de usuario o la dirección IP

Pruebe lo siguiente

```bash
rm ~/.docker/config.json
docker login
```

Más discusiones sobre el mismo tema https://github.com/docker/hub-feedback/issues/1250

## Vista `Table 'datahub.metadata_aspect' doesn't exist` Error al iniciar sesión

Esto significa que la base de datos no se inicializó correctamente como parte de los procesos de inicio rápido (consulte [#1816](https://github.com/datahub-project/datahub/issues/1816)). Ejecute el siguiente comando para inicializarlo manualmente.

    docker exec -i mysql sh -c 'exec mysql datahub -udatahub -pdatahub' < docker/mysql/init.sql

## He estropeado mi configuración de docker. ¿Cómo empiezo desde cero?

Ejecute el siguiente script para quitar todos los contenedores y volúmenes creados durante el tutorial de inicio rápido. Tenga en cuenta que también perderá todos los datos como resultado.

    ./docker/nuke.sh

## Estoy viendo excepciones en el contenedor GMS de DataHub como "Causado por: java.lang.IllegalStateException: Duplicate key com.linkedin.metadata.entity.ebean.EbeanAspectV2@dd26e011". ¿Qué hago?

Esto está relacionado con un problema de intercalación de columnas SQL. La intercalación predeterminada que usamos anteriormente (antes del 26 de octubre de 2021) para los campos URN no distinguía entre mayúsculas y minúsculas (utf8mb4\_unicode_ci). Nos hemos mudado recientemente
para implementar con una intercalación que distingue entre mayúsculas y minúsculas (utf8mb4\_bin) de forma predeterminada. Para actualizar una implementación que se inició antes del 26 de octubre de 2021 (v0.8.16 e inferior) para que tenga la nueva intercalación, debe ejecutar este comando directamente en la base de datos SQL:

    ALTER TABLE metadata_aspect_v2 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

## He modificado el archivo user.props predeterminado para incluir un nombre de usuario y una contraseña personalizados, pero no veo los nuevos usuarios dentro de la pestaña Usuarios y grupos. ¿Por qué no?

Actualmente `user.props` es un archivo utilizado por JAAS PropertyFileLoginModule únicamente con el propósito de **Autenticación**. El archivo no se utiliza como fuente desde la que
ingerir metadatos adicionales sobre el usuario. Para eso, deberá ingerir información personalizada sobre su nuevo usuario utilizando las API de Rest.li o el [Origen de ingesta basado en archivos](./generated/ingestion/sources/file.md).

Para ver un ejemplo de un archivo que ingiere información del usuario, desproteja [single_mce.json](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/mce_files/single_mce.json), que ingiere un único objeto de usuario en DataHub. Observe que el campo "urna" proporcionado
deberá alinearse con el nombre de usuario personalizado que ha proporcionado en el archivo user.props. Por ejemplo, si el archivo user.props contiene:

    my-custom-user:my-custom-password

Deberá ingerir algunos metadatos del siguiente formulario para verlos dentro de la interfaz de usuario de DataHub:

    {
      "auditHeader": null,
      "proposedSnapshot": {
        "com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot": {
          "urn": "urn:li:corpuser:my-custom-user",
          "aspects": [
            {
              "com.linkedin.pegasus2avro.identity.CorpUserInfo": {
                "active": true,
                "displayName": {
                  "string": "The name of the custom user"
                },
                "email": "my-custom-user-email@example.io",
                "title": {
                  "string": "Engineer"
                },
                "managerUrn": null,
                "departmentId": null,
                "departmentName": null,
                "firstName": null,
                "lastName": null,
                "fullName": {
                  "string": "My Custom User"
                },
                "countryCode": null
              }
            }
          ]
        }
      },
      "proposedDelta": null
    }
