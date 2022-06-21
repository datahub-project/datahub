# DataHub CLI

DataHub viene con una cli amigable llamada `datahub` que le permite realizar muchas operaciones comunes utilizando solo la línea de comandos.

## Notas de la versión y versiones de la CLI

Las notas de la versión del servidor se pueden encontrar en [Versiones de github](https://github.com/datahub-project/datahub/releases). Estas versiones se realizan aproximadamente cada semana en una cadencia regular a menos que se descubra un problema de bloqueo o regresión.

La versión de cli se realiza a través de un repositorio diferente y las notas de la versión se pueden encontrar en [Versiones de acryldata](https://github.com/acryldata/datahub/releases). Al menos una versión que está vinculada a la versión del servidor siempre se realiza junto con la versión del servidor. Se realizan varias otras versiones de bigfix en función de la cantidad de correcciones que se combinan entre la versión del servidor mencionada anteriormente.

Si el servidor con versión `0.8.28` se está utilizando, entonces la CLI utilizada para conectarse a ella debe ser `0.8.28.x`. Las pruebas de la nueva CLI no se ejecutan con versiones de servidor anteriores, por lo que no se recomienda actualizar la CLI si el servidor no se actualiza.

## Instalación

### Uso de pip

Recomendamos entornos virtuales python (venv-s) para los módulos pip de espacio de nombres. La gente de más de [Datos de Acryl](https://www.acryl.io/) mantener un paquete PyPI para la ingesta de metadatos de DataHub. Aquí hay un ejemplo de configuración:

```shell
python3 -m venv datahub-env             # create the environment
source datahub-env/bin/activate         # activate the environment
```

***NOTA:*** Si instala `datahub` en un entorno virtual, ese mismo entorno virtual debe reactivarse cada vez que se crea una ventana o sesión de shell.

Una vez dentro del entorno virtual, instalar `datahub` Uso de los siguientes comandos

```shell
# Requires Python 3.6+
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
```

Si se encuentra con un error, intente comprobar el [*problemas comunes de configuración*](../metadata-ingestion/developing.md#Common-setup-issues).

### Uso de docker

[![Docker Hub](https://img.shields.io/docker/pulls/linkedin/datahub-ingestion?style=plastic)](https://hub.docker.com/r/linkedin/datahub-ingestion)
[![datahub-ingestion docker](https://github.com/datahub-project/datahub/actions/workflows/docker-ingestion.yml/badge.svg)](https://github.com/datahub-project/datahub/actions/workflows/docker-ingestion.yml)

Si no desea instalar localmente, puede ejecutar alternativamente la ingesta de metadatos dentro de un contenedor de Docker.
Tenemos imágenes prediseñadas disponibles en [Concentrador de Docker](https://hub.docker.com/r/linkedin/datahub-ingestion). Todos los plugins se instalarán y habilitarán automáticamente.

Puede utilizar el `datahub-ingestion` Imagen de Docker como se explica en [Imágenes de Docker](../docker/README.md). En caso de que esté utilizando Kubernetes, puede iniciar un pod con el `datahub-ingestion` docker image, inicie sesión en un shell en el pod y debería tener acceso a la CLI de datahub en su clúster de kubernetes.

*Limitación: el script de conveniencia datahub_docker.sh asume que la receta y cualquier archivo de entrada/salida son accesibles en el directorio de trabajo actual o sus subdirectorios. No se encontrarán archivos fuera del directorio de trabajo actual y deberá invocar la imagen de Docker directamente.*

```shell
# Assumes the DataHub repo is cloned locally.
./metadata-ingestion/scripts/datahub_docker.sh ingest -c ./examples/recipes/example_to_datahub_rest.yml
```

### Instalar desde el origen

Si desea instalar desde el origen, consulte el [guía para desarrolladores](../metadata-ingestion/developing.md).

## Instalación de plugins

Utilizamos una arquitectura de complementos para que pueda instalar solo las dependencias que realmente necesita. ¡Haga clic en el nombre del complemento para obtener más información sobre la receta de origen específica y cualquier pregunta frecuente!

### Fuentes

| | del nombre del plugin Instalar | de comandos Proporciona |
|-------------------------------------------------------------------------------------|------------------------------------------------------------| ----------------------------------- |
| [archivo](./generated/ingestion/sources/file.md)                                   | *incluido de forma predeterminada*                                      | Origen de archivos y | de receptor
| [Atenea](./generated/ingestion/sources/athena.md)                               | `pip install 'acryl-datahub[athena]'`                      | | de origen de AWS Athena
| [bigquery](./generated/ingestion/sources/bigquery.md)                           | `pip install 'acryl-datahub[bigquery]'`                    | | de origen de BigQuery
| [bigquery-uso](./generated/ingestion/sources/bigquery.md#module-bigquery-usage)                     | `pip install 'acryl-datahub[bigquery-usage]'`              | Fuente de estadísticas de uso de BigQuery |
| [datahub-lineage-file](./generated/ingestion/sources/file-based-lineage.md)           | *sin dependencias adicionales*                               | | de origen del archivo de linaje
| [datahub-business-glossary](./generated/ingestion/sources/business-glossary.md) | *sin dependencias adicionales*                               | | de origen del archivo del glosario empresarial
| [dbt](./generated/ingestion/sources/dbt.md)                                     | *sin dependencias adicionales*                               | | de origen dbt
| [druida](./generated/ingestion/sources/druid.md)                                 | `pip install 'acryl-datahub[druid]'`                       | fuente druida |
| [fiesta-legado](./generated/ingestion/sources/feast.md#module-feast-legacy)                   | `pip install 'acryl-datahub[feast-legacy]'`                | Fuente de la fiesta (legado) |
| [Banquete](./generated/ingestion/sources/feast.md)                                 | `pip install 'acryl-datahub[feast]'`                       | Fuente de la fiesta (0.18.0) |
| [cola](./generated/ingestion/sources/glue.md)                                   | `pip install 'acryl-datahub[glue]'`                        | | de origen de AWS Glue
| [Hana](./generated/ingestion/sources/hana.md)                                   | `pip install 'acryl-datahub[hana]'`                        | | de origen de SAP HANA
| [colmena](./generated/ingestion/sources/hive.md)                                   | `pip install 'acryl-datahub[hive]'`                        | | de origen de colmena
| [Kafka](./generated/ingestion/sources/kafka.md)                                 | `pip install 'acryl-datahub[kafka]'`                       | Fuente Kafka |
| [kafka-connect](./generated/ingestion/sources/kafka-connect.md)                 | `pip install 'acryl-datahub[kafka-connect]'`               | | de origen de conexión kafka
| [ldap](./generated/ingestion/sources/ldap.md)                                   | `pip install 'acryl-datahub[ldap]'` ([requisitos adicionales][extra requirements]) | | de origen LDAP
| [guapa](./generated/ingestion/sources/looker.md)                               | `pip install 'acryl-datahub[looker]'`                      | | de origen de Looker
| [lookml](./generated/ingestion/sources/looker.md#module-lookml)                               | `pip install 'acryl-datahub[lookml]'`                      | Fuente LookML, requiere Python 3.7+ |
| [metabase](./generated/ingestion/sources/metabase.md)                           | `pip install 'acryl-datahub[metabase]'`                    | | de origen de la metabase
| [modo](./generated/ingestion/sources/mode.md)                                   | `pip install 'acryl-datahub[mode]'`                        | | de origen de Mode Analytics
| [mongodb](./generated/ingestion/sources/mongodb.md)                             | `pip install 'acryl-datahub[mongodb]'`                     | | de origen de MongoDB
| [mssql](./generated/ingestion/sources/mssql.md)                                 | `pip install 'acryl-datahub[mssql]'`                       | | de origen de SQL Server
| [mysql](./generated/ingestion/sources/mysql.md)                                 | `pip install 'acryl-datahub[mysql]'`                       | | de origen MySQL
| [mariadb](./generated/ingestion/sources/mariadb.md)                             | `pip install 'acryl-datahub[mariadb]'`                     | | de origen MariaDB
| [openapi](./generated/ingestion/sources/openapi.md)                             | `pip install 'acryl-datahub[openapi]'`                     | | de código abierto
| [oráculo](./generated/ingestion/sources/oracle.md)                               | `pip install 'acryl-datahub[oracle]'`                      | | de origen de Oracle
| [postgres](./generated/ingestion/sources/postgres.md)                           | `pip install 'acryl-datahub[postgres]'`                    | | de origen de Postgres
| [redash](./generated/ingestion/sources/redash.md)                               | `pip install 'acryl-datahub[redash]'`                      | | de origen de Redash
| [corrimiento al rojo](./generated/ingestion/sources/redshift.md)                           | `pip install 'acryl-datahub[redshift]'`                    | | de origen de corrimiento al rojo
| [sagemaker](./generated/ingestion/sources/sagemaker.md)                         | `pip install 'acryl-datahub[sagemaker]'`                   | | de origen de AWS SageMaker
| [copo de nieve](./generated/ingestion/sources/snowflake.md)                         | `pip install 'acryl-datahub[snowflake]'`                   | | de origen de copos de nieve
| [uso de copos de nieve](./generated/ingestion/sources/snowflake.md#module-snowflake-usage)                   | `pip install 'acryl-datahub[snowflake-usage]'`             | Fuente de estadísticas de uso de copos de nieve |
| [sqlalchemy](./generated/ingestion/sources/sqlalchemy.md)                       | `pip install 'acryl-datahub[sqlalchemy]'`                  | | de origen SQLAlchemy genérico
| [superconjunto](./generated/ingestion/sources/superset.md)                           | `pip install 'acryl-datahub[superset]'`                    | | de origen superconjunto
| [cuadro](./generated/ingestion/sources/tableau.md)                             | `pip install 'acryl-datahub[tableau]'`                     | | de origen de Tableau
| [trino](./generated/ingestion/sources/trino.md)                                 | `pip install 'acryl-datahub[trino]'`                       | | de origen Trino
| [starburst-trino-usage](./generated/ingestion/sources/trino.md)                 | `pip install 'acryl-datahub[starburst-trino-usage]'`       | Fuente de estadísticas de uso de Starburst Trino |
| [nifi](./generated/ingestion/sources/nifi.md)                                   | `pip install 'acryl-datahub[nifi]'`                        | | de origen Nifi
| [powerbi](./generated/ingestion/sources/powerbi.md)                             | `pip install 'acryl-datahub[powerbi]'`                     | | de origen de Microsoft Power BI

### Fregaderos

| | del nombre del plugin Instalar | de comandos Proporciona |
| --------------------------------------- | -------------------------------------------- | -------------------------- |
| [archivo](../metadata-ingestion/sink_docs/file.md)             | *incluido de forma predeterminada*                        | Origen de archivos y | de receptor
| [consola](../metadata-ingestion/sink_docs/console.md)       | *incluido de forma predeterminada*                        | | del receptor de la consola
| [datahub-rest](../metadata-ingestion/sink_docs/datahub.md)  | `pip install 'acryl-datahub[datahub-rest]'`  | Receptor de DataHub sobre | de API de REST
| [datahub-kafka](../metadata-ingestion/sink_docs/datahub.md) | `pip install 'acryl-datahub[datahub-kafka]'` | El sumidero de DataHub sobre kafka |

Estos plugins se pueden mezclar y combinar como se desee. Por ejemplo:

```shell
pip install 'acryl-datahub[bigquery,datahub-rest]'
```

### Comprueba los plugins activos

```shell
datahub check plugins
```

[extra requirements]: https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites

## Variables de entorno admitidas

Las variables env tienen prioridad sobre lo que está en la configuración de la CLI de DataHub creada a través de `init` mandar. La lista de variables de entorno admitidas es la siguiente

*   `DATAHUB_SKIP_CONFIG` (por defecto `false`) - Establecer en `true` para omitir la creación del archivo de configuración.
*   `DATAHUB_GMS_URL` (por defecto `http://localhost:8080`) - Establecido en una URL de la instancia gmsmal
*   `DATAHUB_GMS_HOST` (por defecto `localhost`) - Establecido en un host de instancia GMS. Prefiere usar `DATAHUB_GMS_URL` para establecer la dirección URL.
*   `DATAHUB_GMS_PORT` (por defecto `8080`) - Establecido en un puerto de instancia GMS. Prefiere usar `DATAHUB_GMS_URL` para establecer la dirección URL.
*   `DATAHUB_GMS_PROTOCOL` (por defecto `http`) - Establecer en un protocolo como `http` o `https`. Prefiere usar `DATAHUB_GMS_URL` para establecer la dirección URL.
*   `DATAHUB_GMS_TOKEN` (por defecto `None`) - Se utiliza para comunicarse con DataHub Cloud.
*   `DATAHUB_TELEMETRY_ENABLED` (por defecto `true`) - Establecer en `false` para deshabilitar la telemetría. Si la CLI se ejecuta en un entorno sin acceso a Internet público, esto debe deshabilitarse.
*   `DATAHUB_TELEMETRY_TIMEOUT` (por defecto `10`) - Se establece en un valor entero personalizado para especificar el tiempo de espera en segundos al enviar telemetría.
*   `DATAHUB_DEBUG` (por defecto `false`) - Establecer en `true` para habilitar el registro de depuración para la CLI. También se puede lograr a través de `--debug` opción de la CLI.
*   `DATAHUB_VERSION` (por defecto `head`) - Establecido en una versión específica para ejecutar el inicio rápido con la versión particular de las imágenes de Docker.
*   `ACTIONS_VERSION` (por defecto `head`) - Establecido en una versión específica para ejecutar el inicio rápido con esa etiqueta de imagen de `datahub-actions` contenedor.

```shell
DATAHUB_SKIP_CONFIG=false
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_GMS_TOKEN=
DATAHUB_TELEMETRY_ENABLED=true
DATAHUB_TELEMETRY_TIMEOUT=10
DATAHUB_DEBUG=false
```

## Guía del usuario

El `datahub` cli le permite hacer muchas cosas, como iniciar rápidamente una instancia de Docker de DataHub localmente, ingerir metadatos de sus orígenes, así como recuperar y modificar metadatos.
Como la mayoría de las herramientas de línea de comandos, `--help` es tu mejor amigo. Úselo para descubrir las capacidades de la cli y los diferentes comandos y subcomandos que se admiten.

```console
Usage: datahub [OPTIONS] COMMAND [ARGS]...

Options:
  --debug / --no-debug
  --version             Show the version and exit.
  --help                Show this message and exit.

Commands:
  check      Helper commands for checking various aspects of DataHub.
  delete     Delete metadata from datahub using a single urn or a combination of filters
  docker     Helper commands for setting up and interacting with a local DataHub instance using Docker.
  get        Get metadata for an entity with an optional list of aspects to project.
  ingest     Ingest metadata into DataHub.
  init       Configure which datahub instance to connect to
  put        Update a single aspect of an entity
  telemetry  Toggle telemetry.
  version    Print version number and exit.
```

Los siguientes comandos de nivel superior que se enumeran a continuación están aquí principalmente para dar al lector una imagen de alto nivel de cuáles son los tipos de cosas que puede lograr con el cli.
Los hemos ordenado aproximadamente en el orden en que esperamos que interactúe con estos comandos a medida que profundiza en el `datahub`-verso.

### estibador

El `docker` le permite iniciar una instancia local de DataHub mediante `datahub docker quickstart`. También puede comprobar si el clúster de Docker está en buen estado mediante `datahub docker check`.

### ingerir

El `ingest` le permite ingerir metadatos de sus fuentes utilizando archivos de configuración de ingesta, que llamamos recetas. [Eliminación de metadatos de DataHub](./how/delete-metadata.md) contiene instrucciones detalladas acerca de cómo puede utilizar el comando ingest para realizar operaciones como la reversión de metadatos previamente ingeridos a través del comando `rollback` subcomando y lista de todas las ejecuciones que se produjeron a través de `list-runs` subcomando.

```console
Usage: datahub [datahub-options] ingest [command-options]

Command Options:
  -c / --config        Config file in .toml or .yaml format
  -n / --dry-run       Perform a dry run of the ingestion, essentially skipping writing to sink
  --preview            Perform limited ingestion from the source to the sink to get a quick preview
  --preview-workunits  The number of workunits to produce for preview
  --strict-warnings    If enabled, ingestion runs with warnings will yield a non-zero error code
```

### comprobar

El paquete datahub se compone de diferentes complementos que le permiten conectarse a diferentes fuentes de metadatos e ingerir metadatos de ellos.
El `check` le permite verificar si todos los complementos se cargan correctamente, así como validar un archivo MCE individual.

### Init

El comando init se utiliza para decir `datahub` sobre dónde se encuentra la instancia de DataHub. La CLI apuntará a localhost DataHub de forma predeterminada.
Corriente `datahub init` le permitirá personalizar la instancia de datahub con la que se está comunicando.

***Nota***: Proporcione el host de su instancia de GMS cuando el mensaje le pida el host de DataHub.

### telemetría

Para ayudarnos a comprender cómo las personas usan DataHub, recopilamos estadísticas de uso anónimas sobre acciones como invocaciones de comandos a través de Mixpanel.
No recopilamos información privada como direcciones IP, contenido de ingestas o credenciales.
El código responsable de recopilar y transmitir estos eventos es de código abierto y se puede encontrar [dentro de nuestro GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/telemetry/telemetry.py).

La telemetría está habilitada de forma predeterminada y el `telemetry` Le permite alternar el envío de estas estadísticas a través de `telemetry enable/disable`.

### borrar

El `delete` le permite eliminar metadatos de DataHub. Lea esto [guiar](./how/delete-metadata.md) para comprender cómo puede eliminar metadatos de DataHub.

```console
datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --soft
```

### Obtener

El `get` le permite recuperar fácilmente metadatos de DataHub, mediante la API de REST. Esto funciona tanto para aspectos versionados como para aspectos de series temporales. Para los aspectos de series temporales, obtiene el valor más reciente.
Por ejemplo, el siguiente comando obtiene el aspecto de propiedad del conjunto de datos `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`

```console
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership | jq                                                                       put_command
{
  "value": {
    "com.linkedin.metadata.snapshot.DatasetSnapshot": {
      "aspects": [
        {
          "com.linkedin.metadata.key.DatasetKey": {
            "name": "SampleHiveDataset",
            "origin": "PROD",
            "platform": "urn:li:dataPlatform:hive"
          }
        },
        {
          "com.linkedin.common.Ownership": {
            "lastModified": {
              "actor": "urn:li:corpuser:jdoe",
              "time": 1581407189000
            },
            "owners": [
              {
                "owner": "urn:li:corpuser:jdoe",
                "type": "DATAOWNER"
              },
              {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER"
              }
            ]
          }
        }
      ],
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
    }
  }
}
```

### poner

El `put` le permite escribir metadatos en DataHub. Esta es una forma flexible de emitir ediciones de metadatos desde la línea de comandos.
Por ejemplo, el siguiente comando indica `datahub` Para establecer el `ownership` aspecto del conjunto de datos `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)` al valor del archivo `ownership.json`.
El JSON en el `ownership.json` el archivo debe ajustarse a la [`Ownership`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl) Modelo de aspecto como se muestra a continuación.

```json
{
  "owners": [
    {
      "owner": "urn:li:corpuser:jdoe",
      "type": "DEVELOPER"
    },
    {
      "owner": "urn:li:corpuser:jdub",
      "type": "DATAOWNER"
    }
  ]
}
```

```console
datahub --debug put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership -d ownership.json

[DATE_TIMESTAMP] DEBUG    {datahub.cli.cli_utils:340} - Attempting to emit to DataHub GMS; using curl equivalent to:
curl -X POST -H 'User-Agent: python-requests/2.26.0' -H 'Accept-Encoding: gzip, deflate' -H 'Accept: */*' -H 'Connection: keep-alive' -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'Content-Type: application/json' --data '{"proposal": {"entityType": "dataset", "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)", "aspectName": "ownership", "changeType": "UPSERT", "aspect": {"contentType": "application/json", "value": "{\"owners\": [{\"owner\": \"urn:li:corpuser:jdoe\", \"type\": \"DEVELOPER\"}, {\"owner\": \"urn:li:corpuser:jdub\", \"type\": \"DATAOWNER\"}]}"}}}' 'http://localhost:8080/aspects/?action=ingestProposal'
Update succeeded with status 200
```

### migrar

El `migrate` grupo de comandos le permite realizar ciertos tipos de migraciones.

#### dataplatform2instance

El `dataplatform2instance` El comando migration le permite migrar las entidades de un identificador de plataforma independiente de la instancia a un identificador de plataforma específico de la instancia. Si ha ingerido metadatos en el pasado para esta plataforma y desea transferir cualquier metadato importante a las nuevas entidades específicas de la instancia, debe usar este comando. Por ejemplo, si los usuarios han agregado documentación o agregado etiquetas o términos a los conjuntos de datos, debe ejecutar este comando para transferir estos metadatos a las nuevas entidades. Para obtener más contexto, lea la Guía de instancias de plataforma [aquí](./platform-instances.md).

Algunas opciones importantes que vale la pena llamar:

*   \--dry-run / -n : Utilice esto para obtener un informe de lo que se migrará antes de ejecutar
*   \--force / -F : Use esto si sabe lo que está haciendo y no desea recibir un mensaje de confirmación antes de iniciar la migración
*   \--keep : Cuando esté habilitado, conservará las entidades antiguas y no las eliminará. El comportamiento predeterminado es eliminar por software las entidades antiguas.
*   \--hard : Cuando esté habilitado, eliminará las entidades antiguas.

***Nota***: Los aspectos de las series temporales, como las estadísticas de uso y los perfiles de conjunto de datos, no se migran a las nuevas instancias de entidad, obtendrá nuevos puntos de datos creados cuando vuelva a ejecutar la ingesta mediante el comando `usage` o fuentes con la generación de perfiles activada.

##### Carrera en seco

```console
datahub migrate dataplatform2instance --platform elasticsearch --instance prod_index --dry-run
Starting migration: platform:elasticsearch, instance=prod_index, force=False, dry-run=True
100% (25 of 25) |####################################################################################################################################################################################| Elapsed Time: 0:00:00 Time:  0:00:00
[Dry Run] Migration Report:
--------------
[Dry Run] Migration Run Id: migrate-5710349c-1ec7-4b83-a7d3-47d71b7e972e
[Dry Run] Num entities created = 25
[Dry Run] Num entities affected = 0
[Dry Run] Num entities migrated = 25
[Dry Run] Details:
[Dry Run] New Entities Created: {'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahubretentionindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.schemafieldindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.system_metadata_service_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.tagindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataset_datasetprofileaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlmodelindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlfeaturetableindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datajob_datahubingestioncheckpointaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahub_usage_event,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataset_operationaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datajobindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataprocessindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.glossarytermindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataplatformindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlmodeldeploymentindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datajob_datahubingestionrunsummaryaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.graph_service_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahubpolicyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataset_datasetusagestatisticsaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dashboardindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.glossarynodeindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlfeatureindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataflowindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlprimarykeyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.chartindex_v2,PROD)'}
[Dry Run] External Entities Affected: None
[Dry Run] Old Entities Migrated = {'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataset_datasetusagestatisticsaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlmodelindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlmodeldeploymentindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datajob_datahubingestionrunsummaryaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datahubretentionindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datahubpolicyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataset_datasetprofileaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,glossarynodeindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataset_operationaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,graph_service_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datajobindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlprimarykeyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dashboardindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datajob_datahubingestioncheckpointaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,tagindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datahub_usage_event,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,schemafieldindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlfeatureindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataprocessindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataplatformindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlfeaturetableindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,glossarytermindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataflowindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,chartindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,system_metadata_service_v1,PROD)'}
```

##### Migración real (con soft-delete)

    > datahub migrate dataplatform2instance --platform hive --instance
    datahub migrate dataplatform2instance --platform hive --instance warehouse
    Starting migration: platform:hive, instance=warehouse, force=False, dry-run=False
    Will migrate 4 urns such as ['urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)']
    New urns will look like ['urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.logging_events,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_created,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_deleted,PROD)']

    Ok to proceed? [y/N]:
    ...
    Migration Report:
    --------------
    Migration Run Id: migrate-f5ae7201-4548-4bee-aed4-35758bb78c89
    Num entities created = 4
    Num entities affected = 0
    Num entities migrated = 4
    Details:
    New Entities Created: {'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_deleted,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.logging_events,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_created,PROD)'}
    External Entities Affected: None
    Old Entities Migrated = {'urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)'}

### línea de tiempo

El `timeline` permite ver un historial de versiones de las entidades. Actualmente solo se admite para conjuntos de datos. Por ejemplo
el siguiente comando le mostrará las modificaciones en las etiquetas de un conjunto de datos de la semana pasada. La salida incluye una versión semántica calculada,
relevante para los cambios de esquema solo actualmente, el destino de la modificación y una descripción del cambio que incluya una marca de tiempo.
La salida predeterminada se desinfecta para que sea más legible, pero la salida completa de la API se puede obtener pasando el `--verbose` bandera y
Para obtener la diferencia JSON sin procesar además de la salida de la API, puede agregar el `--raw` bandera. Para obtener más detalles sobre la función, consulte [la página principal de características](dev-guides/timeline.md)

```console
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)" --category TAG --start 7daysago
2022-02-17 14:03:42 - 0.0.0-computed
	MODIFY TAG dataset:mysql:User.UserAccount : A change in aspect editableSchemaMetadata happened at time 2022-02-17 20:03:42.0
2022-02-17 14:17:30 - 0.0.0-computed
	MODIFY TAG dataset:mysql:User.UserAccount : A change in aspect editableSchemaMetadata happened at time 2022-02-17 20:17:30.118
```
