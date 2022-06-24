# ⚡ Marco de acciones de DataHub

¡Bienvenido a DataHub Actions! El marco de acciones facilita la respuesta a los cambios en tiempo real en su gráfico de metadatos, lo que le permite integrarse sin problemas [Centro de datos](https://github.com/datahub-project/datahub) en una arquitectura más amplia basada en eventos.

Para una introducción detallada, echa un vistazo a la [anuncio original](https://www.youtube.com/watch?v=7iwNxHgqxtg\&t=2189s) del Marco de Acciones de DataHub en el Ayuntamiento de DataHub de abril de 2022. Para una mirada más profunda a los casos de uso y conceptos, echa un vistazo a [Conceptos de acciones de DataHub](concepts.md).

## Inicio rápido

Para comenzar de inmediato, echa un vistazo a la [Inicio rápido de acciones de DataHub](quickstart.md) Guiar.

## Prerrequisitos

Los comandos de la CLI de DataHub Actions son una extensión de la base `datahub` Comandos de la CLI. Recomendamos
Primero instalando el `datahub` CLI:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub --version
```

> Tenga en cuenta que el Marco de acciones requiere una versión de `acryl-datahub` >= v0.8.34

## Instalación

A continuación, simplemente instale el `acryl-datahub-actions` paquete de PyPi:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions
datahub actions version
```

## Configuración de una acción

Las acciones se configuran utilizando un archivo YAML, de la misma manera que las fuentes de ingesta de DataHub. Un archivo de configuración de acciones consta de lo siguiente:

1.  Nombre de la canalización de acciones (debe ser único y estático)
2.  Configuraciones de origen
3.  Configuraciones de transformación + filtro
4.  Configuración de acciones
5.  Opciones de canalización (opcional)
6.  Configuraciones de la API de DataHub (opcional- necesaria para acciones seleccionadas)

Con cada componente siendo conectable y configurable de forma independiente.

```yml
# 1. Required: Action Pipeline Name
name: <action-pipeline-name>

# 2. Required: Event Source - Where to source event from.
source:
  type: <source-type>
  config:
    # Event Source specific configs (map)

# 3a. Optional: Filter to run on events (map)
filter: 
  event_type: <filtered-event-type>
  event:
    # Filter event fields by exact-match
    <filtered-event-fields>

# 3b. Optional: Custom Transformers to run on events (array)
transform:
  - type: <transformer-type>
    config: 
      # Transformer-specific configs (map)

# 4. Required: Action - What action to take on events. 
action:
  type: <action-type>
  config:
    # Action-specific configs (map)

# 5. Optional: Additional pipeline options (error handling, etc)
options: 
  retry_count: 0 # The number of times to retry an Action with the same event. (If an exception is thrown). 0 by default. 
  failure_mode: "CONTINUE" # What to do when an event fails to be processed. Either 'CONTINUE' to make progress or 'THROW' to stop the pipeline. Either way, the failed event will be logged to a failed_events.log file. 
  failed_events_dir: "/tmp/datahub/actions"  # The directory in which to write a failed_events.log file that tracks events which fail to be processed. Defaults to "/tmp/logs/datahub/actions". 

# 6. Optional: DataHub API configuration
datahub:
  server: "http://localhost:8080" # Location of DataHub API
  # token: <your-access-token> # Required if Metadata Service Auth enabled
```

### Ejemplo: Hello World

Un archivo de configuración simple para una acción "Hello World", que simplemente imprime todos los eventos que recibe, es

```yml
# 1. Action Pipeline Name
name: "hello_world"
# 2. Event Source: Where to source event from.
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
# 3. Action: What action to take on events. 
action:
  type: "hello_world"
```

Podemos modificar aún más esta configuración para filtrar eventos específicos, agregando un bloque de "filtro".

```yml
# 1. Action Pipeline Name
name: "hello_world"

# 2. Event Source - Where to source event from.
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}

# 3. Filter - Filter events that reach the Action
filter:
  event_type: "EntityChangeEvent_v1"
  event:
    category: "TAG"
    operation: "ADD"
    modifier: "urn:li:tag:pii"

# 4. Action - What action to take on events. 
action:
  type: "hello_world"
```

## Ejecución de una acción

Para ejecutar una nueva acción, simplemente use el botón `actions` Comando CLI

    datahub actions -c <config.yml>

Una vez que la acción se esté ejecutando, verá

    Action Pipeline with name '<action-pipeline-name>' is now running.

### Ejecución de varias acciones

Puede ejecutar varias canalizaciones de acciones dentro del mismo comando. Simplemente proporcione múltiples
config archivos restando el argumento de línea de comandos "-c".

Por ejemplo

    datahub actions -c <config-1.yaml> -c <config-2.yaml>

### Ejecución en modo de depuración

Simplemente agregue el `--debug` a la CLI para ejecutar la acción en modo de depuración.

    datahub actions -c <config.yaml> --debug

### Detener una acción

Simplemente emita un Control-C como de costumbre. Debería ver la canalización de acciones cerrada correctamente, con un pequeño
resumen de los resultados del procesamiento.

    Actions Pipeline with name '<action-pipeline-name' has been stopped.

## Eventos admitidos

Actualmente se admiten dos tipos de eventos. Lea más sobre ellos a continuación.

*   [Evento de cambio de entidad V1](events/entity-change-event.md)
*   [Registro de cambios de metadatos V1](events/metadata-change-log-event.md)

## Orígenes de eventos admitidos

Actualmente, el único origen de eventos que se admite oficialmente es `kafka`, que sondea para eventos
a través de un consumidor de Kafka.

*   [Origen del evento Kafka](sources/kafka-event-source.md)

## Acciones admitidas

De forma predeterminada, DataHub admite un conjunto de complementos de acciones estándar. Estos se pueden encontrar dentro de la carpeta
`src/datahub-actions/plugins`.

Algunas acciones preincluidas incluyen

*   [Hola mundo](actions/hello_world.md)
*   [Ejecutor](actions/executor.md)

## Desarrollo

### Compilación y prueba

Observe que admitimos todos los comandos de acciones utilizando un comando independiente `datahub-actions` Punto de entrada de la CLI. Siéntete libre
para usar esto durante el desarrollo.

    # Build datahub-actions module
    ./gradlew datahub-actions:build

    # Drop into virtual env
    cd datahub-actions && source venv/bin/activate 

    # Start hello world action 
    datahub-actions actions -c ../examples/hello_world.yaml

    # Start ingestion executor action
    datahub-actions actions -c ../examples/executor.yaml

    # Start multiple actions 
    datahub-actions actions -c ../examples/executor.yaml -c ../examples/hello_world.yaml

### Desarrollo de un transformador

Para desarrollar un nuevo Transformer, echa un vistazo a la [Desarrollo de un transformador](guides/developing-a-transformer.md) guiar.

### Desarrollo de una acción

Para desarrollar una nueva acción, consulte el [Desarrollo de una acción](guides/developing-an-action.md) guiar.

## Contribuyendo

Las pautas de contribución siguen las de la [proyecto principal de DataHub](docs/CONTRIBUTING.md). Estamos aceptando contribuciones para Acciones, Transformadores y mejoras generales del marco (pruebas, manejo de errores, etc.).

## Recursos

Echa un vistazo a la [anuncio original](https://www.youtube.com/watch?v=7iwNxHgqxtg\&t=2189s) del Marco de Acciones de DataHub en el Ayuntamiento de DataHub de abril de 2022.

## Licencia

Apache 2.0
