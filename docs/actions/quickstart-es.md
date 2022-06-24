# Inicio rápido de acciones de DataHub

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

Para instalar DataHub Actions, debe instalar el `acryl-datahub-actions` paquete de PyPi

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions

# Verify the installation by checking the version.
datahub actions version
```

### Hola mundo

DataHub se envía con una acción "Hello World" que registra todos los eventos que recibe en la consola.
Para ejecutar esta acción, simplemente cree un nuevo archivo de configuración de Acción:

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
action:
  type: "hello_world"
```

y, a continuación, ejecútelo utilizando el comando `datahub actions` mandar:

```shell
datahub actions -c hello_world.yaml
```

Debería ver el siguiente resultado si la acción se ha iniciado correctamente:

```shell
Action Pipeline with name 'hello_world' is now running.
```

Ahora, navegue hasta la instancia de DataHub a la que se ha conectado y realice una acción como

*   Agregar / eliminar una etiqueta
*   Agregar / eliminar un término del glosario
*   Agregar / eliminar un dominio

Si todo está bien, debería ver algunos eventos registrados en la consola

```shell
Hello world! Received event:
{
    "event_type": "EntityChangeEvent_v1",
    "event": {
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
        "category": "TAG",
        "operation": "ADD",
        "modifier": "urn:li:tag:pii",
        "parameters": {},
        "auditStamp": {
            "time": 1651082697703,
            "actor": "urn:li:corpuser:datahub",
            "impersonator": null
        },
        "version": 0,
        "source": null
    },
    "meta": {
        "kafka": {
            "topic": "PlatformEvent_v1",
            "offset": 1262,
            "partition": 0
        }
    }
}
```

*Ejemplo de un evento emitido cuando se ha agregado una etiqueta 'pii' a un conjunto de datos.*

¡Woohoo! Ha comenzado a usar correctamente el marco de acciones. Ahora, veamos cómo podemos ponernos elegantes.

#### Filtrado de eventos

Si sabemos qué tipos de eventos nos gustaría consumir, opcionalmente podemos añadir un `filter` configuración, que
evitará que los eventos que no coincidan con el filtro se reenvíen a la acción.

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
action:
  type: "hello_world"
```

*Filtrado solo para eventos de tipo EntityChangeEvent_v1*

#### Filtrado avanzado

Más allá de simplemente filtrar por tipo de evento, también podemos filtrar eventos haciendo coincidir los valores de sus campos. Para ello,
utilice el botón `event` Bloquear. Cada campo proporcionado se comparará con el valor del evento real. Un evento que coincide
**todo** de los campos se remitirán a la acción.

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
  event: 
    category: "TAG"
    operation: "ADD"
    modifier: "urn:li:tag:pii"
action:
  type: "hello_world"
```

*Este filtro solo coincide con los eventos que representan las adiciones de etiquetas "PII" a una entidad.*

Y más, podemos lograr la semántica "OR" en un campo en particular al proporcionar una matriz de valores.

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
  event: 
    category: "TAG"
    operation: [ "ADD", "REMOVE" ]
    modifier: "urn:li:tag:pii"
action:
  type: "hello_world"
```

*Este filtro solo coincide con los eventos que representan las adiciones de etiquetas "PII" a las eliminaciones or de una entidad. ¡Qué lujo!*
