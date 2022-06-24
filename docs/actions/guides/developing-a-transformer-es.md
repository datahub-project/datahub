# Desarrollo de un transformador

En esta guía, describiremos cada paso para desarrollar un transformador personalizado para el marco de acciones de DataHub.

## Visión general

Desarrollar un DataHub Actions Transformer es una cuestión de extender el `Transformer` clase base en Python, instalando su
Transformador para que sea visible para el marco y, a continuación, configurar el marco para que utilice el nuevo transformador.

## Paso 1: Definición de un transformador

Para implementar un transformador, necesitaremos extender el `Transformer` clase base y anular las siguientes funciones:

*   `create()` - Esta función se invoca para instanciar la acción, con un diccionario de configuración de forma libre
    extraído del archivo de configuración Acciones como entrada.
*   `transform()` - Esta función se invoca cuando se recibe un evento. Debe contener la lógica central del Transformador.
    y devolverá el evento transformado, o `None` si el evento debe filtrarse.

Comencemos por definir una nueva implementación de Transformer llamada `CustomTransformer`. Lo mantendremos simple: este Transformador
imprima la configuración que se proporciona cuando se crea e imprima los eventos que reciba.

```python
# custom_transformer.py
from datahub_actions.transform.transformer import Transformer
from datahub_actions.event.event import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from typing import Optional

class CustomTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        # Simply print the config_dict.
        print(config_dict)
        return cls(config_dict, ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        # Simply print the received event.
        print(event)
        # And return the original event (no-op)
        return event
```

## Paso 2: Instalación del transformador

Ahora que hemos definido el Transformer, necesitamos hacerlo visible para el marco haciendo
está disponible en el entorno de tiempo de ejecución de Python.

La forma más fácil de hacerlo es simplemente colocarlo en el mismo directorio que su archivo de configuración, en cuyo caso el nombre del módulo es el mismo que el archivo.
nombre - en este caso será `custom_transformer`.

### Avanzado: Instalación como paquete

Alternativamente, cree un `setup.py` en el mismo directorio que el nuevo Transformer para convertirlo en un paquete que pip pueda entender.

    from setuptools import find_packages, setup

    setup(
        name="custom_transformer_example",
        version="1.0",
        packages=find_packages(),
        # if you don't already have DataHub Actions installed, add it under install_requires
        # install_requires=["acryl-datahub-actions"]
    )

A continuación, instale el paquete

```shell
pip install -e .
```

dentro del módulo. (alt.`python setup.py`).

Una vez que hayamos hecho esto, nuestra clase será referenciable a través de `custom_transformer_example.custom_transformer:CustomTransformer`.

## Paso 3: Ejecutar la acción

Ahora que hemos definido nuestro Transformer, podemos crear un archivo de configuración de Acción que haga referencia al nuevo Transformer.
Tendremos que proporcionar el módulo Python completo y el nombre de clase al hacerlo.

*Configuración de ejemplo*

```yaml
# custom_transformer_action.yaml
name: "custom_transformer_test"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
transform: 
  - type: "custom_transformer_example.custom_transformer:CustomTransformer"
    config:
      # Some sample configuration which should be printed on create.
      config1: value1
action:
  # Simply reuse the default hello_world action
  type: "hello_world"
```

A continuación, ejecute el `datahub actions` como de costumbre:

```shell
datahub actions -c custom_transformer_action.yaml
```

Si todo está bien, su transformador ahora debería estar recibiendo e imprimiendo eventos.

### (Opcional) Paso 4: Contribuir con el transformador

Si su Transformer es generalmente aplicable, puede plantear un PR para incluirlo en la biblioteca principal de Transformer
proporcionado por DataHub. Todos los Transformers vivirán bajo el `datahub_actions/plugin/transform` dentro del directorio
[datahub-actions](https://github.com/acryldata/datahub-actions) depósito.

Una vez que haya agregado su nuevo Transformer allí, asegúrese de que sea reconocible actualizando el `entry_points` sección
del `setup.py` archivo. Esto le permite asignar un nombre único global para su Transformer, de modo que las personas puedan usar
sin definir la ruta completa del módulo.

#### Prerrequisitos:

Los requisitos previos para la consideración para la inclusión en la biblioteca principal de Transformer incluyen

*   **Ensayo** Defina pruebas unitarias para su transformador
*   **Desduplicación** Confirme que ningún transformador existente sirve para el mismo propósito, o se puede extender fácilmente para servir al mismo propósito.
