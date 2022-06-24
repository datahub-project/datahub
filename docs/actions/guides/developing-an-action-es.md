# Desarrollo de una acción

En esta guía, describiremos cada paso para desarrollar una Acción para el Marco de Acciones de DataHub.

## Visión general

Desarrollar una acción de DataHub es una cuestión de extender el `Action` clase base en Python, instalando su
Acción para que sea visible para el marco y, a continuación, configurar el marco para que use la nueva acción.

## Paso 1: Definir una acción

Para implementar una acción, tendremos que ampliar el `Action` clase base y anular las siguientes funciones:

*   `create()` - Esta función se invoca para instanciar la acción, con un diccionario de configuración de forma libre
    extraído del archivo de configuración Acciones como entrada.
*   `act()` - Esta función se invoca cuando se recibe una Acción. Debe contener la lógica central de la Acción.
*   `close()` - Esta función se invoca cuando el framework ha emitido un apagado de la tubería. Debe usarse
    para limpiar cualquier proceso que ocurra dentro de la Acción.

Comencemos por definir una nueva implementación de Acción llamada `CustomAction`. Lo mantendremos simple: esta acción
imprima la configuración que se proporciona cuando se crea e imprima los eventos que reciba.

```python
# custom_action.py
from datahub_actions.action.action import Action
from datahub_actions.event.event import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

class CustomAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        # Simply print the config_dict.
        print(config_dict)
        return cls(ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def act(self, event: EventEnvelope) -> None:
        # Do something super important.
        # For now, just print. :) 
        print(event)

    def close(self) -> None:
        pass
```

## Paso 2: Instalación de la acción

Ahora que hemos definido la Acción, necesitamos hacerla visible para el marco haciéndola
disponible en el entorno de tiempo de ejecución de Python.

La forma más fácil de hacerlo es simplemente colocarlo en el mismo directorio que su archivo de configuración, en cuyo caso el nombre del módulo es el mismo que el archivo.
nombre - en este caso será `custom_action`.

### Avanzado: Instalación como paquete

Alternativamente, cree un `setup.py` en el mismo directorio que la nueva Acción para convertirlo en un paquete que pip pueda entender.

    from setuptools import find_packages, setup

    setup(
        name="custom_action_example",
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

Una vez que hayamos hecho esto, nuestra clase será referenciable a través de `custom_action_example.custom_action:CustomAction`.

## Paso 3: Ejecutar la acción

Ahora que hemos definido nuestra Acción, podemos crear un archivo de configuración de Acción que haga referencia a la nueva Acción.
Tendremos que proporcionar el módulo Python completo y el nombre de clase al hacerlo.

*Configuración de ejemplo*

```yaml
# custom_action.yaml
name: "custom_action_test"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
action:
  type: "custom_action_example.custom_action:CustomAction"
  config:
    # Some sample configuration which should be printed on create.
    config1: value1
```

A continuación, ejecute el `datahub actions` como de costumbre:

```shell
datahub actions -c custom_action.yaml
```

Si todo está bien, su Acción ahora debería estar recibiendo e imprimiendo Eventos.

## (Opcional) Paso 4: Contribuir a la acción

Si su Acción es generalmente aplicable, puede plantear un PR para incluirlo en la biblioteca de Acción principal
proporcionado por DataHub. Todas las Acciones vivirán bajo el `datahub_actions/plugin/action` dentro del directorio
[datahub-actions](https://github.com/acryldata/datahub-actions) depósito.

Una vez que haya agregado su nueva Acción allí, asegúrese de que sea reconocible actualizando el `entry_points` sección
del `setup.py` archivo. Esto le permite asignar un nombre único global para su Acción, de modo que las personas puedan usar
sin definir la ruta completa del módulo.

### Prerrequisitos:

Los requisitos previos para la consideración para su inclusión en la biblioteca de acciones principal incluyen

*   **Ensayo** Definir pruebas unitarias para la acción
*   **Desduplicación** Confirme que ninguna acción existente sirve al mismo propósito, o puede ampliarse fácilmente para servir al mismo propósito
