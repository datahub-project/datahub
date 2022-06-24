***

## título: "Uso de una fuente de ingesta personalizada"

# ¿Cómo usar una fuente de ingesta personalizada sin bifurcar Datahub?

Agregar un origen de ingesta personalizado es la forma más fácil de ampliar el marco de ingesta de Datahubs para admitir sistemas de origen
que aún no son oficialmente compatibles con Datahub.

## Lo que tienes que hacer

Lo primero que debe hacer es crear una fuente personalizada como se describe en
el [Guía de origen de metadata-ingestion](../../metadata-ingestion/adding-source.md) en tu propio proyecto.

### ¿Cómo usar esta fuente?

:::nota
[Ingestión basada en la interfaz de usuario](../ui-ingestion.md) actualmente no admite fuentes de ingesta personalizadas.
:::

Para poder usar esta fuente solo necesitas hacer algunas cosas.

1.  Cree un paquete de Python a partir de su proyecto, incluida la clase de origen personalizada.
2.  Instale este paquete en su entorno de trabajo donde está utilizando la CLI de Datahub para ingerir metadatos.

Ahora puede simplemente hacer referencia a su clase de origen de ingestión como un tipo en la receta yaML utilizando el sistema completo
nombre del paquete. Por ejemplo, si la estructura del proyecto tiene este aspecto `<project>/src/my-source/custom_ingestion_source.py`
con la clase de origen personalizada denominada `MySourceClass` su receta de YAML se vería como la siguiente:

```yaml
source:
  type: my-source.custom_ingestion_source.MySourceClass
  config:
  # place for your custom config defined in the configModel
```

Si ahora ejecuta la ingesta, el cliente datahub recogerá su código y llamará al `get_workunits` método y hacer
el resto para ti. Eso es todo.

### ¿Código de ejemplo?

Para ver ejemplos de cómo podría verse esta configuración y un buen punto de partida para crear su primera fuente personalizada, visite
nuestro [meta-mundo](https://github.com/acryldata/meta-world) repositorio de ejemplo.
