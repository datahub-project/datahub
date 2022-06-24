# Configuración de la retención de bases de datos

## Gol

DataHub almacena diferentes versiones de [aspectos de metadatos](https://datahubproject.io/docs/what/aspect) a medida que se ingieren
utilizando una base de datos (o almacén clave-valor).  Estas múltiples versiones nos permiten ver los cambios históricos de un aspecto y
revertir a una versión anterior si se ingieren metadatos incorrectos. Sin embargo, cada versión almacenada requiere almacenamiento adicional
espacio, mientras que posiblemente aporta menos valor al sistema. Tenemos que ser capaces de imponer un **retención** política sobre estos
registros para mantener el tamaño de la base de datos bajo control.

El objetivo del sistema de retención es poder **configurar y aplicar directivas de retención** en los documentos de cada uno de ellos
varios niveles:

*   global
*   nivel de entidad
*   nivel de aspecto

## ¿Qué tipo de políticas de retención se admiten?

Soportamos 3 tipos de políticas de retención para aspectos:

|     Política |            Versiones mantenidas |
|:-------------:|:-----------------------------------:|
| | indefinida Todas las versiones |
| | basada en versiones Último *N* versiones |
| | basada en el tiempo Versiones ingeridas en la última *N* segundos |

**Nota:** La última versión (versión 0) nunca se elimina. Esto garantiza que la funcionalidad principal de DataHub no se vea afectada al aplicar la retención.

## ¿Cuándo se aplica la política de retención?

A partir de ahora, las políticas de retención se aplican en dos lugares:

1.  **Arranque de GMS**: un paso de arranque ingiere el conjunto predefinido de políticas de retención. Si no existía ninguna directiva antes o la directiva existente
    se actualizó, se activará una llamada asincrónica.  Aplicará la política de retención (o políticas) a **todo** registros en la base de datos.
2.  **Ingerir**: En cada ingesta, si se actualizó un aspecto existente, se aplica la política de retención al par urna-aspecto que se está ingiriendo.

Estamos planeando apoyar una aplicación de retención basada en cron en un futuro cercano para garantizar que la retención basada en el tiempo se aplique correctamente.

## ¿Cómo configurar?

Para la iteración inicial, hemos hecho que esta función sea opt-in. Por favor, configure **ENTITY_SERVICE_ENABLE_RETENTION=verdadero** cuando
creando el contenedor datahub-gms/pod k8s.

En la puesta en marcha de GMS, las políticas de retención se inicializan con:

1.  En primer lugar, el valor predeterminado proporcionado **basado en versiones** retención para mantener **20 últimos aspectos** para todos los pares entidad-aspecto.
2.  En segundo lugar, leemos los archivos YAML del `/etc/datahub/plugins/retention` y superponerlos en el conjunto predeterminado de políticas que proporcionamos.

Para docker, configuramos docker-compose para montar `${HOME}/.datahub` directorio a `/etc/datahub` directorio
dentro de los contenedores, para que pueda personalizar el conjunto inicial de políticas de retención creando
un `${HOME}/.datahub/plugins/retention/retention.yaml` archivo.

Admitiremos una forma estandarizada de hacer esto en la configuración de Kubernetes en un futuro próximo.

El formato para el archivo YAML es el siguiente:

```yaml
- entity: "*" # denotes that policy will be applied to all entities
  aspect: "*" # denotes that policy will be applied to all aspects
  config:
    retention:
      version:
        maxVersions: 20
- entity: "dataset"
  aspect: "datasetProperties"
  config:
    retention:
      version:
        maxVersions: 20
      time:
        maxAgeInSeconds: 2592000 # 30 days
```

Tenga en cuenta que busca las políticas correspondientes al par de aspectos de entidad en el siguiente orden:

1.  entidad, aspecto
2.  \*, aspecto
3.  entidad, \*
4.  \*, \*

Al reiniciar datahub-gms después de crear el archivo yaml del complemento, se aplicará el nuevo conjunto de políticas de retención.
