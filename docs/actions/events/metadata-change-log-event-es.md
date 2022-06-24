# Evento de registro de cambios de metadatos V1

## Tipo de evento

`MetadataChangeLog_v1`

## Visión general

Este evento se emite cuando se cambia cualquier aspecto del gráfico de metadatos de DataHub. Esto incluye creaciones, actualizaciones y eliminaciones de aspectos "versionados" y aspectos de "series temporales".

> Descargo de responsabilidad: Este evento es bastante poderoso, pero también de nivel bastante bajo. Debido a que expone directamente el modelo de metadatos subyacente, está sujeto a cambios estructurales y semánticos más frecuentes que el nivel superior. [Evento de cambio de entidad](entity-change-event.md). Recomendamos usar ese evento en su lugar para lograr su caso de uso cuando sea posible.

## Estructura del evento

Los campos incluyen

| Nombre | Tipo | Descripción | | opcional
|---------------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| entityUrn | | de cadenas Identificador único de la entidad que se está cambiando. Por ejemplo, la urna de un conjunto de datos.                                                                                                                                                                      | Falso |
| entityType | | de cadenas El tipo de entidad que se va a cambiar. Los valores admitidos incluyen dataset, gráfico, panel, dataFlow (Pipeline), dataJob (Tarea), dominio, etiqueta, glossaryTerm, corpGroup y corpUser.                                                                       | Falso |
| entityKeyAspect | | de objetos La estructura clave de la entidad que se cambió. Solo se presenta si la propuesta de cambio de metadatos contenía la estructura de clave sin procesar.                                                                                                                              | Verdadero |
| changeType | | de cadenas Tipo de cambio. UPSERT o DELETE son compatibles actualmente.                                                                                                                                                                                             | Falso |
| aspectName | | de cadenas El aspecto de la entidad que se cambió.                                                                                                                                                                                                                   | Falso |
| | de aspectos | de objetos El nuevo valor de aspecto. Null si se eliminó el aspecto.                                                                                                                                                                                                  | Verdadero |
| aspect.contentType | | de cadenas El tipo de serialización del aspecto en sí. El único valor admitido es `application/json`.                                                                                                                                                           | Falso |
| | aspect.value | de cadenas El aspecto serializado. Se trata de un serializado JSON que representa el documento de aspecto definido originalmente en PDL. Consulte https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin para obtener más información.                        | Falso |
| anteriorAspectValue | | de objetos El valor del aspecto anterior. Null si el aspecto no existía anteriormente.                                                                                                                                                                                | Verdadero |
| anteriorAspectValue.contentType | | de cadenas El tipo de serialización del aspecto en sí. El único valor admitido es  `application/json`                                                                                                                                                           | Falso |
| anteriorAspectValue.value | | de cadenas El aspecto serializado. Se trata de un serializado JSON que representa el documento de aspecto definido originalmente en PDL. Consulte https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin para obtener más información.                        | Falso |
| systemMetadata | | de objetos Los nuevos metadatos del sistema. Esto incluye el identificador de ejecución de ingestión, el registro de modelos y más. Para ver la estructura completa, consulte https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/SystemMetadata.pdl | Verdadero |
| anteriorSystemMetadata | | de objetos Los metadatos del sistema anteriores. Esto incluye el identificador de ejecución de ingestión, el registro de modelos y más. Para ver la estructura completa, consulte https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/mxe/SystemMetadata.pdl | Verdadero |
| | creado | de objetos Sello de auditoría sobre quién activó el cambio de metadatos y cuándo.                                                                                                                                                                                          | Falso |
| | created.time Número | La marca de tiempo en milisegundos cuando se produjo el cambio de aspecto.                                                                                                                                                                                         | Falso |
| | created.actor | de cadenas La URN del actor (por ejemplo, el corpuser) que desencadenó el cambio.

### Eventos de ejemplo

#### Evento de cambio de etiqueta

```json
{
    "entityType": "container",
    "entityUrn": "urn:li:container:DATABASE",
    "entityKeyAspect": null,
    "changeType": "UPSERT",
    "aspectName": "globalTags",
    "aspect": {
        "value": "{\"tags\":[{\"tag\":\"urn:li:tag:pii\"}]}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516475595,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
    },
    "previousAspectValue": null,
    "previousSystemMetadata": null,
    "created": {
        "time": 1651516475594,
        "actor": "urn:li:corpuser:datahub",
        "impersonator": null
    }
}
```

#### Evento de cambio de término del glosario

```json
{
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
    "entityKeyAspect": null,
    "changeType": "UPSERT",
    "aspectName": "glossaryTerms",
    "aspect": {
        "value": "{\"auditStamp\":{\"actor\":\"urn:li:corpuser:datahub\",\"time\":1651516599479},\"terms\":[{\"urn\":\"urn:li:glossaryTerm:CustomerAccount\"}]}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516599486,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
    },
    "previousAspectValue": null,
    "previousSystemMetadata": null,
    "created": {
        "time": 1651516599480,
        "actor": "urn:li:corpuser:datahub",
        "impersonator": null
    }
}
```

#### Evento de cambio de propietario

```json
{
    "auditHeader": null,
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
    "entityKeyAspect": null,
    "changeType": "UPSERT",
    "aspectName": "ownership",
    "aspect": {
        "value": "{\"owners\":[{\"type\":\"DATAOWNER\",\"owner\":\"urn:li:corpuser:datahub\"}],\"lastModified\":{\"actor\":\"urn:li:corpuser:datahub\",\"time\":1651516640488}}",
        "contentType": "application/json"
    },
    "systemMetadata": {
        "lastObserved": 1651516640493,
        "runId": "no-run-id-provided",
        "registryName": "unknownRegistry",
        "registryVersion": "0.0.0.0-dev",
        "properties": null
    },
    "previousAspectValue": {
        "value": "{\"owners\":[{\"owner\":\"urn:li:corpuser:jdoe\",\"type\":\"DATAOWNER\"},{\"owner\":\"urn:li:corpuser:datahub\",\"type\":\"DATAOWNER\"}],\"lastModified\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1581407189000}}",
        "contentType": "application/json"
    },
    "previousSystemMetadata": {
        "lastObserved": 1651516415088,
        "runId": "file-2022_05_02-11_33_35",
        "registryName": null,
        "registryVersion": null,
        "properties": null
    },
    "created": {
        "time": 1651516640490,
        "actor": "urn:li:corpuser:datahub",
        "impersonator": null
    }
}
```

## PREGUNTAS MÁS FRECUENTES

### ¿Dónde puedo encontrar todos los aspectos y sus esquemas?

¡Gran pregunta! Todos los eventos metadataChangeLog se basan en el modelo de metadatos que se compone de entidades,
Aspectos y relaciones que componen un gráfico de metadatos empresarial. Recomendamos consultar lo siguiente
recursos para obtener más información al respecto:

*   [Introducción al modelo de metadatos](https://datahubproject.io/docs/metadata-modeling/metadata-model)

También puede encontrar una lista completa de Entidades + Aspectos del Modelo de Metadatos en el **Modelado de metadatos > entidades** sección de la [documentos oficiales de DataHub](https://datahubproject.io/docs/).
