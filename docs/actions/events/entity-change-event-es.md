# Evento de cambio de entidad V1

## Tipo de evento

`EntityChangeEvent_v1`

## Visión general

Este evento se emite cuando se realizan ciertos cambios en una entidad (conjunto de datos, panel, gráfico, etc.) en DataHub.

## Estructura del evento

Los eventos de cambio de entidad se generan en una variedad de circunstancias, pero comparten un conjunto común de campos.

### Campos comunes

| Nombre | Tipo | Descripción | | opcional
|------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| entityUrn | | de cadenas Identificador único de la entidad que se está cambiando. Por ejemplo, la urna de un conjunto de datos.                                                                                                                      | Falso |
| entityType | | de cadenas El tipo de entidad que se va a cambiar. Los valores admitidos incluyen dataset, gráfico, panel, dataFlow (Pipeline), dataJob (Tarea), dominio, etiqueta, glossaryTerm, corpGroup y corpUser.                       | Falso |
| categoría | | de cadenas La categoría del cambio, relacionada con el tipo de operación que se realizó. Los ejemplos incluyen TAG, GLOSSARY_TERM, DOMINIO, CICLO DE VIDA y más.                                                     | Falso |
| | de operación | de cadenas La operación que se está realizando en la entidad dada la categoría. Por ejemplo, ADD , REMOVE, MODIFY. Para el conjunto de operaciones válidas, consulte el catálogo completo a continuación.                                         | Falso |
| | modificador | de cadenas Modificador que se ha aplicado a la entidad. El valor depende de la categoría. Un ejemplo incluye el URN de una etiqueta que se aplica a un dataset o campo de esquema.                                  | Verdadero |
| | de parámetros Dict | Parámetros clave-valor adicionales utilizados para proporcionar un contexto específico. El contenido preciso depende de la categoría + operación del evento. Consulte el catálogo a continuación para obtener un resumen completo de las combinaciones. | Verdadero |
| auditStamp.actor | | de cadenas La urna del actor que desencadenó el cambio.                                                                                                                                                         | Falso |
| | auditStamp.time Número | La marca de tiempo en milisegundos correspondiente al evento.                                                                                                                                              | Falso |

En las secciones siguientes, proporcionaremos eventos de ejemplo para cada escenario en el que se activan los eventos de cambio de entidad.

### Agregar evento de etiqueta

Este evento se emite cuando se ha agregado una etiqueta a una entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "ADD",
  "modifier": "urn:li:tag:PII",
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Quitar evento de etiqueta

Este evento se emite cuando se ha quitado una etiqueta de una entidad en DataHub.
Encabezado

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "REMOVE",
  "modifier": "urn:li:tag:PII",
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Agregar término de glosario (evento)

Este evento se emite cuando se ha agregado un término de glosario a una entidad en DataHub.
Encabezado

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "GLOSSARY_TERM",
  "operation": "ADD",
  "modifier": "urn:li:glossaryTerm:ExampleNode.ExampleTerm",
  "parameters": {
    "termUrn": "urn:li:glossaryTerm:ExampleNode.ExampleTerm"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Eliminar el evento De término del glosario

Este evento se emite cuando se ha quitado un término del glosario de una entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "GLOSSARY_TERM",
  "operation": "REMOVE",
  "modifier": "urn:li:glossaryTerm:ExampleNode.ExampleTerm",
  "parameters": {
    "termUrn": "urn:li:glossaryTerm:ExampleNode.ExampleTerm"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Agregar evento de dominio

Este evento se emite cuando se ha agregado Dominio a una entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOMAIN",
  "operation": "ADD",
  "modifier": "urn:li:domain:ExampleDomain",
  "parameters": {
    "domainUrn": "urn:li:domain:ExampleDomain"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Quitar evento de dominio

Este evento se emite cuando se ha quitado Dominio de una entidad en DataHub.
Encabezado

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOMAIN",
  "operation": "REMOVE",
  "modifier": "urn:li:domain:ExampleDomain",
  "parameters": {
     "domainUrn": "urn:li:domain:ExampleDomain"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Agregar evento de propietario

Este evento se emite cuando se ha asignado un nuevo propietario a una entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "OWNER",
  "operation": "ADD",
  "modifier": "urn:li:corpuser:jdoe",
  "parameters": {
     "ownerUrn": "urn:li:corpuser:jdoe",
     "ownerType": "BUSINESS_OWNER"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Quitar evento de propietario

Este evento se emite cuando se ha quitado un propietario existente de una entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "OWNER",
  "operation": "REMOVE",
  "modifier": "urn:li:corpuser:jdoe",
  "parameters": {
    "ownerUrn": "urn:li:corpuser:jdoe",
    "ownerType": "BUSINESS_OWNER"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Modificar evento de obsolescencia

Este evento se emite cuando se ha modificado el estado de obsolescencia de una entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DEPRECATION",
  "operation": "MODIFY",
  "modifier": "DEPRECATED",
  "parameters": {
    "status": "DEPRECATED"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Evento de campo de esquema de conjunto de datos de agregar

Este evento se emite cuando se ha agregado un nuevo campo a un esquema de conjunto de datos.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TECHNICAL_SCHEMA",
  "operation": "ADD",
  "modifier": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
  "parameters": {
    "fieldUrn": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
    "fieldPath": "newFieldName",
    "nullable": false
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Quitar evento de campo de esquema de conjunto de datos

Este evento se emite cuando se ha quitado un nuevo campo de un esquema de conjunto de datos.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TECHNICAL_SCHEMA",
  "operation": "REMOVE",
  "modifier": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
  "parameters": {
    "fieldUrn": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
    "fieldPath": "newFieldName",
    "nullable": false
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Evento de creación de entidad

Este evento se emite cuando se ha creado una nueva entidad en DataHub.
Encabezado

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "CREATE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Evento de eliminación suave de entidad

Este evento se emite cuando una nueva entidad se ha eliminado por software en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "SOFT_DELETE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Evento de eliminación de elementos duros de entidad

Este evento se emite cuando se ha eliminado una nueva entidad en DataHub.

#### Ejemplo de evento

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "HARD_DELETE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```
