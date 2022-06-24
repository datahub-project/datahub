*   Fecha de inicio: 2021-02-17
*   RFC PR: https://github.com/datahub-project/datahub/pull/2112
*   Problema de discusión: (problema de GitHub en el que se discutió antes del RFC, si lo hubiera)
*   PR(s) de implementación: (déjelo vacío)

# Etiquetas

## Resumen

Sugerimos una solución de etiquetado genérica y global para Datahub. Como la solución es bastante genérica y flexible, también puede
Esperemos que sirva como un trampolín para nuevas y geniales características en el futuro.

## Motivación

Actualmente, algunas entidades, como Datasets, se pueden etiquetar mediante cadenas, pero desafortunadamente esta solución es bastante
limitado.

Una implementación general de etiquetas nos permitirá definir y adjuntar un nuevo y sencillo tipo de metadatos a todo tipo de
Entidades. Como las etiquetas se definirían globalmente, etiquetar múltiples objetos con la misma etiqueta nos dará la posibilidad
para definir y buscar en función de un nuevo tipo de relación, por ejemplo, en qué conjuntos de datos y modelos de aprendizaje automático están etiquetados en
incluir datos de PII. Esto permite describir las relaciones entre objetos que de otro modo no tendrían un linaje directo.
relación. Además, las etiquetas bajarían esa barra para agregar metadatos simples a cualquier objeto en la instancia de Datahub y abrir
la puerta a los metadatos de crowdsourcing. Recordando que las etiquetas en sí mismas son entidades, también sería posible etiquetar
etiquetas, habilitando una especie de jerarquía.

La solución está destinada a ser bastante genérica y flexible, y no estamos tratando de ser demasiado obstinados sobre cómo un usuario
debe utilizar la función. Esperamos que esta solución genérica inicial pueda servir como un trampolín para futuros geniales en el
futuro.

## Requisitos

*   Posibilidad de asociar etiquetas con cualquier tipo de entidad, ¡incluso con otras etiquetas!
*   Posibilidad de etiquetar la misma entidad con varias etiquetas.
*   Posibilidad de etiquetar varios objetos con la misma instancia de etiqueta.
*   Al punto anterior, la capacidad de hacer búsquedas fáciles basadas en etiquetas más adelante.
*   Los metadatos de las etiquetas están por determinar

### Extensibilidad

Obviamente, se requiere el trabajo normal de incorporación de nuevas entidades.

Esperemos que esto pueda servir como un trampolín para trabajar en casos especiales como el etiquetado de privacidad basado en etiquetas mencionado en
la hoja de ruta.

## No requisitos

Dejemos el trabajo de interfaz de usuario requerido para esto para otro momento.

## Diseño detallado

Queremos introducir algunas novedades bajo `datahub/metadata-models/src/main/pegasus/com/linkedin/common/`.

### `Tag` entidad

Primero creamos un `TagMetadata` entidad, que define el objeto de etiqueta real.

La propiedad edit define los derechos de edición de la etiqueta, ya que algunas etiquetas (como las etiquetas de sensibilidad) deben ser de solo lectura para un
mayoría de usuarios

    /**
     * Tag information
     */
    record TagMetadata {
       /**
       * Tag URN, e.g. urn:li:tag:<name>
       */
       urn: TagUrn

       /**
       * Tag value.
       */
       value: string

       /**
       * Optional tag description
       */
       description: optional string

       /**
       * Audit stamp associated with creation of this tag
       */
       createStamp: AuditStamp
    }

### `TagAttachment`

Definimos un `TagAttachment`-model, que describe la aplicación de una etiqueta a una entidad

    /**
     * Tag information
     */
    record TagAttachment {

      /**
       * Tag in question
       */
      tag: TagUrn

      /**
       * Who has edit rights to this employment.
       * WIP, pending access-control support in Datahub.
       * Relevant for privacy tags at least.
       * We might also want to add view rights?
       */
      edit: union[None, any, role-urn]

       /**
       * Audit stamp associated with employment of this tag to this entity
       */
       attachmentStamp: AuditStamp
    }

### `Tags` contenedor

A continuación definimos un `Tags`-aspect, que se utiliza como contenedor para empleos de etiquetas.

    namespace com.linkedin.common

    /**
     * Tags information
     */
    record Tags {

       /**
       * List of tag employments
       */
       elements: array[TagAttachment] = [ ]
    }

Esto se puede usar fácilmente con entidades de pared que queremos poder usar etiquetas, por ejemplo. `Datasets`. Como vemos un
mucho potencial en el etiquetado de campos de conjuntos de datos individuales también, podemos agregar una referencia a un objeto Tags en el
`SchemaField` objeto o alternativa crear un nuevo `DatasetFieldTags`, similar a `DatasetFieldMapping`.

## Cómo enseñamos esto

Debemos crear/actualizar guías de usuario para educar a los usuarios para:

*   Sugerencias sobre cómo usar etiquetas: adición de metadatos de bajo umbral y la posibilidad de realizar nuevos tipos de búsquedas

## Inconvenientes

Esto es definitivamente más complejo que simplemente agregar cadenas a una matriz.

## Alternativas

Una matriz de cadena es una solución simple, pero permite la misma funcionalidad que se sugiere aquí.

Otra alternativa sería simplificar los modelos eliminando algunos de los metadatos en el `TagMetadata` y
`TagAttachment` entidades, como el campo de permisos editar/ver, los sellos de auditoría y las descripciones.

Apache Atlas utiliza un enfoque similar. El requisito de crear una instancia de etiqueta antes de que pueda asociarse con un
"activo", y el archivo adjunto se realiza utilizando una lista desplegable. Las etiquetas también pueden tener atributos y una descripción. Ver
[aquí](https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.5.3/bk_data-governance/content/ch_working_with_atlas_tags.html)
por ejemplo. Las etiquetas son una pieza central en la interfaz de usuario y se pueden buscar de forma legible, tan fácilmente como los conjuntos de datos.

Atlas también tiene un concepto muy estrechamente relacionado con las etiquetas, llamado *clasificación*. Las clasificaciones son similares a las etiquetas en
que necesitan ser creados por separado, pueden tener atributos (¿pero no descripción?) y están adjuntos a los activos se hace
utilizando una lista desplegable. Las clasificaciones tienen la funcionalidad añadida de propagación, lo que significa que son
se aplica automáticamente a los activos posteriores, a menos que se establezca específicamente para que no lo hagan. Cualquier cambio en una clasificación (digamos un
cambio de atributo) también fluye hacia abajo, y en los activos descendentes puede ver desde dónde está la clasificación
propagado desde. Ver
[aquí](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/using-atlas/content/propagate_classifications_to_derived_entities.html)
por ejemplo.

## Estrategia de implementación / adopción

El uso de la funcionalidad es opcional y no interrumpe otras funcionalidades tal como están. La solución es lo suficientemente genérica como para
los usuarios pueden usarlo fácilmente. Se puede tomar en uso como cualquier otra entidad y aspecto.

## Labor futura

*   agregar `Tags` a aspectos para entidades.
*   Implemente constructores de relaciones según sea necesario.
*   La implementación y la necesidad de control de acceso a las etiquetas es una pregunta abierta
*   Como esta es ante todo una herramienta para el descubrimiento, el trabajo de la interfaz de usuario es extensible:
    *   Crear etiquetas de una manera que dificulte la duplicación y los errores ortográficos.
    *   Adjuntar etiquetas a entidades: autocompletar, desplegable, etc.
    *   ¿Visualizando las etiquetas existentes y cuáles son las más populares?
*   Explore la idea de un tipo especial de "clasificación", que se propaga aguas abajo, como en Atlas.

## Preguntas no resueltas

*   ¿Cómo queremos asignar campos de dataset a etiquetas?
*   ¿Queremos implementar derechos de edición/visualización?
