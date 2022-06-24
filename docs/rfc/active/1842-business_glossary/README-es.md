*   Fecha de inicio: 28/08/2020
*   RFC PR: 1842
*   Pr(s) de implementación: TBD

# Glosario de Negocios

## Resumen

Agregar compatibilidad con el glosario empresarial mejora el valor de los metadatos y aporta la visión empresarial. Esto ayuda a documentar los términos comerciales utilizados en toda la empresa y proporciona el vocabulario común a todas las partes interesadas / comunidad de datos. Esto alienta / motiva a la comunidad empresarial a interactuar con Data Catalog para descubrir los activos de datos relevantes de interés. Esto también permite encontrar la relación en los activos de datos a través de términos comerciales que les pertenecen. Este siguiente enlace ilustra la importancia del glosario de negocios [artículo](https://dataedo.com/blog/business-glossary-vs-data-dictionary).

## Motivación

Necesitamos modelar el Glosario de Negocios, donde el equipo de negocios puede definir los términos de negocio y vincularlos a los elementos de datos que se incorporan a las plataformas de datos / catálogos de datos. Esto da los siguientes beneficios:

*   Definir y habilitar vocabulario común en las organizaciones y permitir colaboraciones fáciles con las comunidades empresariales y técnicas
*   Las organizaciones pueden aprovechar las taxonomías existentes de la industria donde pueden importar las definiciones y pueden mejorar o definir términos / definiciones específicos
*   El quid de la cuestión y el uso del glosario empresarial será vincular el conjunto de datos/elementos a los Términos comerciales, de modo que las empresas/consumidores puedan descubrir fácilmente los conjuntos de datos interesados con la ayuda de los términos comerciales.
*   Promueva el uso y reduzca la redundancia: Business Glossary ayuda a descubrir los conjuntos de datos rápidamente a través de términos comerciales y esto también ayuda a reducir la incorporación innecesaria de conjuntos de datos iguales / similares por parte de diferentes consumidores.

## Diseño detallado

### Que es Business Glossary

**Glosario de Negocios**, es una lista de términos comerciales con sus definiciones. Define conceptos de negocio para una organización o industria y es independiente de cualquier base de datos específica o plataforma o proveedor.

**Diccionario de datos** es una descripción de un conjunto de datos, proporciona los detalles sobre los atributos y tipos de datos

### Relación

Aunque data Dictionary y Business Glossary son entidades separadas, funcionan muy bien juntas para describir diferentes aspectos y niveles de abstracción del entorno de datos de una organización.
Los términos comerciales se pueden vincular a entidades/tablas y columnas específicas en un diccionario de datos/activos de datos para proporcionar más contexto y una definición aprobada coherente a diferentes instancias de los términos en diferentes plataformas/bases de datos.

### Ejemplo de definición del glosario empresarial

| URN| Término comercial | Definición | | dominio/espacio de nombres | propietario Fuente Ext| | de referencia Ext
|--|--|--|--|--|--|--|
|urno:li:glosarioTérminos:instrument.cashInstrument | instrument.cashInstrument| punto de tiempo que incluye una fecha y una hora, opcionalmente incluyendo un desplazamiento de zona horaria| | de fundación abc@domain.com | | fibo https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/CashInstrument |
|urno:li:glosarioTérminos:common.dateTime | common.dateTime| un instrumento financiero cuyo valor está determinado por el mercado y que es fácilmente transferible (altamente líquido) | | financiero xyz@domain.com | | fibo https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/DateTime |
|urno:li:glosarioTérminos:market.bidTamaño | market.bidSize| El tamaño de la oferta representa la cantidad de un valor que los inversores están dispuestos a comprar a un precio de oferta específico| | de Trading xyz@domain.com | - | - | - |
|--|--|--|--|--|--|--|
| | | | | | | |

### Término de negocio y conjunto de datos - Relación

| Nombre del atributo| Tipo de datos| ¿Anulable?| **Término comercial**| Descripción|
|--|--|--|--|--|
| recordId| int| N| | |en el caso de FX QuoteData, el RecordId es igual al UIC de SymbolsBase|
| llegadaHora| TimestampTicks| N| | Tiempo en que el libro de precios fue recibido por el TickCollector. 100 segundos de nanosegundos desde el 1 de enero de 1970 (garrapatas)|
| bid1Precio| com.xxxx.yyy.schema.common.Price| N| **common.monetoryAmount**| El precio de oferta con rango 1/29.|
| bid1Tamaño| int| N| market.bidSize| La cantidad que el precio de oferta con rango 5/29 es buena para.|
|--|--|--|--|--|--|--|
| | | | | | | |

### Stiching Juntos

Business Glossary será una entidad de primera clase donde se puede definir el `GlossaryTerm`s y esto será similar a entidades como Dataset, CorporateUser, etc. Business Term se puede vincular a otras entidades como Dataset, DatasetField. En el futuro, los términos comerciales se pueden vincular a paneles, métricas, etc.

![high level design](business_glossary_rel.png)

El diagrama anterior ilustra cómo los términos de negocio se conectarán a otras entidades entidades como Dataset, DatasetField. El ejemplo anterior muestra que los términos comerciales son `Term-1`, `Term-2`, .. `Term-n` y cómo están vinculados a `DatasetField` y `Dataset`.
Conjunto de datos (`DS-1`) campos `e11` está vinculado a Business Term `Term-2` y `e12` está vinculado a `Term-1`.
Conjunto de datos (`DS-2`) elemento `e23` vincular el Término de Negocio `Term-2`, `e22` con `Term-3` y `e24` con `Term-5`. Dataset (DS-2) está vinculado al término comercial `Term-4`
Conjunto de datos (`DS-2`) it-self vinculado a Business Term `Term-4`

## Mejoras en el modelo de metadatos

Habrá 1 GMA de nivel superior [Entidades](../../../what/entity.md) en el diseño: glossaryTerm (Business Glossary).
Es importante hacer glossaryTerm como una entidad de nivel superior porque puede existir sin un conjunto de datos y puede ser definido de forma independiente por el equipo de negocios.

### Representación DE URN

Definiremos un [Urnas](../../../what/urn.md): `GlossaryTermUrn`.
Estas URN deben permitir la identificación única del término comercial.

Un término comercial URN (GlossaryTermUrn) se verá de la siguiente manera:

    urn:li:glossaryTerm:<<name>>

### Nuevo objeto Snapshot

Habrá un nuevo objeto de instantánea para incorporar términos comerciales junto con definiciones

Ruta : metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot/

```java
/**
 * A metadata snapshot for a specific GlossaryTerm entity.
 */
record GlossaryTermSnapshot {

  /**
   * URN for the entity the metadata snapshot is associated with.
   */
  urn: GlossaryTermUrn

  /**
   * The list of metadata aspects associated with the dataset. Depending on the use case, this can either be all, or a selection, of supported aspects.
   */
  aspects: array[GlossaryTermAspect]
}
```

Ruta : metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/

### GlosarioTérminosAspecto

Habrá un nuevo aspecto definido para capturar los atributos requeridos y la información de propiedad

    /**
     * A union of all supported metadata aspects for a GlossaryTerm
     */
    typeref GlossaryTermAspect = union[
      GlossaryTermInfo,
      Ownership
    ]

Definición de entidad de término comercial

```java
/**
 * Data model for a Business Term entity
 */
record GlossaryTermEntity includes BaseEntity {

  /**
   * Urn for the dataset
   */
  urn: GlossaryTermUrn

  /**
   * Business Term native name e.g. CashInstrument
   */
  name: optional string

}
```

### Glosario de entidadesTermInfo

```java
/**
 * Properties associated with a GlossaryTerm
 */
record GlossaryTermInfo {

  /**
   * Definition of business term
   */
  definition: string

  /**
   * Source of the Business Term (INTERNAL or EXTERNAL) with default value as INTERNAL
   */
  termSource: string

  /**
   * External Reference to the business-term (URL)
   */
  sourceRef: optional string

  /**
   * The abstracted URI such as https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/CashInstrument.
   */
  sourceUrl: optional Url

  /**
   * A key-value map to capture any other non-standardized properties for the glossary term
   */
  customProperties: map[string, string] = { }

}

```

### Realationship con el propietario

Los Términos comerciales serán cobrados por ciertos usuarios comerciales

    /**
     * A generic model for the Owned-By relationship
     */
    @pairings = [ {
      "destination" : "com.linkedin.common.urn.CorpuserUrn",
      "source" : "com.linkedin.common.urn.GlossaryTermUrn"
    }, {
       "destination" : "com.linkedin.common.urn.GlossaryTermUrn",
       "source" : "com.linkedin.common.urn.CorpuserUrn"
    } ]
    record OwnedBy includes BaseRelationship {

      /**
       * The type of the ownership
       */
      type: OwnershipType
    }

### Aspecto del glosario de negocios

Business Term se puede asociar con Dataset Field y Dataset. Definición del aspecto que se puede asociar con Dataset y DatasetField

    record GlossaryTerms {
      /**
       * The related business terms
       */
      terms: array[GlossaryTermAssociation]

      /**
       * Audit stamp containing who reported the related business term
       */
      auditStamp: AuditStamp
    }

    record GlossaryTermAssociation {
       /**
        * Urn of the applied glossary term
        */
        urn: GlossaryTermUrn
    }

Se propone que los siguientes cambios en SchemaField se asocien (opcionalmente) con Business Glossary (términos)

    record SchemaField {
      ...
      /**
       * Tags associated with the field
       */
      globalTags: optional GlobalTags

     +/**
     + * Glossary terms associated with the field
     + */
     +glossaryTerms: optional GlossaryTerms
    }

Se propone que los siguientes cambios en el aspecto Dataset se asocien (opcionalmente) con Business Glossary (términos)

    /**
     * A union of all supported metadata aspects for a Dataset
     */
    typeref DatasetAspect = union[
      DatasetProperties,
      DatasetDeprecation,
      UpstreamLineage,
      InstitutionalMemory,
      Ownership,
      Status,
      SchemaMetadata
    + GlossaryTerms
    ]

## Gráfico de metadatos

Esto podría no ser un requisito crítico, pero es bueno tenerlo.

1.  Los usuarios deben poder buscar Términos comerciales y les gustaría ver todos los conjuntos de datos que tienen elementos vinculados a ese término comercial.

## Cómo enseñamos esto

Debemos crear/actualizar guías de usuario para educar a los usuarios para:

*   Importancia y valor que el Glosario de Negocios aporta al Catálogo de Datos
*   Experiencia de búsqueda y descubrimiento a través de términos comerciales (cómo encontrar rápidamente conjuntos de datos relevantes en DataHub)

## Alternativas

Esta es una nueva característica de Datahub que aporta la vocabulry común a través de las partes interesadas en los datos y también permite una mejor capacidad de descubrimiento de los conjuntos de datos. Veo que no hay una alternativa clara a esta característica, a lo sumo los usuarios pueden documentar el `business term` fuera de la `Data Catalog` y puede hacer referencia/asociar esos términos como una propiedad adicional a la columna Dataset.

## Estrategia de implementación / adopción

Se supone que el diseño es lo suficientemente genérico como para que cualquier usuario de DataHub pueda incorporar fácilmente su Glosario de negocios (lista de términos y definiciones) a DataHub, independientemente de su industria. Algunas organizaciones pueden suscribirse / descargar taxonomía estándar de la industria con un ligero modelado e integración que debería poder traer el glosario de negocios rápidamente

Al incorporar conjuntos de datos, los equipos de negocios / tecnología deben vincular los términos comerciales a los elementos de datos, una vez que los usuarios vean el valor de esto, se sentirán motivados a vincular los elementos con los términos comerciales apropiados.

## Preguntas no resueltas

*   Este RFC no cubre el diseño de la interfaz de usuario para la definición del glosario empresarial.
