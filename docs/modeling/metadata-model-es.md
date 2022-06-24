***

## título: The Metadata Model&#xD;&#xA;sidebar_label: El modelo de metadatos&#xD;&#xA;slug: /metadata-modeling/metadata-model

# ¿Cómo modela DataHub los metadatos?

DataHub adopta un enfoque de esquema primero para modelar metadatos. Utilizamos el lenguaje de esquema Pegasus de código abierto ([PDL](https://linkedin.github.io/rest.li/pdl_schema)) ampliado con un conjunto personalizado de anotaciones para modelar metadatos. La capa de almacenamiento, servicio, indexación e ingesta de DataHub opera directamente sobre el modelo de metadatos y admite tipos fuertes desde el cliente hasta la capa de almacenamiento.

Conceptualmente, los metadatos se modelan utilizando las siguientes abstracciones

*   **Entidades**: Una entidad es el nodo principal en el gráfico de metadatos. Por ejemplo, una instancia de un dataset o un corpUser es una entidad. Una entidad se compone de un tipo, por ejemplo, 'conjunto de datos', un identificador único (por ejemplo, una 'urna') y grupos de atributos de metadatos (por ejemplo, documentos) que llamamos aspectos.

*   **Aspectos**: Un aspecto es una colección de atributos que describe una faceta particular de una entidad. Son la unidad atómica de escritura más pequeña en DataHub. Es decir, múltiples aspectos asociados a una misma Entidad se pueden actualizar de forma independiente. Por ejemplo, DatasetProperties contiene una colección de atributos que describe un dataset. Los aspectos se pueden compartir entre entidades, por ejemplo, "Propiedad" es un aspecto que se reutiliza en todas las entidades que tienen propietarios. Los aspectos comunes incluyen

    *   [propiedad](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl): captura los usuarios y grupos propietarios de una entidad.
    *   [globalEtiquetas](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/GlobalTags.pdl): captura referencias a las etiquetas asociadas a una entidad.
    *   [glosarioTérminos](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/GlossaryTerms.pdl): captura referencias a los términos del glosario asociados a una entidad.
    *   [institucionalMemoria](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/InstitutionalMemory.pdl): Captura documentos internos de la empresa asociados con una entidad (por ejemplo, enlaces)
    *   [estado](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Status.pdl): Captura el estado de "eliminación" de una Entidad, es decir, si debe eliminarse por software.
    *   [Subtipos](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/SubTypes.pdl): captura uno o más "subtipos" de un tipo de entidad más genérico. Un ejemplo puede ser un conjunto de datos "Looker Explore", un conjunto de datos "Ver". Los subtipos específicos pueden implicar que ciertos aspectos adicionales están presentes para una Entidad determinada.

*   **Relaciones**: una relación representa una arista con nombre entre 2 entidades. Se declaran a través de atributos de clave externa dentro de Aspects junto con una anotación personalizada (@Relationship). Las relaciones permiten atravesar las aristas bidireccionalmente. Por ejemplo, un gráfico puede referirse a un corpUser como su propietario a través de una relación llamada "OwnedBy". Esta arista sería transitable a partir del gráfico *o* la instancia de CorpUser.

*   **Identificadores (llaves y urnas)**: Una clave es un tipo especial de aspecto que contiene los campos que identifican de forma única a una entidad individual. Los aspectos clave se pueden serializar en *Urnas*, que representan una forma encadenada de los campos clave utilizados para la búsqueda de clave primaria. Además *Urnas* se puede convertir de nuevo en estructuras de aspectos clave, haciendo de los aspectos clave un tipo de aspecto "virtual". Los aspectos clave proporcionan un mecanismo para que los clientes lean fácilmente los campos que comprenden la clave principal, que generalmente son útiles como nombres de conjuntos de datos, nombres de plataformas, etc. Las urnas proporcionan un manejo amigable por el cual las Entidades pueden ser consultadas sin requerir una estructura completamente materializada.

Aquí hay un gráfico de ejemplo que consta de 3 tipos de entidad (CorpUser, Chart, Dashboard), 2 tipos de relación (OwnedBy, Contains) y 3 tipos de aspecto de metadatos (Ownership, ChartInfo y DashboardInfo).

![metadata-modeling](../imgs/metadata-model-chart.png)

## Las entidades principales

Los tipos de entidad "principales" de DataHub modelan los activos de datos que componen la pila de datos moderna. Incluyen

1.  **[Plataforma de datos](docs/generated/metamodel/entities/dataPlatform.md)**: Un tipo de "Plataforma" de Datos. Es decir, un sistema externo que participa en el procesamiento, almacenamiento o visualización de activos de datos. Los ejemplos incluyen MySQL, Snowflake, Redshift y S3.
2.  **[Conjunto de datos](docs/generated/metamodel/entities/dataset.md)**: Una recopilación de datos. Las tablas, las vistas, las secuencias, las colecciones de documentos y los archivos se modelan como "conjuntos de datos" en DataHub. Los conjuntos de datos pueden tener etiquetas, propietarios, enlaces, términos del glosario y descripciones adjuntas a ellos. También pueden tener subtipos específicos, como "Ver", "Colección", "Transmitir", "Explorar" y más. Los ejemplos incluyen tablas Postgres, colecciones MongoDB o archivos S3.
3.  **[Gráfico](docs/generated/metamodel/entities/chart.md)**: Una única visualización de datos derivada de un dataset. Un solo gráfico puede formar parte de varios paneles. Los gráficos pueden tener etiquetas, propietarios, enlaces, términos del glosario y descripciones adjuntas a ellos. Los ejemplos incluyen un superconjunto o un gráfico de looker.
4.  **[Salpicadero](docs/generated/metamodel/entities/dashboard.md)**: Una colección de gráficos para visualización. Los paneles pueden tener etiquetas, propietarios, enlaces, términos del glosario y descripciones adjuntas a ellos. Los ejemplos incluyen un superconjunto o un panel de control de modo.
5.  **[Trabajo de datos](docs/generated/metamodel/entities/dataJob.md)** (Tarea): Un trabajo ejecutable que procesa activos de datos, donde "procesar" implica consumir datos, producir datos o ambos. Los trabajos de datos pueden tener etiquetas, propietarios, enlaces, términos del glosario y descripciones adjuntas. Deben pertenecer a un único flujo de datos. Los ejemplos incluyen una tarea de flujo de aire.
6.  **[Flujo de datos](docs/generated/metamodel/entities/dataFlow.md)** (Pipeline): una colección ejecutable de trabajos de datos con dependencias entre ellos, o un DAG. Los trabajos de datos pueden tener etiquetas, propietarios, enlaces, términos del glosario y descripciones adjuntas. Los ejemplos incluyen un DAG de flujo de aire.

Ver el **Modelado de metadatos/Entidades** a la izquierda para explorar todo el modelo.

## El Registro de Entidades

¿Dónde están definidas las entidades y sus aspectos en DataHub? ¿Dónde "vive" el modelo de metadatos? El modelo de metadatos se une por medio de
de un **Registro de Entidades**, un catálogo de Entidades que componen el Gráfico de Metadatos junto con los aspectos asociados a cada una. Poner
simplemente, aquí es donde se define el "esquema" del modelo.

Tradicionalmente, el Registro de Entidades se construía utilizando [Instantánea](https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot) modelos, que son esquemas que vinculan explícitamente
una Entidad a los Aspectos asociados a ella. Un ejemplo es [DatasetSnapshot](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot/DatasetSnapshot.pdl), que define el núcleo `Dataset` Entidad.
Los aspectos de la entidad Dataset se capturan a través de un campo de unión dentro de un esquema especial "Aspect". Un ejemplo es
[DatasetAspect](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/DatasetAspect.pdl).
Este archivo asocia aspectos específicos del conjunto de datos (como [DatasetProperties](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProperties.pdl)) y aspectos comunes (como [Propiedad](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl),
[Memoria Institucional](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/InstitutionalMemory.pdl),
y [Estado](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Status.pdl))
a la entidad Dataset. Este enfoque para definir Entidades pronto quedará obsoleto en favor de un nuevo enfoque.

A partir de enero de 2022, DataHub ha dejado de admitir modelos de instantáneas como medio para agregar nuevas entidades. En lugar de
El Registro de entidades se define dentro de un archivo de configuración YAML denominado [entity-registry.yml](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/resources/entity-registry.yml),
que se proporciona al servicio de metadatos de DataHub en el inicio. Este fichero declara Entidades y Aspectos haciendo referencia a sus [Nombres](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl#L7).
En el momento del arranque, DataHub valida la estructura del archivo de registro y garantiza que pueda encontrar esquemas PDL asociados con
cada nombre de aspecto proporcionado por la configuración (a través del [@Aspect](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl#L6) anotación).

Al pasar a este formato, la evolución del modelo de metadatos se vuelve mucho más fácil. Agregar entidades y aspectos se convierte en una cuestión de agregar un
a la configuración yaML, en lugar de crear nuevos archivos Snapshot / Aspect.

## Exploración del modelo de metadatos de DataHub

Para explorar el modelo de metadatos de DataHub actual, puede inspeccionar esta imagen de alto nivel que muestra las diferentes entidades y bordes entre ellos mostrando las relaciones entre ellos.
![Metadata Model Graph](../imgs/datahub-metadata-model.png)

Para navegar por el modelo de aspecto para entidades específicas y explorar relaciones utilizando el `foreign-key` concepto, puede verlos en nuestro entorno de demostración o navegar por los documentos generados automáticamente en el **Modelado de metadatos/Entidades** sección a la izquierda.

Por ejemplo, aquí hay enlaces útiles a las entidades más populares en el modelo de metadatos de DataHub:

*   [Conjunto de datos](docs/generated/metamodel/entities/dataset.md): [Perfil](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,Dataset,PROD\)/Schema?is_lineage_mode=false) [Documentación](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,Dataset,PROD\)/Documentation?is_lineage_mode=false)
*   [Salpicadero](docs/generated/metamodel/entities/dashboard.md): [Perfil](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,Dashboard,PROD\)/Schema?is_lineage_mode=false) [Documentación](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,Dashboard,PROD\)/Documentation?is_lineage_mode=false)
*   [Usuario (también conocido como CorpUser)](docs/generated/metamodel/entities/corpuser.md): [Perfil](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,Corpuser,PROD\)/Schema?is_lineage_mode=false) [Documentación](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,Corpuser,PROD\)/Documentation?is_lineage_mode=false)
*   [Pipeline (también conocido como DataFlow)](docs/generated/metamodel/entities/dataFlow.md): [Perfil](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,DataFlow,PROD\)/Schema?is_lineage_mode=false) [Documentación](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,DataFlow,PROD\)/Documentation?is_lineage_mode=false)
*   [Tabla de características (también conocida como MLFeatureTable)](docs/generated/metamodel/entities/mlFeatureTable.md): [Perfil](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,MlFeatureTable,PROD\)/Schema?is_lineage_mode=false) [Documentación](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:datahub,MlFeatureTable,PROD\)/Documentation?is_lineage_mode=false)
*   Para obtener la lista completa de entidades en el modelo de metadatos, examinarlas [aquí](https://demo.datahubproject.io/browse/dataset/prod/datahub/entities) o utilice el botón **Modelado de metadatos/Entidades** sección a la izquierda.

### Generación de documentación para el modelo de metadatos

*   Este sitio web: La documentación del modelo de metadatos para este sitio web se genera utilizando `./gradlew :docs-website:yarnBuild`, que delega la generación de documentos del modelo en el `modelDocGen` en el `metadata-ingestion` módulo.
*   Carga de documentación en una instancia de DataHub en ejecución: la documentación del modelo de metadatos se puede generar y cargar en una instancia de DataHub en ejecución mediante el comando `./gradlew :metadata-ingestion:modelDocUpload`. ***NOTA***: Esto cargará la documentación del modelo en la instancia de DataHub que se ejecuta en la variable de entorno `$DATAHUB_SERVER` (http://localhost:8080 por defecto)

## Consulta del gráfico de metadatos

El lenguaje de modelado de DataHub le permite optimizar la persistencia de metadatos para alinearse con los patrones de consulta.

Hay tres formas compatibles de consultar el gráfico de metadatos: mediante la búsqueda de claves principales, una consulta de búsqueda y mediante el recorrido de relaciones.

> Nuevo en [PDL](https://linkedin.github.io/rest.li/pdl_schema) ¿Archivos? No te preocupes. Son solo una forma de definir un "esquema" de documento JSON para Aspectos en DataHub. Todos los datos ingeridos en el servicio de metadatos de DataHub se validan con un esquema PDL, y cada @Aspect corresponde a un único esquema. Estructuralmente, PDL es bastante similar a [Protobuf](https://developers.google.com/protocol-buffers) y se asigna convenientemente a JSON.

### Consulta de una entidad

#### Obtención de los últimos aspectos de la entidad (instantánea)

Consultar una entidad por clave principal significa usar el extremo "entidades", pasando el
urna de la entidad a recuperar.

Por ejemplo, para obtener una entidad Chart, podemos usar lo siguiente `curl`:

    curl --location --request GET 'http://localhost:8080/entities/urn%3Ali%3Achart%3Acustomers

Esta solicitud devolverá un conjunto de aspectos versionados, cada uno en la última versión.

Como notará, realizamos la búsqueda utilizando la url codificada *Urna* asociado a una entidad.
La respuesta sería un registro de "Entidad" que contiene la Instantánea de Entidad (que a su vez contiene los últimos aspectos asociados con la Entidad).

#### Obtención de aspectos versionados

DataHub también admite la obtención de piezas individuales de metadatos sobre una entidad, lo que llamamos aspectos. Para ello,
proporcionará la clave principal (urna) de una entidad junto con el nombre del aspecto y la versión que desea recuperar.

Por ejemplo, para obtener la versión más reciente del aspecto SchemaMetadata de un conjunto de datos, debe emitir la siguiente consulta:

    curl 'http://localhost:8080/aspects/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Afoo%2Cbar%2CPROD)?aspect=schemaMetadata&version=0'

    {
       "version":0,
       "aspect":{
          "com.linkedin.schema.SchemaMetadata":{
             "created":{
                "actor":"urn:li:corpuser:fbar",
                "time":0
             },
             "platformSchema":{
                "com.linkedin.schema.KafkaSchema":{
                   "documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
                }
             },
             "lastModified":{
                "actor":"urn:li:corpuser:fbar",
                "time":0
             },
             "schemaName":"FooEvent",
             "fields":[
                {
                   "fieldPath":"foo",
                   "description":"Bar",
                   "type":{
                      "type":{
                         "com.linkedin.schema.StringType":{
                            
                         }
                      }
                   },
                   "nativeDataType":"string"
                }
             ],
             "version":0,
             "hash":"",
             "platform":"urn:li:dataPlatform:foo"
          }
       }
    }

#### Aspectos de Fetching Timeseries

DataHub admite una API para obtener un grupo de aspectos de series temporales sobre una entidad. Por ejemplo, es posible que desee utilizar esta API
para obtener ejecuciones de perfiles recientes y estadísticas sobre un conjunto de datos. Para hacerlo, puede emitir una solicitud de "obtener" contra el `/aspects` Extremo.

Por ejemplo, para obtener perfiles de conjunto de datos (es decir, estadísticas) para un conjunto de datos, debe emitir la siguiente consulta:

    curl -X POST 'http://localhost:8080/aspects?action=getTimeseriesAspectValues' \
    --data '{
        "urn": "urn:li:dataset:(urn:li:dataPlatform:redshift,global_dev.larxynx_carcinoma_data_2020,PROD)",
        "entity": "dataset",
        "aspect": "datasetProfile",
        "startTimeMillis": 1625122800000,
        "endTimeMillis": 1627455600000
    }'

    {
       "value":{
          "limit":10000,
          "aspectName":"datasetProfile",
          "endTimeMillis":1627455600000,
          "startTimeMillis":1625122800000,
          "entityName":"dataset",
          "values":[
             {
                "aspect":{
                   "value":"{\"timestampMillis\":1626912000000,\"fieldProfiles\":[{\"uniqueProportion\":1.0,\"sampleValues\":[\"123MMKK12\",\"13KDFMKML\",\"123NNJJJL\"],\"fieldPath\":\"id\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":3742},{\"uniqueProportion\":1.0,\"min\":\"1524406400000\",\"max\":\"1624406400000\",\"sampleValues\":[\"1640023230002\",\"1640343012207\",\"16303412330117\"],\"mean\":\"1555406400000\",\"fieldPath\":\"date\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":3742},{\"uniqueProportion\":0.037,\"min\":\"21\",\"median\":\"68\",\"max\":\"92\",\"sampleValues\":[\"45\",\"65\",\"81\"],\"mean\":\"65\",\"distinctValueFrequencies\":[{\"value\":\"12\",\"frequency\":103},{\"value\":\"54\",\"frequency\":12}],\"fieldPath\":\"patient_age\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":79},{\"uniqueProportion\":0.00820873786407767,\"sampleValues\":[\"male\",\"female\"],\"fieldPath\":\"patient_gender\",\"nullCount\":120,\"nullProportion\":0.03,\"uniqueCount\":2}],\"rowCount\":3742,\"columnCount\":4}",
                   "contentType":"application/json"
                }
             },
          ]
       }
    }

Notarás que el aspecto en sí está serializado como JSON escapado. Esto es parte de un cambio hacia un conjunto más genérico de API de LECTURA / ESCRITURA
que permiten la serialización de aspectos de diferentes maneras. De forma predeterminada, el tipo de contenido será JSON y el aspecto se puede deserializar en un objeto JSON normal
en el idioma de su elección. Tenga en cuenta que esto pronto se convertirá en la forma de facto de escribir y leer aspectos individuales.

### Consulta de búsqueda

Una consulta de búsqueda permite buscar entidades que coincidan con una cadena arbitraria.

Por ejemplo, para buscar entidades que coincidan con el término "clientes", podemos utilizar el siguiente CURL:

    curl --location --request POST 'http://localhost:8080/entities?action=search' \                           
    --header 'X-RestLi-Protocol-Version: 2.0.0' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "input": "\"customers\"",
        "entity": "chart",
        "start": 0,
        "count": 10
    }'

Los parámetros notables son `input` y `entity`. `input` especifica la consulta que estamos emitiendo y `entity` especifica el tipo de entidad que queremos buscar. Este es el nombre común de la Entidad tal como se define en la definición de @Entity. La respuesta contiene una lista de urnas, que se pueden usar para obtener la entidad completa.

### Consulta de relación

Una consulta de relación le permite encontrar Entidad conectada a una Entidad de origen particular a través de un borde de un tipo determinado.

Por ejemplo, para encontrar los propietarios de un gráfico en particular, podemos usar el siguiente CURL:

    curl --location --request GET --header 'X-RestLi-Protocol-Version: 2.0.0' 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Achart%3Acustomers&types=List(OwnedBy)'

Los parámetros notables son `direction`, `urn` y `types`. La respuesta contiene *Urnas* asociado a todas las entidades conectadas
a la entidad principal (urn:li:chart:customer) mediante una relación denominada "OwnedBy". Es decir, permite buscar a los propietarios de un determinado
gráfico.

### Aspectos especiales

Hay algunos aspectos especiales que vale la pena mencionar:

1.  Aspectos clave: contiene las propiedades que identifican de forma única a una entidad.
2.  Aspecto Examinar trazados: representa un trazado jerárquico asociado a una entidad.

#### Aspectos clave

Como se introdujo anteriormente, los aspectos clave son estructuras / registros que contienen los campos que identifican de forma única a una entidad. Hay
algunas restricciones sobre los campos que pueden estar presentes en Aspectos clave:

*   Todos los campos deben ser de tipo STRING o ENUM
*   Todos los campos deben ser OBLIGATORIOS

Las claves se pueden crear y convertir en *Urnas*, que representan la versión encadenada del registro Key.
El algoritmo utilizado para realizar la conversión es sencillo: los campos del aspecto Clave se sustituyen en un
plantilla de cadena basada en su índice (orden de definición) utilizando la siguiente plantilla:

```aidl
// Case 1: # key fields == 1
urn:li:<entity-name>:key-field-1

// Case 2: # key fields > 1
urn:li:<entity-name>:(key-field-1, key-field-2, ... key-field-n) 
```

Por convención, los aspectos clave se definen en [metadata-models/src/main/pegasus/com/linkedin/metadata/key](https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/metadata/key).

##### Ejemplo

Un CorpUser puede ser identificado de forma única por un "nombre de usuario", que normalmente debe corresponder a un nombre LDAP.

Por lo tanto, su aspecto clave se define como el siguiente:

```aidl
namespace com.linkedin.metadata.key

/**
 * Key for a CorpUser
 */
@Aspect = {
  "name": "corpUserKey"
}
record CorpUserKey {
  /**
  * The name of the AD/LDAP user.
  */
  username: string
}
```

y su modelo de instantánea de entidad se define como

```aidl
/**
 * A metadata snapshot for a specific CorpUser entity.
 */
@Entity = {
  "name": "corpuser",
  "keyAspect": "corpUserKey"
}
record CorpUserSnapshot {

  /**
   * URN for the entity the metadata snapshot is associated with.
   */
  urn: CorpuserUrn

  /**
   * The list of metadata aspects associated with the CorpUser. Depending on the use case, this can either be all, or a selection, of supported aspects.
   */
  aspects: array[CorpUserAspect]
}
```

Utilizando una combinación de la información proporcionada por estos modelos, podemos generar la Urna correspondiente a un CorpUser como

    urn:li:corpuser:<username>

Imagina que tenemos una Entidad CorpUser con el nombre de usuario "johnsmith". En este mundo, la versión JSON del Aspecto Clave asociado a la Entidad sería

```aidl
{
  "username": "johnsmith"
}
```

y su correspondiente Urna sería

```aidl
urn:li:corpuser:johnsmith 
```

#### Aspecto BrowsePaths

El aspecto BrowsePaths le permite definir una "ruta de exploración" personalizada para una entidad. Una ruta de exploración es una forma de organizar jerárquicamente
Entidades. Se manifiestan dentro de las características "Explorar" en la interfaz de usuario, lo que permite a los usuarios navegar a través de árboles de entidades relacionadas de un tipo determinado.

Para admitir la exploración de una entidad en particular, agregue el aspecto "browsePaths" a la entidad en su `entity-registry.yml` archivo.

```aidl
/// entity-registry.yml 
entities:
  - name: dataset
    doc: Datasets represent logical or physical data assets stored or represented in various data platforms. Tables, Views, Streams are all instances of datasets.
    keyAspect: datasetKey
    aspects:
      ...
      - browsePaths
```

Al declarar este aspecto, puede producir rutas de exploración personalizadas, así como consultar rutas de exploración manualmente utilizando un CURL como el siguiente:

```aidl
curl --location --request POST 'http://localhost:8080/entities?action=browse' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
    "path": "/my/custom/browse/path",
    "entity": "dataset",
    "start": 0,
    "limit": 10
}'
```

Tenga en cuenta que debe proporcionar:

*   Ruta de acceso raíz delimitada por "/" para la que se van a obtener resultados.
*   Una entidad "type" usando su nombre común ("dataset" en el ejemplo anterior).

### Tipos de aspecto

Hay 2 "tipos" de aspectos de metadatos. Ambos se modelan utilizando esquemas PDL, y ambos se pueden ingerir de la misma manera.
Sin embargo, difieren en lo que representan y cómo son manejados por el Servicio de Metadatos de DataHub.

#### 1. Aspectos versionados

Cada uno de los aspectos versionados tiene un **versión numérica** asociado a ellos. Cuando un campo de un aspecto cambia, un nuevo
la versión se crea y almacena automáticamente en el backend de DataHub. En la práctica, todos los aspectos versionados se almacenan dentro de una base de datos relacional
que se puede respaldar y restaurar. Los aspectos versionados potencian gran parte de la experiencia de la interfaz de usuario a la que está acostumbrado, incluida la propiedad, las descripciones,
Etiquetas, términos del glosario y más. Los ejemplos incluyen propiedad, etiquetas globales y términos del glosario.

#### 2. Aspectos de la serie temporal

Los aspectos de la serie de tiempo tienen cada uno un **Timestamp** asociado a ellos. Son útiles para representar
eventos ordenados por tiempo sobre una entidad. Por ejemplo, los resultados de la generación de perfiles de un conjunto de datos o un conjunto de comprobaciones de calidad de datos que
correr todos los días. Es importante tener en cuenta que los aspectos de la relojería NO persisten dentro de la tienda relacional, y en su lugar se conservan.
persistió solo en el índice de búsqueda (por ejemplo, elasticsearch) y la cola de mensajes (Kafka). Esto hace que la restauración de aspectos de series temporales
en un escenario de desastre un poco más desafiante. Los aspectos de las series temporales se pueden consultar por rango de tiempo, que es lo que los hace más diferentes de los aspectos versionados.
Un aspecto de la serie temporal se puede identificar por las "series temporales" [tipo](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProfile.pdl#L10) en su [@Aspect](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProfile.pdl#L8) anotación.
Algunos ejemplos son: [DatasetProfile](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProfile.pdl) & [DatasetUsageStatistics](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetUsageStatistics.pdl).

Los aspectos de series temporales son aspectos que tienen un campo de marca de tiempoMillis, y están destinados a aspectos que cambian continuamente en un
base oportuna, por ejemplo, perfiles de datos, estadísticas de uso, etc.

Cada aspecto de la serie de tiempo debe declararse "tipo": "series" y debe
incluír [TimeeriesAspectBase](https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/timeseries/TimeseriesAspectBase.pdl)
, que contiene un campoMillis con marca de tiempo.

El aspecto de series temporales no puede tener ningún campo que tenga la @Searchable o @Relationship anotación, ya que pasa por un
flujo completamente diferente.

Consulte
Para [DatasetProfile](https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProfile.pdl)
para ver un ejemplo de un aspecto de series temporales.

Debido a que los aspectos de las series temporales se actualizan con frecuencia, las ingestas de estos aspectos van directamente a la búsqueda elástica (
en lugar de almacenarse en la base de datos local).

Puede recuperar aspectos de series temporales mediante el punto final "aspects?action=getTimeseriesAspectValues".

##### Aspectos de la serie de tiempo agregables

Ser capaz de realizar SQL como *agrupar por + agregado* Las operaciones en los aspectos de la serie temporal es un caso de uso muy natural para
este tipo de datos (perfiles de conjuntos de datos, estadísticas de uso, etc.). En esta sección se describe cómo definir, ingerir y realizar un
consulta de agregación en un aspecto de series temporales.

###### Definición de un nuevo aspecto de timeseries agregable.

El *@TimeseriesField* y el *@TimeseriesFieldCollection* son dos nuevas anotaciones que se pueden adjuntar a un campo de
un *Aspecto de la serie temporal* que le permite formar parte de una consulta agregable. Los tipos de agregaciones permitidas en estos
los campos anotados dependen del tipo de campo, así como del tipo de agregación, como
Descrito [aquí](#Performing-an-aggregation-on-a-Timeseries-aspect).

*   `@TimeseriesField = {}` - Esta anotación se puede utilizar con cualquier tipo de campo de tipo no coleccionario del aspecto, como
    tipos y registros primitivos (ver los campos *Estadísticas*, *strStat* y *strArray* Campos
    de [TestEntityProfile.pdl](https://github.com/datahub-project/datahub/blob/master/test-models/src/main/pegasus/com/datahub/test/TestEntityProfile.pdl)).

*   El `@TimeseriesFieldCollection {"key":"<name of the key field of collection item type>"}` la anotación permite
    Compatibilidad con la agregación en los elementos de un tipo de colección (solo se admite para las colecciones de tipos de matriz por ahora), donde el
    valor de `"key"` es el nombre del campo del tipo de elemento de colección que se utilizará para especificar la cláusula group-by (
    ver *userCounts* y *fieldContables* campos de [DatasetUsageStatistics.pdl](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetUsageStatistics.pdl)).

Además de definir el nuevo aspecto con las anotaciones apropiadas de Timeseries,
el [entity-registry.yml](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/resources/entity-registry.yml)
el archivo también debe actualizarse. Simplemente agregue el nuevo nombre de aspecto debajo de la lista de aspectos contra la entidad apropiada como se muestra a continuación, como `datasetUsageStatistics` para el aspecto DatasetUsageStatistics.

```yaml
entities:
  - name: dataset
    keyAspect: datasetKey
    aspects:
      - datasetProfile
      - datasetUsageStatistics
```

###### Ingerir un aspecto de Timeseries

Los aspectos de la serie temporal se pueden ingerir a través del punto final REST de GMS `/aspects?action=ingestProposal` o a través de la API de Python.

Ejemplo1: A través de la API REST de GMS usando curl.

```shell
curl --location --request POST 'http://localhost:8080/aspects?action=ingestProposal' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
  "proposal" : {
    "entityType": "dataset",
    "entityUrn" : "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
    "changeType" : "UPSERT",
    "aspectName" : "datasetUsageStatistics",
    "aspect" : {
      "value" : "{ \"timestampMillis\":1629840771000,\"uniqueUserCount\" : 10, \"totalSqlQueries\": 20, \"fieldCounts\": [ {\"fieldPath\": \"col1\", \"count\": 20}, {\"fieldPath\" : \"col2\", \"count\": 5} ]}",
      "contentType": "application/json"
    }
  }
}'
```

Ejemplo2: A través de la API de Python a Kafka (o REST)

```python
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
)
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
from datahub.emitter.rest_emitter import DatahubRestEmitter

usageStats = DatasetUsageStatisticsClass(
            timestampMillis=1629840771000,
            uniqueUserCount=10,
            totalSqlQueries=20,
            fieldCounts=[
                DatasetFieldUsageCountsClass(
                    fieldPath="col1",
                    count=10
                )
            ]
        )

mcpw = MetadataChangeProposalWrapper(
    entityType="dataset",
    aspectName="datasetUsageStatistics",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
    aspect=usageStats,
)

# Instantiate appropriate emitter (kafk_emitter/rest_emitter)
my_emitter = DatahubKafkaEmitter("""<config>""")
my_emitter.emit(mcpw)
```

###### Realizar una agregación en un aspecto de Timeseries.

Las agregaciones en aspectos de series temporales pueden ser realizadas por la API REST de GMS para `/analytics?action=getTimeseriesStats` cuál
acepta los siguientes parámetros.

*   `entityName` - El nombre de la entidad a la que está asociado el aspecto.
*   `aspectName` - El nombre del aspecto.
*   `filter` - Se realizan los criterios de prefiltrado antes de agrupar y agregar.
*   `metrics` - Una lista de especificaciones de agregación. El `fieldPath` miembro de una especificación de agregación se refiere a la
    nombre del campo con el que debe realizarse la agregación y el `aggregationType` especifica el tipo de agregación.
*   `buckets` - Una lista de especificaciones de agrupación de cucharones. Cada bucket de agrupación tiene un `key` campo que hace referencia al campo
    para usar para agrupar. El `type` especifica el tipo de bucket de agrupación.

Admitimos tres tipos de agregaciones que se pueden especificar en una consulta de agregación en los campos anotados de Timeseries.
Los valores que `aggregationType` pueden tomar son:

*   `LATEST`: el valor más reciente del campo en cada bucket. Compatible con cualquier tipo de campo.
*   `SUM`: La suma acumulativa del campo en cada bucket. Compatible solo con tipos integrales.
*   `CARDINALITY`: El número de valores únicos o la cardinalidad del conjunto en cada bucket. Compatible con cadenas y
    tipos de registro.

Admitimos dos tipos de agrupación para definir los buckets contra los que se realizarán agregaciones:

*   `DATE_GROUPING_BUCKET`: Permite crear buckets basados en el tiempo, como por segundo, minuto, hora, día, semana, mes,
    trimestre, año, etc. Debe utilizarse junto con un campo de marca de tiempo cuyo valor esté en milisegundos ya que *época*.
    El `timeWindowSize` param especifica la fecha de ancho del bucket del histograma.
*   `STRING_GROUPING_BUCKET`: Permite crear buckets agrupados por los valores únicos de un campo. Siempre debe usarse en
    junto con un campo de tipo cadena.

La API devuelve una tabla genérica similar a SQL como `table` miembro del resultado que contiene los resultados de
el `group-by/aggregate` query, además de hacer eco de los parámetros de entrada.

*   `columnNames`: los nombres de las columnas de la tabla. El grupo-por `key` Los nombres aparecen en el mismo orden en que se especifican
    en la solicitud. Las especificaciones de agregación siguen los campos de agrupación en el mismo orden que se especifica en la solicitud,
    y será nombrado `<agg_name>_<fieldPath>`.
*   `columnTypes`: los tipos de datos de las columnas.
*   `rows`: los valores de los datos, cada fila correspondiente al bucket o buckets respectivos.

Ejemplo: Último recuento de usuarios únicos para cada día.

```shell
# QUERY
curl --location --request POST 'http://localhost:8080/analytics?action=getTimeseriesStats' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
    "entityName": "dataset",
    "aspectName": "datasetUsageStatistics",
    "filter": {
        "criteria": []
    },
    "metrics": [
        {
            "fieldPath": "uniqueUserCount",
            "aggregationType": "LATEST"
        }
    ],
    "buckets": [
        {
            "key": "timestampMillis",
            "type": "DATE_GROUPING_BUCKET",
            "timeWindowSize": {
                "multiple": 1,
                "unit": "DAY"
            }
        }
    ]
}'

# SAMPLE RESPOSNE
{
    "value": {
        "filter": {
            "criteria": []
        },
        "aspectName": "datasetUsageStatistics",
        "entityName": "dataset",
        "groupingBuckets": [
            {
                "type": "DATE_GROUPING_BUCKET",
                "timeWindowSize": {
                    "multiple": 1,
                    "unit": "DAY"
                },
                "key": "timestampMillis"
            }
        ],
        "aggregationSpecs": [
            {
                "fieldPath": "uniqueUserCount",
                "aggregationType": "LATEST"
            }
        ],
        "table": {
            "columnNames": [
                "timestampMillis",
                "latest_uniqueUserCount"
            ],
            "rows": [
                [
                    "1631491200000",
                    "1"
                ]
            ],
            "columnTypes": [
                "long",
                "int"
            ]
        }
    }
}
```

Para obtener más ejemplos sobre los tipos complejos de agrupación por agregaciones, consulte las pruebas del grupo `getAggregatedStats` de [ElasticSearchTimeseriesAspectServiceTest.java](https://github.com/datahub-project/datahub/blob/master/metadata-io/src/test/java/com/linkedin/metadata/timeseries/elastic/ElasticSearchTimeseriesAspectServiceTest.java).
