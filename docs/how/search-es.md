# Guía de búsqueda

## Introducción

La barra de búsqueda es uno de los medios para encontrar datos en Datahub. En este documento, discutimos formas más efectivas de encontrar información más allá de hacer una búsqueda estándar de palabras clave. Esto se debe a que las búsquedas de palabras clave pueden devolver resultados de casi cualquier parte de una entidad.

### Buscar en campos específicos

Los siguientes ejemplos tienen el formato de\
X: *pregunta típica* :\
`what to key in search bar`.  [URL de ejemplo](https://example.com)\
También se pueden agregar caracteres comodín a los términos de búsqueda. Estos ejemplos no son exhaustivos y utilizan Datasets como referencia.

Quiero:

1.  *Buscar un conjunto de datos con la palabra **máscara** en el nombre* :\
    `name: *mask*` [Resultados de la muestra](https://demo.datahubproject.io/search?page=1\&query=name%3A%20%2Amask%2A)\
    Esto devolverá entidades con **máscara** en el nombre.\
    Los nombres tienden a estar conectados por otros símbolos, de ahí los símbolos comodín antes y después de la palabra.

2.  *Buscar un conjunto de datos con una propiedad, **codificación***\
    `customProperties: encoding*` [Resultados de la muestra](https://demo.datahubproject.io/search?page=1\&query=customProperties%3A%20encoding%2A)\
    Las propiedades del conjunto de datos se indexan en ElasticSearch a la manera de key=value. Por lo tanto, si conoce el par clave-valor preciso, puede buscar usando `key=value`. Sin embargo, si solo conoce la clave, puede usar comodines para reemplazar el valor y eso es lo que se está haciendo aquí.

3.  *Buscar un conjunto de datos con un nombre de columna, **latitud***\
    `fieldPaths: latitude` [Resultados de la muestra](https://demo.datahubproject.io/search?page=1\&query=fieldPaths%3A%20latitude)\
    fieldPaths es el nombre del atributo que contiene el nombre de columna en Datasets.

4.  *Buscar un conjunto de datos con el término **latitud** en la descripción del campo*\
    `editedFieldDescriptions: latitude OR fieldDescriptions: latitude`  [Resultados de la muestra](https://demo.datahubproject.io/search?page=1\&query=editedFieldDescriptions%3A%20latitude%20OR%20fieldDescriptions%3A%20latitude)\
    Datasets tiene 2 atributos que contienen la descripción del campo. fieldDescription proviene del aspecto SchemaMetadata, mientras que editedFieldDescriptions proviene del aspecto EditableSchemaMetadata. EditableSchemaMetadata contiene información que proviene de las ediciones de la interfaz de usuario, mientras que SchemaMetadata contiene datos de la ingesta del conjunto de datos.

5.  *Buscar un conjunto de datos con el término **lógico** en la descripción del conjunto de datos*\
    `editedDescription: *logical* OR description: *logical*` [Resultados de la muestra](https://demo.datahubproject.io/search?page=1\&query=editedDescription%3A%20%2Alogical%2A%20OR%20description%3A%20%2Alogical%2A)\
    Al igual que las descripciones de campo, las descripciones de conjuntos de datos se pueden encontrar en 2 aspectos, de ahí la necesidad de buscar 2 atributos.

6.  *Busque un conjunto de datos que resida en una de las carpetas de exploración, por ejemplo, el **colmena** carpeta*\
    `browsePaths: *hive*` [Resultados de la muestra](https://demo.datahubproject.io/search?page=1\&query=browsePaths%3A%20%2Ahive%2A)\
    BrowsePath se almacena como una cadena completa, por ejemplo. `/datasets/prod/hive/SampleKafkaDataset`, de ahí la necesidad de comodines en ambos extremos del plazo para devolver un resultado.

## ¿Dónde encontrar más información?

Las consultas de ejemplo aquí no son exhaustivas. [El enlace aquí](https://demo.datahubproject.io/tag/urn:li:tag:Searchable) muestra la lista actual de campos indexados para cada entidad dentro de Datahub. Haga clic en los campos dentro de cada entidad y vea qué campo tiene la etiqueta `Searchable`.\
Sin embargo, no le indica el nombre de atributo específico que se debe usar para búsquedas especializadas. Una forma de hacerlo es inspeccionar los índices de ElasticSearch, por ejemplo:\
`curl http://localhost:9200/_cat/indices` devuelve todos los índices ES del contenedor de ElasticSearch.

    yellow open chartindex_v2_1643510690325                           bQO_RSiCSUiKJYsmJClsew 1 1   2 0   8.5kb   8.5kb
    yellow open mlmodelgroupindex_v2_1643510678529                    OjIy0wb7RyKqLz3uTENRHQ 1 1   0 0    208b    208b
    yellow open dataprocessindex_v2_1643510676831                     2w-IHpuiTUCs6e6gumpYHA 1 1   0 0    208b    208b
    yellow open corpgroupindex_v2_1643510673894                       O7myCFlqQWKNtgsldzBS6g 1 1   3 0  16.8kb  16.8kb
    yellow open corpuserindex_v2_1643510672335                        0rIe_uIQTjme5Wy61MFbaw 1 1   6 2  32.4kb  32.4kb
    yellow open datasetindex_v2_1643510688970                         bjBfUEswSoSqPi3BP4iqjw 1 1  15 0  29.2kb  29.2kb
    yellow open dataflowindex_v2_1643510681607                        N8CMlRFvQ42rnYMVDaQJ2g 1 1   1 0  10.2kb  10.2kb
    yellow open dataset_datasetusagestatisticsaspect_v1_1643510694706 kdqvqMYLRWq1oZt1pcAsXQ 1 1   4 0   8.9kb   8.9kb
    yellow open .ds-datahub_usage_event-000003                        YMVcU8sHTFilUwyI4CWJJg 1 1 186 0 203.9kb 203.9kb
    yellow open datajob_datahubingestioncheckpointaspect_v1           nTXJf7C1Q3GoaIJ71gONxw 1 1   0 0    208b    208b
    yellow open .ds-datahub_usage_event-000004                        XRFwisRPSJuSr6UVmmsCsg 1 1 196 0 165.5kb 165.5kb
    yellow open .ds-datahub_usage_event-000005                        d0O6l5wIRLOyG6iIfAISGw 1 1  77 0 108.1kb 108.1kb
    yellow open dataplatformindex_v2_1643510671426                    _4SIIhfAT8yq_WROufunXA 1 1   0 0    208b    208b
    yellow open mlmodeldeploymentindex_v2_1643510670629               n81eJIypSp2Qx-fpjZHgRw 1 1   0 0    208b    208b
    yellow open .ds-datahub_usage_event-000006                        oyrWKndjQ-a8Rt1IMD9aSA 1 1 143 0 127.1kb 127.1kb
    yellow open mlfeaturetableindex_v2_1643510677164                  iEXPt637S1OcilXpxPNYHw 1 1   5 0   8.9kb   8.9kb
    yellow open .ds-datahub_usage_event-000001                        S9EnGj64TEW8O3sLUb9I2Q 1 1 257 0 163.9kb 163.9kb
    yellow open .ds-datahub_usage_event-000002                        2xJyvKG_RYGwJOG9yq8pJw 1 1  44 0 155.4kb 155.4kb
    yellow open dataset_datasetprofileaspect_v1_1643510693373         uahwTHGRRAC7w1c2VqVy8g 1 1  31 0  18.9kb  18.9kb
    yellow open mlprimarykeyindex_v2_1643510687579                    MUcmT8ASSASzEpLL98vrWg 1 1   7 0   9.5kb   9.5kb
    yellow open glossarytermindex_v2_1643510686127                    cQL8Pg6uQeKfMly9GPhgFQ 1 1   3 0    10kb    10kb
    yellow open datajob_datahubingestionrunsummaryaspect_v1           rk22mIsDQ02-52MpWLm1DA 1 1   0 0    208b    208b
    yellow open mlmodelindex_v2_1643510675399                         gk-WSTVjRZmkDU5ggeFSqg 1 1   1 0  10.3kb  10.3kb
    yellow open dashboardindex_v2_1643510691686                       PQjSaGhTRqWW6zYjcqXo6Q 1 1   1 0   8.7kb   8.7kb
    yellow open datahubpolicyindex_v2_1643510671774                   ZyTrYx3-Q1e-7dYq1kn5Gg 1 1   0 0    208b    208b
    yellow open datajobindex_v2_1643510682977                         K-rbEyjBS6ew5uOQQS4sPw 1 1   2 0  11.3kb  11.3kb
    yellow open datahubretentionindex_v2                              8XrQTPwRTX278mx1SrNwZA 1 1   0 0    208b    208b
    yellow open glossarynodeindex_v2_1643510678826                    Y3_bCz0YR2KPwCrrVngDdA 1 1   1 0   7.4kb   7.4kb
    yellow open system_metadata_service_v1                            36spEDbDTdKgVlSjE8t-Jw 1 1 387 8  63.2kb  63.2kb
    yellow open schemafieldindex_v2_1643510684410                     tZ1gC3haTReRLmpCxirVxQ 1 1   0 0    208b    208b
    yellow open mlfeatureindex_v2_1643510680246                       aQO5HF0mT62Znn-oIWBC8A 1 1  20 0  17.4kb  17.4kb
    yellow open tagindex_v2_1643510684785                             PfnUdCUORY2fnF3I3W7HwA 1 1   3 1  18.6kb  18.6kb

El nombre del índice variará de una instancia a otra. La información indexada sobre los conjuntos de datos se puede encontrar en:\
`curl http://localhost:9200/datasetindex_v2_1643510688970/_search?=pretty`

información de ejemplo de un conjunto de datos:

    {
            "_index" : "datasetindex_v2_1643510688970",
            "_type" : "_doc",
            "_id" : "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Akafka%2CSampleKafkaDataset%2CPROD%29",
            "_score" : 1.0,
            "_source" : {
              "urn" : "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
              "name" : "SampleKafkaDataset",
              "browsePaths" : [
                "/prod/kafka/SampleKafkaDataset"
              ],
              "origin" : "PROD",
              "customProperties" : [
                "prop2=pikachu",
                "prop1=fakeprop"
              ],
              "hasDescription" : false,
              "hasOwners" : true,
              "owners" : [
                "urn:li:corpuser:jdoe",
                "urn:li:corpuser:datahub"
              ],
              "fieldPaths" : [
                "[version=2.0].[type=boolean].field_foo_2",
                "[version=2.0].[type=boolean].field_bar",
                "[version=2.0].[key=True].[type=int].id"
              ],
              "fieldGlossaryTerms" : [ ],
              "fieldDescriptions" : [
                "Foo field description",
                "Bar field description",
                "Id specifying which partition the message should go to"
              ],
              "fieldTags" : [
                "urn:li:tag:NeedsDocumentation"
              ],
              "platform" : "urn:li:dataPlatform:kafka"
            }
          },
