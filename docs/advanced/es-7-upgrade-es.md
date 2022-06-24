# Actualización de Elasticsearch de 5.6.8 a 7.9.3

## Resumen de los cambios

Echa un vistazo a la lista de cambios importantes para [Elasticsearch 6](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/breaking-changes-6.0.html) y [Elasticsearch 7](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.0.html). A continuación se muestra el resumen de los cambios que afectan a Datahub.

### Mapeo y configuración del índice de búsqueda

*   Eliminación de tipos de mapeo (como se mencionó) [aquí](https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html))
*   Especifique la diferencia máxima permitida entre `min_gram` y `max_gram` para NGramTokenizer y NGramTokenFilter agregando propiedades `max_ngram_diff` en la configuración del índice, especialmente si la diferencia es mayor que 1 (como se ha mencionado) [aquí](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html))

### Consulta de búsqueda

Los siguientes parámetros son/fueron `optional` y, por lo tanto, se rellena automáticamente en la consulta de búsqueda. Algunas pruebas que esperan que se envíe una determinada consulta de búsqueda a ES cambiarán con la actualización de ES.

*   `disable_coord` parámetro de la `bool` y `common_terms` se han eliminado las consultas (como se mencionó) [aquí](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/breaking-changes-6.0.html))
*   `auto_generate_synonyms_phrase_query` parámetro en `match` La consulta se agrega con un valor predeterminado de `true` (como se mencionó [aquí](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-match-query.html))

### Cliente Java High Level Rest

*   En 7.9.3, la instancia de Java High Level Rest Client necesita que se construya un generador de clientes rest de bajo nivel REST. En 5.6.8, la misma instancia necesita un cliente de bajo nivel rest
*   Las API de documentos, como la API de índice, la API de eliminación, etc., ya no toman el documento `type` como entrada

## Estrategia de migración

Como se mencionó en los documentos, los índices creados en Elasticsearch 5.x no son legibles por Elasticsearch 7.x. Se producirá un error al ejecutar el contenedor de elasticsearch actualizado en el volumen de datos de esdata existente.

Para el desarrollo local, nuestra recomendación es ejecutar el `docker/nuke.sh` para quitar el volumen de datos de esdata existente antes de iniciar los contenedores. Tenga en cuenta que todos los datos se perderán.

Para migrar sin perder datos, consulte el script de Python y Dockerfile en `contrib/elasticsearch/es7-upgrade`. El script toma la URL del clúster de elasticsearch de origen y destino y la configuración SSL (si corresponde) como entrada. Porta las asignaciones y la configuración de todos los índices del clúster de origen al clúster de destino realizando los cambios necesarios indicados anteriormente. A continuación, transfiere todos los documentos del clúster de origen al clúster de destino.

Puede ejecutar el script en un contenedor docker de la siguiente manera

    docker build -t migrate-es-7 .
    docker run migrate-es-7 -s SOURCE -d DEST [--disable-source-ssl]
                       [--disable-dest-ssl] [--cert-file CERT_FILE]
                       [--key-file KEY_FILE] [--ca-file CA_FILE] [--create-only]
                       [-i INDICES] [--name-override NAME_OVERRIDE]

## Plan

Crearemos una rama "elasticsearch-5-legacy" con la versión de master anterior a la actualización de elasticsearch 7. Sin embargo, no apoyaremos esta rama en el futuro y todo el desarrollo futuro se realizará utilizando elasticsearch 7.9.3
