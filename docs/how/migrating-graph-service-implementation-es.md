# Migrar la implementación de Graph Service a Elasticsearch

Actualmente admitimos Elasticsearch o Neo4j como implementaciones de backend para el servicio de gráficos. Recomendamos
Elasticsearch para aquellos que buscan una implementación más ligera o no quieren administrar una base de datos Neo4j.
Si comenzó a usar Neo4j como backend del servicio de gráficos, así es como puede migrar a Elasticsearch.

## Docker-componer

Si está ejecutando su instancia a través de docker localmente, querrá activar su instancia de Datahub con
elasticsearch como backend. En un inicio limpio, esto sucede de forma predeterminada. Sin embargo, si ha escrito datos en
Neo4j debe solicitar explícitamente a DataHub que se inicie en modo elástico.

```aidl
datahub docker quickstart --graph-service-impl=elasticsearch
```

A continuación, ejecute el siguiente comando desde la raíz para reconstruir el índice del gráfico.

    ./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices

Una vez completado este comando, debe migrarse. Abra la interfaz de usuario de DataHub y verifique que sus relaciones sean
visible.

Una vez que confirme que la migración se realiza correctamente, debe eliminar el volumen neo4j ejecutando

```aidl
docker volume rm datahub_neo4jdata
```

Esto evita que su instancia de DataHub aparezca en modo neo4j en el futuro.

## Timón

Primero, ajuste las variables de timón para desactivar neo4j y configure su graph_service_impl en elasticsearch.

Para desactivar neo4j en el archivo de requisitos previos, establezca `neo4j-community`'s `enabled` propiedad a `false`
en este [valores.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml#L54).

A continuación, configura `graph_service_impl` Para `elasticsearch` En
[valores.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml#L63) de datahub.

Ver el [guía de timón de implementación](../deploy/kubernetes.md#components) para obtener más detalles sobre cómo
configurar la implementación del timón.

Finalmente, siga el [Guía de timón de restore-indices](./restore-indices.md) para reconstruir
su índice gráfico.

Una vez que se complete el trabajo, sus datos se migrarán.
