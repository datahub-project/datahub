# Restauración de índices de búsqueda y gráficos desde la base de datos local

Si los servicios de búsqueda o gráficos se caen o si ha realizado cambios en ellos que requieren reindexación, puede restaurarlos desde
los aspectos almacenados en la base de datos local.

Cuando se ingiere una nueva versión del aspecto, GMS inicia un evento MAE para el aspecto que se consume para actualizar
los índices de búsqueda y gráficos. Como tal, podemos obtener la última versión de cada aspecto en la base de datos local y producir
Eventos MAE correspondientes a los aspectos para restaurar los índices de búsqueda y gráfico.

## Docker-componer

Ejecute el siguiente comando desde la raíz para enviar MAE para cada aspecto de la base de datos local.

    ./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices

Si necesita borrar los índices de búsqueda y gráficos antes de restaurar, agregue `-a clean` al final del comando.

Consulte esto [Doc](../../docker/datahub-upgrade/README.md#environment-variables) Sobre cómo establecer variables de entorno
para su entorno.

## Kubernetes

Correr `kubectl get cronjobs` para ver si se ha implementado la plantilla de trabajo de restauración. Si ves resultados como los siguientes,
son buenos para ir.

    NAME                                          SCHEDULE    SUSPEND   ACTIVE   LAST SCHEDULE   AGE
    datahub-datahub-cleanup-job-template          * * * * *   True      0        <none>          2d3h
    datahub-datahub-restore-indices-job-template  * * * * *   True      0        <none>          2d3h

De lo contrario, implemente los gráficos de timón más recientes para usar esta funcionalidad.

Una vez implementada la plantilla de trabajo restore indices, ejecute el siguiente comando para iniciar un trabajo que restaure índices.

    kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc

Una vez que se complete el trabajo, sus índices se habrán restaurado.
