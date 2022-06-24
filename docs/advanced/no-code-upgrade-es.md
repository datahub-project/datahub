# Sin actualización de código (Guía de migración local)

## Resumen de los cambios

Con la iniciativa de metadatos Sin código, hemos introducido varios cambios importantes:

1.  Nueva tabla Ebean Aspect (metadata_aspect_v2)
2.  Nuevos índices elásticos (*entityName*index_v2)
3.  Nuevos triples de borde. (Elimine las rutas de clase completamente calificadas de los nodos y bordes)
4.  Entidades dinámicas de DataPlatform (no más DataPlatformInfo.json codificado)
5.  Rutas de exploración dinámicas (no más lógica de creación de rutas de exploración codificada)
6.  Adición de aspectos de clave de entidad, requisito eliminado para urnas fuertemente tipadas.
7.  Adición de @Entity, @Aspect, @Searchable @Relationship anotaciones a los modelos existentes.

Debido a estos cambios, es necesario migrar la capa de persistencia después de que se hayan migrado los contenedores NoCode
Desplegado.

Para obtener más información acerca de la actualización sin código, consulte [modelado sin código](./no-code-modeling.md).

## Estrategia de migración

Estamos fusionando estos cambios de ruptura en la rama principal por adelantado porque creemos que son fundamentales para la posterior
cambios, proporcionando una base más sólida sobre la cual se construirán nuevas y emocionantes características. Seguiremos haciéndolo
ofrecen soporte limitado para versiones anteriores de DataHub.

Este enfoque significa que las empresas que implementen activamente la última versión de DataHub deberán realizar una actualización a
continúe operando DataHub sin problemas.

## Pasos de actualización

### Paso 1: Extraiga e implemente las últimas imágenes de contenedores

Es importante que los siguientes contenedores se extraigan y desplieguen simultáneamente:

*   datahub-frontend-react
*   datahub-gms
*   datahub-mae-consumer
*   datahub-mce-consumidor

#### Implementaciones de Docker Compose

Del `docker` directorio:

```aidl
docker-compose down --remove-orphans && docker-compose pull && docker-compose -p datahub up --force-recreate
```

#### Timón

La implementación de los gráficos de timón más recientes actualizará todos los componentes a la versión 0.8.0. Una vez que todas las cápsulas estén en funcionamiento,
ejecute el trabajo de actualización de datahub, que ejecutará el contenedor de Docker anterior para migrar a los nuevos orígenes.

### Paso 2: Ejecutar el trabajo de migración

#### Implementaciones de Docker Compose - Conservar datos

Si no le importa migrar sus datos, puede consultar Docker Compose Deployments - Lose All Existing Data
sección a continuación.

Para migrar los datos existentes, la opción más fácil es ejecutar el `run_upgrade.sh` script ubicado en `docker/datahub-upgrade/nocode`.

    cd docker/datahub-upgrade/nocode
    ./run_upgrade.sh

Con este comando, se utilizarán las variables de entorno predeterminadas (`docker/datahub-upgrade/env/docker.env`). Estos asumen
que su implementación es local y que está ejecutando MySQL. Si este no es el caso, deberá definir sus propias variables de entorno para indicar el
actualizar el sistema donde residen y se ejecutan los contenedores de DataHub

Para actualizar las variables de entorno predeterminadas, puede

1.  Cambio `docker/datahub-upgrade/env/docker.env` en su lugar y, a continuación, ejecute uno de los comandos anteriores O
2.  Defina un nuevo archivo ".env" que contenga sus variables y ejecútelo `docker pull acryldata/datahub-upgrade && docker run acryldata/datahub-upgrade:latest -u NoCodeDataMigration`

Para ver las variables de entorno necesarias, consulte el [datahub-upgrade](../../docker/datahub-upgrade/README.md)
documentación.

Para ejecutar la actualización en una base de datos que no sea MySQL, puede utilizar el `-a dbType=<db-type>` argumento.

Ejecutar

    ./docker/datahub-upgrade.sh -u NoCodeDataMigration -a dbType=POSTGRES

donde dbType puede ser: `MYSQL`, `MARIA`, `POSTGRES`.

#### Implementaciones de Docker Compose: pierda todos los datos existentes

Esta ruta es la más rápida, pero borrará la base de datos de su DataHub.

Si desea asegurarse de que sus datos actuales se migran, consulte la sección Anterior de Docker Compose Deployments - Preserve Data.
Si está de acuerdo en perder sus datos y volver a ingerir, este enfoque es el más simple.

    # make sure you are on the latest
    git checkout master
    git pull origin master

    # wipe all your existing data and turn off all processes
    ./docker/nuke.sh

    # spin up latest datahub
    ./docker/quickstart.sh

    # re-ingest data, for example, to ingest sample data:
    ./docker/ingestion/ingestion.sh

Después de eso, estarás listo para comenzar.

##### Cómo solucionar el problema "escuchar el puerto 5005"

Se han publicado soluciones para este problema en la etiqueta acryldata/datahub-upgrade:head. Por favor, extraiga el último maestro y vuelva a ejecutar
el script de actualización.

Sin embargo, hemos visto casos en los que la imagen de Docker problemática se almacena en caché y Docker no extrae la última versión. Si
La secuencia de comandos falla con el mismo error después de extraer el maestro más reciente, ejecute el siguiente comando para borrar la ventana acoplable
caché de imágenes.

    docker images -a | grep acryldata/datahub-upgrade | awk '{print $3}' | xargs docker rmi -f

#### Implementaciones de Helm

Actualice a los gráficos de timón más recientes ejecutando lo siguiente después de extraer el último maestro.

```(shell)
helm upgrade datahub datahub/
```

En los últimos gráficos de helm, agregamos un datahub-upgrade-job, que ejecuta el contenedor docker mencionado anteriormente para migrar a
la nueva capa de almacenamiento. Tenga en cuenta que el trabajo fallará al principio mientras espera que gms y MAE consumer se implementen con
el código NoCode. Se volverá a ejecutar hasta que se ejecute correctamente.

Una vez que se haya migrado la capa de almacenamiento, las ejecuciones posteriores de este trabajo serán un noop.

### Paso 3 (Opcional): Limpieza

Advertencia: Este paso borra todos los metadatos heredados. Si algo está mal con los metadatos actualizados, no habrá una manera fácil de
vuelva a ejecutar la migración.

Este paso implica eliminar datos de versiones anteriores de DataHub. Este paso solo debe realizarse una vez que haya
validó que la implementación de DataHub está en buen estado después de realizar la actualización. Si puedes buscar, navegar y
ver sus metadatos después de que se hayan completado los pasos de actualización, debe estar en buena forma.

En implementaciones avanzadas de DataHub, o en casos en los que no puede reconstruir fácilmente el estado almacenado en DataHub, es muy importante
se le aconseja que realice la debida diligencia antes de ejecutar la limpieza. Esto puede implicar la inspección manual de la relación
tablas (metadata_aspect_v2), índices de búsqueda y topología de gráficos.

#### Implementaciones de Docker Compose

La opción más fácil es ejecutar el `run_clean.sh` script ubicado en `docker/datahub-upgrade/nocode`.

    cd docker/datahub-upgrade/nocode
    ./run_clean.sh

Con este comando, se utilizarán las variables de entorno predeterminadas (`docker/datahub-upgrade/env/docker.env`). Estos asumen
que la implementación es local. Si este no es el caso, deberá definir sus propias variables de entorno para indicar el
actualizar el sistema donde residen los contenedores de DataHub.

Para actualizar las variables de entorno predeterminadas, puede

1.  Cambio `docker/datahub-upgrade/env/docker.env` en su lugar y, a continuación, ejecute uno de los comandos anteriores O
2.  Defina un nuevo archivo ".env" que contenga sus variables y ejecútelo
    `docker pull acryldata/datahub-upgrade && docker run acryldata/datahub-upgrade:latest -u NoCodeDataMigrationCleanup`

Para ver las variables de entorno necesarias, consulte el [datahub-upgrade](../../docker/datahub-upgrade/README.md)
documentación

#### Implementaciones de Helm

Suponiendo que el último gráfico de timón se haya implementado en el paso anterior, datahub-cleanup-job-template cronJob debería tener
ha sido creado. Puede comprobarlo ejecutando lo siguiente:

    kubectl get cronjobs

Debería ver una salida como la siguiente:

    NAME                                   SCHEDULE     SUSPEND   ACTIVE   LAST SCHEDULE   AGE
    datahub-datahub-cleanup-job-template   * * * * *    True      0        <none>          12m

Tenga en cuenta que el cronJob ha sido suspendido. Está destinado a ser ejecutado de manera ad hoc cuando esté listo para limpiar. Asegúrate
la migración se realizó correctamente y DataHub funciona como se esperaba. A continuación, ejecute el siguiente comando para ejecutar el trabajo de limpieza:

    kubectl create job --from=cronjob/<<release-name>>-datahub-cleanup-job-template datahub-cleanup-job

Reemplace release-name por el nombre de la release de helm. Si siguió la guía de kubernetes, debería ser "datahub".

## Apoyo

El equipo de Acryl estará en espera para ayudarle en su migración. Por favor
juntar [#release-0\_8\_0](https://datahubspace.slack.com/archives/C0244FHMHJQ) canalizar y comunicarse con nosotros si encuentra
problemas con la actualización o tener comentarios sobre el proceso. Trabajaremos estrechamente para asegurarnos de que pueda continuar operando
DataHub sin problemas.
