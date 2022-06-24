# Eliminación de metadatos de DataHub

Hay dos formas de eliminar metadatos de DataHub.

*   Eliminar metadatos adjuntos a entidades proporcionando una urna específica o un filtro que identifique un conjunto de entidades
*   Eliminar metadatos afectados por una sola ejecución de ingesta

Para seguir esta guía necesitas usar [DataHub CLI](../cli.md).

Siga leyendo para averiguar cómo realizar este tipo de eliminaciones.

*Nota: La eliminación de metadatos solo debe hacerse con cuidado. Usar siempre `--dry-run` para comprender lo que se eliminará antes de continuar. Prefiere las eliminaciones suaves (`--soft`) a menos que realmente desee bombardear filas de metadatos. Las eliminaciones completas en realidad eliminarán filas en el almacén principal y recuperarlas requerirá el uso de copias de seguridad del almacén de metadatos principal. Asegúrese de comprender las implicaciones de emitir eliminaciones suaves frente a eliminaciones automáticas antes de continuar.*

## Eliminar por urna

Para eliminar todos los datos relacionados con una sola entidad, ejecute

### Eliminación suave (el valor predeterminado)

Esto establece el `Status` aspecto de la entidad a `Removed`, que oculta la entidad y todos sus aspectos de ser devueltos por la interfaz de usuario.

    datahub delete --urn "<my urn>"

o

    datahub delete --urn "<my urn>" --soft

### Eliminación dura

Esto elimina físicamente todas las filas de todos los aspectos de la entidad. Esta acción no se puede deshacer, así que ejecútela solo después de estar seguro de que desea eliminar todos los datos asociados con esta entidad.

    datahub delete --urn "<my urn>" --hard

A partir de datahub v.0.8.35, hacer una eliminación completa por urna también le proporcionará una forma de eliminar las referencias a la urna que se elimina en el gráfico de metadatos. Es importante usarlo si no desea tener referencias fantasma en el modelo de metadatos y desea ahorrar espacio en la base de datos de gráficos.
Por ahora, este comportamiento debe ser optado por un mensaje que aparecerá para que usted lo acepte o niegue manualmente.

Opcionalmente, puede agregar `-n` o `--dry-run` para ejecutar una ejecución en seco antes de emitir el comando de eliminación final.
Opcionalmente, puede agregar `-f` o `--force` Para omitir confirmaciones

:::nota

¡Asegúrate de rodear tu urna con cotizaciones! Si no incluye las cotizaciones, su terminal puede malinterpretar el command.\_

:::

Si desea eliminar con una solicitud de rizo, puede usar algo como lo siguiente. Reemplace la URN por la URN que desea eliminar

    curl "http://localhost:8080/entities?action=delete" -X POST --data '{"urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"}'

## Eliminar usando filtros más amplios

\_Note: Todos estos comandos a continuación admiten la opción de eliminación suave (`-s/--soft`) así como la opción de funcionamiento en seco (`-n/--dry-run`). Además, a partir de v0.8.29 hay una nueva opción: `--include-removed` que elimina las entidades eliminadas suavemente que coinciden con el filtro proporcionado.

### Eliminar todos los conjuntos de datos en el entorno DEV

    datahub delete --env DEV --entity_type dataset

### Eliminar todos los contenedores de una plataforma en particular

    datahub delete --entity_type container --platform s3

### Eliminar todas las canalizaciones y tareas en el entorno DEV

    datahub delete --env DEV --entity_type "datajob"
    datahub delete --env DEV --entity_type "dataflow"

### Eliminar todos los datasets de bigquery en el entorno PROD

    datahub delete --env PROD --entity_type dataset --platform bigquery

### Eliminar todos los paneles y gráficos del looker

    datahub delete --entity_type dashboard --platform looker
    datahub delete --entity_type chart --platform looker

### Eliminar todos los conjuntos de datos que coincidan con una consulta

    datahub delete --entity_type dataset --query "_tmp" -n

## Ejecución por lotes de ingestión de reversión

La segunda forma de eliminar metadatos es identificar entidades (y los aspectos afectados) mediante una ingestión `run-id`. Siempre que corras `datahub ingest -c ...`, todos los metadatos ingeridos con esa ejecución tendrán el mismo identificador de ejecución.

Para ver los identificadores del conjunto más reciente de lotes de ingestión, ejecute

    datahub ingest list-runs

Eso imprimirá una tabla de todas las tiradas. Una vez que tenga una idea de qué carrera desea revertir, ejecute

    datahub ingest show --run-id <run-id>

para ver más información de la carrera.

Alternativamente, puede ejecutar una reversión en seco para lograr el mismo resultado.

    datahub ingest rollback --dry-run --run-id <run-id>

Finalmente, una vez que esté seguro de que desea eliminar estos datos para siempre, ejecute

    datahub ingest rollback --run-id <run-id>

para revertir todos los aspectos agregados con esta ejecución y todas las entidades creadas por esta ejecución.

### Entidades inseguras y reversión

> ***NOTA:*** Se ha agregado la preservación de entidades inseguras en datahub `0.8.32`. Siga leyendo para comprender lo que significa y cómo funciona.

En algunos casos, las entidades que fueron ingeridas inicialmente por una ejecución podrían haber tenido modificaciones adicionales en sus metadatos (por ejemplo, agregar términos, etiquetas o documentación) a través de la interfaz de usuario u otros medios. Durante una reversión de la ingestión que inicialmente creó estas entidades (técnicamente, si el aspecto clave para estas entidades se está revirtiendo), el proceso de ingestión analizará el gráfico de metadatos en busca de aspectos que se dejarán "colgando" y:

1.  Deje estos aspectos intactos en la base de datos y elimine la entidad de forma suave. Una reingestión de estas entidades hará que estos metadatos adicionales vuelvan a ser visibles en la interfaz de usuario, para que no pierda nada de su trabajo.
2.  La cli de datahub guardará información sobre estas entidades inseguras como CSV para que los operadores la revisen más tarde y decidan los próximos pasos (mantener o eliminar).

El comando rollback informará cuántas entidades tienen tales aspectos y guardará como CSV las urnas de estas entidades en un directorio de informes de reversión, que de forma predeterminada es `rollback_reports` en el directorio actual donde se ejecuta la CLI y se puede configurar aún más mediante el comando `--reports-dir` línea de comandos arg.

El operador puede utilizar `datahub get --urn <>` para inspeccionar los aspectos que quedaron atrás y mantenerlos (no hacer nada) o eliminar la entidad (y sus aspectos) completamente utilizando `datahub delete --urn <urn> --hard`. Si el operador desea eliminar todos los metadatos asociados con estas entidades no seguras, puede volver a emitir el comando rollback con el comando `--nuke` bandera.
