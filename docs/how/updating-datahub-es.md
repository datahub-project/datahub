# Actualización de DataHub

Este archivo documenta cualquier cambio incompatible con versiones anteriores en DataHub y ayuda a las personas a migrar a una nueva versión.

## Próximo

### Cambios de última hora

### Tiempo de inactividad potencial

### Obsolescencias

*   `KAFKA_TOPIC_NAME` variable de entorno en **datahub-mae-consumer** y **datahub-gms** ahora está en desuso. Uso `METADATA_AUDIT_EVENT_NAME` en lugar de.
*   `KAFKA_MCE_TOPIC_NAME` variable de entorno en **datahub-mce-consumidor** y **datahub-gms** ahora está en desuso. Uso `METADATA_CHANGE_EVENT_NAME` en lugar de.
*   `KAFKA_FMCE_TOPIC_NAME` variable de entorno en **datahub-mce-consumidor** y **datahub-gms** ahora está en desuso. Uso `FAILED_METADATA_CHANGE_EVENT_NAME` en lugar de.

### Otros cambios notables

*   \#5132 Tablas de perfiles en `snowflake` origen sólo si se han actualizado desde que se configuraron (valor predeterminado: `1`) número de día(s). Actualizar la configuración `profiling.profile_if_updated_since_days` según su programación de generación de perfiles o configúrela en `None` si quieres un comportamiento más antiguo.

## `v0.8.38`

### Cambios de última hora

### Tiempo de inactividad potencial

### Obsolescencias

### Otros cambios notables

*   Crear y revocar tokens de acceso a través de la interfaz de usuario
*   Crear y administrar nuevos usuarios a través de la interfaz de usuario
*   Mejoras en la interfaz de usuario del glosario empresarial
*   REVISIÓN: no es necesario volver a indexar para migrar al uso del glosario empresarial de la interfaz de usuario

## `v0.8.36`

### Cambios de última hora

*   En esta versión presentamos una nueva experiencia de Glosario de Negocios. Con esta nueva experiencia vienen algunas nuevas formas de indexar datos para hacer posible la visualización y el recorrido de los diferentes niveles de su Glosario. Por lo tanto, tendrá que [Restaurar los índices](https://datahubproject.io/docs/how/restore-indices/) para que la nueva experiencia de Glosario funcione para los usuarios que ya tienen Glosarios existentes. Si es la primera vez que usas DataHub Glossaries, ¡ya estás listo!

### Tiempo de inactividad potencial

### Obsolescencias

### Otros cambios notables

*   \#4961 La creación de perfiles eliminados no se informa de forma predeterminada, ya que eso causó una gran cantidad de registros espurios en algunos casos. Poner `profiling.report_dropped_profiles` Para `True` si quieres un comportamiento más antiguo.

## `v0.8.35`

### Cambios de última hora

### Tiempo de inactividad potencial

### Obsolescencias

*   \#4875 El contenido del archivo de vista Lookml ya no se rellenará en custom_properties, sino que las definiciones de vista estarán siempre disponibles en la pestaña Ver definiciones.

### Otros cambios notables

## `v0.8.34`

### Cambios de última hora

*   \#4644 Eliminar `database` opción desde `snowflake` fuente que quedó obsoleta desde entonces `v0.8.5`
*   \#4595 Cambiar el nombre de la configuración confusa `report_upstream_lineage` Para `upstream_lineage_in_report` en `snowflake` conector que se agregó en `0.8.32`

### Tiempo de inactividad potencial

### Obsolescencias

*   \#4644 `host_port` opción de `snowflake` y `snowflake-usage` fuentes en desuso ya que el nombre era confuso. Uso `account_id` en su lugar.

### Otros cambios notables

*   \#4760 `check_role_grants` Se agregó la opción en `snowflake` Para deshabilitar la protección de roles en `snowflake` ya que algunas personas informaban de largos plazos al comprobar los roles.
