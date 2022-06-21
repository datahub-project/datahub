# Guía de ingestión de la interfaz de usuario

## Introducción

A partir de la versión `0.8.25`, DataHub admite la creación, configuración, programación y ejecución de la ingesta de metadatos por lotes mediante la interfaz de usuario de DataHub. Esto hace que
obtener metadatos en DataHub más fácilmente al minimizar la sobrecarga requerida para operar canalizaciones de integración personalizadas.

Este documento describirá los pasos necesarios para configurar, programar y ejecutar la ingesta de metadatos dentro de la interfaz de usuario.

## Ejecución de la ingesta de metadatos

### Prerrequisitos

Para ver y administrar la ingesta de metadatos basada en la interfaz de usuario, debe tener el `Manage Metadata Ingestion` & `Manage Secrets`
privilegios asignados a su cuenta. Estos pueden ser otorgados por un [Política de la plataforma](./policies.md).

![](./imgs/ingestion-privileges.png)

Una vez que tenga estos privilegios, puede comenzar a administrar la ingesta navegando a la pestaña 'Ingestión' en DataHub.

![](./imgs/ingestion-tab.png)

En esta página, verá una lista de activos **Fuentes de ingestión**. Un origen de ingesta es una fuente única de metadatos ingeridos
en DataHub desde una fuente externa como Snowflake, Redshift o BigQuery.

Si recién está comenzando, no tendrá ninguna fuente. En las siguientes secciones, describiremos cómo crear
tu primera **Fuente de ingestión**.

### Creación de un origen de ingesta

Antes de ingerir cualquier metadato, debe crear un nuevo origen de ingesta. Comience haciendo clic en **+ Crear nueva fuente**.

![](./imgs/create-new-ingestion-source-button.png)

#### Paso 1: Seleccione una plantilla de plataforma

En el primer paso, seleccione un **Plantilla de receta** correspondiente al tipo de origen del que desea extraer metadatos. Elige entre
una variedad de integraciones soportadas de forma nativa, desde Snowflake hasta Postgres y Kafka.
Escoger `Custom` para construir una receta de ingestión desde cero.

![](./imgs/select-platform-template.png)

A continuación, configurará una ingestión **Receta**, que define *cómo* y *Qué* para extraer del sistema de origen.

#### Paso 2: Configurar una receta

A continuación, definirás una ingestión **Receta** en [YAML](https://yaml.org/). Un [Receta](https://datahubproject.io/docs/metadata-ingestion/#recipes) es un conjunto de configuraciones que es
utilizado por DataHub para extraer metadatos de un sistema de 3ª parte. La mayoría de las veces consta de las siguientes partes:

1.  Una fuente **tipo**: El tipo de sistema del que desea extraer metadatos (por ejemplo, copo de nieve, mysql, postgres). Si ha elegido una plantilla nativa, esta ya se rellenará por usted.
    Para ver una lista completa de los soportes actuales **Tipos**Check-out [esta lista](https://datahubproject.io/docs/metadata-ingestion/#installing-plugins).

2.  Una fuente **configuración**: Un conjunto de configuraciones específicas del origen **tipo**. La mayoría de los orígenes admiten los siguientes tipos de valores de configuración:
    *   **Coordenadas**: La ubicación del sistema del que desea extraer metadatos
    *   **Credenciales**: Credenciales autorizadas para acceder al sistema del que desea extraer metadatos
    *   **Personalizaciones**: Personalizaciones con respecto a los metadatos que se extraerán, por ejemplo, qué bases de datos o tablas escanear en una base de datos relacional

3.  Un fregadero **tipo**: tipo de receptor para enrutar los metadatos extraídos del tipo de origen. El receptor de DataHub oficialmente compatible
    los tipos son `datahub-rest` y `datahub-kafka`.

4.  Un fregadero **configuración**: Configuración necesaria para enviar metadatos al tipo de receptor proporcionado. Por ejemplo, coordenadas y credenciales de DataHub.

Una muestra de una receta completa configurada para ingerir metadatos de MySQL se puede encontrar en la imagen a continuación.

![](./imgs/example-mysql-recipe.png)

Se pueden encontrar ejemplos detallados de configuración y documentación para cada tipo de fuente en el [Documentos de DataHub](https://datahubproject.io/docs/metadata-ingestion/) sitio web.

##### Creación de un secreto

Para casos de uso de producción, valores de configuración confidenciales, como nombres de usuario y contraseñas de bases de datos,
debe estar oculto a la vista dentro de su receta de ingestión. Para lograr esto, puede crear e incrustar **Secretos**. Los secretos se denominan valores
que se cifran y almacenan dentro de la capa de almacenamiento de DataHub.

Para crear un secreto, primero navegue a la pestaña 'Secretos'. A continuación, haga clic en `+ Create new secret`.

![](./imgs/create-secret.png)

*Creación de un secreto para almacenar el nombre de usuario de una base de datos MySQL*

Dentro del formulario, proporcione un nombre único para el secreto junto con el valor que se va a cifrar y una descripción opcional. Clic **Crear** cuando haya terminado.
Esto creará un secreto al que se puede hacer referencia dentro de su receta de ingestión usando su nombre.

##### Hacer referencia a un secreto

Una vez que se ha creado un secreto, se puede hacer referencia a él desde su **Receta** utilizando la sustitución de variables. Por ejemplo
para sustituir los secretos por un nombre de usuario y contraseña de MySQL en una receta, su receta se definiría de la siguiente manera:

```yaml
source:
    type: mysql
    config:
        host_port: 'localhost:3306'
        database: my_db
        username: ${MYSQL_USERNAME}
        password: ${MYSQL_PASSWORD}
        include_tables: true
        include_views: true
        profiling:
            enabled: true
sink:
    type: datahub-rest
    config:
        server: 'http://datahub-gms:8080'
```

*Hacer referencia a secretos de DataHub desde una definición de receta*

Cuando se ejecute la fuente de ingesta con esta receta, DataHub intentará "resolver" los secretos encontrados dentro del YAML. Si se puede resolver un secreto, la referencia se sustituye por su valor descifrado antes de la ejecución.
Los valores secretos no se conservan en el disco más allá del tiempo de ejecución y nunca se transmiten fuera de DataHub.

> **Atención**: Cualquier usuario de DataHub al que se le haya concedido el `Manage Secrets` [Privilegio de plataforma](./policies.md) podrá recuperar valores secretos de texto sin formato utilizando la API graphQL.

#### Paso 3: Programar la ejecución

A continuación, puede configurar opcionalmente una programación en la que ejecutar el nuevo origen de ingesta. Esto permite programar la extracción de metadatos en una cadencia mensual, semanal, diaria o horaria, dependiendo de las necesidades de su organización.
Las programaciones se definen utilizando el formato CRON.

![](./imgs/schedule-ingestion.png)

*Una fuente de ingestión que se ejecuta a las 9:15 am todos los días, hora de Los Ángeles*

Para obtener más información sobre el formato de programación CRON, consulte el [Wikipedia (en inglés](https://en.wikipedia.org/wiki/Cron) visión general.

Si planea ejecutar la ingestión de forma ad-hoc, puede hacer clic en **Saltarse** para omitir el paso de programación por completo. No te preocupes -
siempre puedes volver y cambiar esto.

#### Paso 4: Terminar

Por último, asigne un nombre a su fuente de ingestión.

![](./imgs/name-ingestion-source.png)

Una vez que esté satisfecho con sus configuraciones, haga clic en 'Listo' para guardar sus cambios.

##### Avanzado: Ejecución con una versión específica de la CLI

DataHub viene preconfigurado para usar la última versión de la CLI de DataHub ([acryl-datahub](https://pypi.org/project/acryl-datahub/)) que es compatible
con el servidor. Sin embargo, puede anular la versión predeterminada del paquete utilizando las configuraciones de origen 'Avanzadas'.

Para hacerlo, simplemente haga clic en 'Avanzado', luego cambie el cuadro de texto 'Versión de cli' para que contenga la versión exacta
de la CLI de DataHub que desea usar.

![](./imgs/custom-ingestion-cli-version.png)
*Anclar la versión de la CLI a la versión `0.8.23.2`*

Una vez que esté satisfecho con sus cambios, simplemente haga clic en 'Listo' para guardar.

### Ejecución de un origen de ingesta

Una vez que haya creado su fuente de ingestión, puede ejecutarla haciendo clic en 'Ejecutar'. Poco después,
Debería ver la columna 'Último estado' del cambio de origen de ingesta de `N/A` Para `Running`. Éste
significa que la solicitud para ejecutar la ingesta ha sido recogida correctamente por el ejecutor de ingesta de DataHub.

![](./imgs/running-ingestion.png)

Si la ingestión se ha ejecutado correctamente, debería ver su estado que se muestra en verde como `Succeeded`.

![](./imgs/successful-ingestion.png)

### Cancelación de una ejecución de ingestión

Si su ejecución de ingestión está colgando, puede haber un error en la fuente de ingestión u otro problema persistente como tiempos de espera exponenciales. Si estas situaciones,
Puede cancelar la ingestión haciendo clic en **Cancelar** en la carrera problemática.

![](./imgs/cancelled-ingestion.png)

Una vez cancelado, puede ver la salida de la ejecución de ingestión haciendo clic en **Detalles**.

### Depuración de una ejecución de ingesta fallida

![](./imgs/failed-ingestion.png)

Una variedad de cosas pueden hacer que una carrera de ingestión falle. Las razones comunes para el fracaso incluyen:

1.  **Configuración incorrecta de la receta**: Una receta no ha proporcionado las configuraciones requeridas o esperadas para la fuente de ingestión. Puede consultar
    al [Marco de ingesta de metadatos](https://datahubproject.io/docs/metadata-ingestion) documentos de origen para obtener más información sobre las configuraciones necesarias para el tipo de origen.

2.  **Falta de resolución de secretos**: Si DataHub no puede encontrar secretos a los que hizo referencia la configuración de la receta, se producirá un error en la ejecución de ingesta.
    Verifique que los nombres de los secretos a los que se hace referencia en su receta coincidan con los que se han creado.

3.  **Conectividad / Accesibilidad de red**: Si DataHub no puede llegar a un origen de datos, por ejemplo, debido a la resolución DNS
    fallos, la ingesta de metadatos fallará. Asegúrese de que la red donde se implementa DataHub tenga acceso a la fuente de datos que
    estás tratando de alcanzar.

4.  **Autenticación**: Si has habilitado [Autenticación del servicio de metadatos](https://datahubproject.io/docs/introducing-metadata-service-authentication/), deberá proporcionar un token de acceso personal
    en la configuración de la receta. Para ello, establezca el campo 'token' de la configuración del receptor para que contenga un token de acceso personal:
    ![](./imgs/ingestion-with-token.png)

El resultado de cada ejecución se captura y está disponible para ver en la interfaz de usuario para facilitar la depuración. Para ver los registros de salida, haga clic en **DETALLES**
en la ejecución de ingestión correspondiente.

![](./imgs/ingestion-logs.png)

## PREGUNTAS MÁS FRECUENTES

### Intenté ingerir metadatos después de ejecutar 'datahub docker quickstart', pero la ingesta está fallando con los errores 'Failed to Connect'. ¿Qué hago?

Si no se debe a una de las razones descritas anteriormente, esto puede deberse a que el ejecutor que ejecuta la ingestión no puede
para llegar al back-end de DataHub utilizando las configuraciones predeterminadas. Intente cambiar su receta de ingestión para hacer el `sink.config.server` punto variable al Docker
Nombre DNS para el `datahub-gms` vaina:

![](./imgs/quickstart-ingestion-config.png)

### Veo 'N / A' cuando intento ejecutar la ingestión. ¿Qué hago?

Si ve 'N/A' y el estado de ejecución de ingestión nunca cambia a 'Running', esto puede significar
que su albacea (`datahub-actions`) el contenedor está caído.

Este contenedor es responsable de ejecutar las solicitudes para ejecutar la ingestión cuando entran, ya sea
bajo demanda en un horario determinado. Puede verificar el estado del contenedor mediante `docker ps`. Además, puede inspeccionar los registros del contenedor utilizando la búsqueda del identificador del contenedor
para el `datahub-actions` contenedor y ejecución `docker logs <container-id>`.

### ¿Cuándo NO debo usar UI Ingestion?

Hay casos válidos para ingerir metadatos sin el programador de ingesta basado en la interfaz de usuario. Por ejemplo

*   Ha escrito un origen de ingesta personalizado
*   No se puede acceder a los orígenes de datos en la red donde se implementa DataHub
*   Su fuente de ingesta requiere contexto de un sistema de archivos local (por ejemplo, archivos de entrada, variables de entorno, etc.)
*   Desea distribuir la ingesta de metadatos entre varios productores / entornos

### ¿Cómo adjunto directivas al pod de acciones para otorgarle permisos para extraer metadatos de varios orígenes?

Esto varía entre la plataforma subyacente. Para AWS, consulte esto [guiar](./deploy/aws.md#iam-policies-for-ui-based-ingestion).

## Demo

Clic [aquí](https://www.youtube.com/watch?v=EyMyLcaw\_74) para ver una demostración completa de la característica De ingestión de la interfaz de usuario.

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, comuníquese con [Flojo](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email)!
