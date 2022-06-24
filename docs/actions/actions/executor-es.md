# Ejecutor de ingestión

<!-- Set Support Status -->

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

## Visión general

Esta acción ejecuta recetas de ingesta que se configuran a través de la interfaz de usuario.

### Capacidades

*   Ejecutar `datahub ingest` en un subproceso cuando se recibe un comando solicitud de ejecución de DataHub. (Ejecución de ingestión programada o manual)
*   Resolución de secretos dentro de una receta de ingesta desde DataHub
*   Notificación del estado de ejecución de la ingesta a DataHub

### Eventos admitidos

*   `MetadataChangeLog_v1`

Específicamente, los cambios en el `dataHubExecutionRequestInput` y `dataHubExecutionRequestSignal` aspectos de la `dataHubExecutionRequest` se requieren entidades.

## Inicio rápido de acción

### Prerrequisitos

#### Privilegios de DataHub

Esta acción debe ejecutarse como un usuario privilegiado de DataHub (por ejemplo, utilizando tokens de acceso personal). En concreto, el usuario debe disponer de la `Manage Secrets` Privilegio de plataforma, que permite la recuperación
de secretos descifrados para inyectar en una receta de ingestión.

Un token de acceso generado a partir de una cuenta con privilegios debe configurarse en el `datahub` configuración
de la configuración de YAML, como se muestra en el ejemplo siguiente.

#### Conexión a fuentes de ingestión

Para que la ingestión se ejecute correctamente, el proceso que ejecuta las Acciones debe tener
conectividad de red a cualquier sistema de origen que se requiera para la ingesta.

Por ejemplo, si la receta de ingestión se extrae de un DBMS interno, el contenedor de acciones
debe ser capaz de resolver y conectarse a ese sistema DBMS para que el comando de ingestión se ejecute correctamente.

### Instalar el(los) Plugin(s)

Ejecute los siguientes comandos para instalar los complementos de acción relevantes:

`pip install 'acryl-datahub-actions[executor]'`

### Configurar la configuración de acción

Utilice las siguientes configuraciones para comenzar con esta acción.

```yml
name: "pipeline-name"
source:
  # source configs
action:
  type: "executor"
# Requires DataHub API configurations to report to DataHub
datahub:
  server: "http://${GMS_HOST:-localhost}:${GMS_PORT:-8080}"
  # token: <token> # Must have "Manage Secrets" privilege
```

<details>
  <summary>View All Configuration Options</summary>

| | de campo | requerido | predeterminada Descripción |
| --- | :-: | :-: | --- |
| `executor_id` | ❌ | `default` | Un identificador de ejecutor asignado al albacea. Esto se puede utilizar para administrar múltiples ejecutores distintos. |

</details>

## Solución de problemas

### Salir del marco de acciones

Actualmente, al salir del marco de acciones, cualquier procesamiento de ingesta en curso continuará ejecutándose como un subproceso en el sistema. Esto significa que puede haber procesos "huérfanos" que
nunca se marcan como "Correcto" o "Fallido" en la interfaz de usuario, aunque se hayan completado.

Para solucionar esto, simplemente "Cancelar" el origen de ingestión en la interfaz de usuario una vez que haya reiniciado la acción Ejecutor de ingestión.
