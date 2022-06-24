# Hola mundo

<!-- Set Support Status -->

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

## Visión general

Esta acción es una acción de ejemplo que simplemente imprime todos los eventos que recibe como JSON.

### Capacidades

*   Impresión de eventos recibidos por la acción en la consola.

### Eventos admitidos

Todos los tipos de eventos, incluidos

*   `EntityChangeEvent_v1`
*   `MetadataChangeLog_v1`

## Inicio rápido de acción

### Prerrequisitos

Sin requisitos previos. Esta acción viene precargada con `acryl-datahub-actions`.

### Instalar el(los) Plugin(s)

Esta acción viene con el Marco de acciones de forma predeterminada:

`pip install 'acryl-datahub-actions'`

### Configurar la configuración de acción

Utilice las siguientes configuraciones para comenzar con esta acción.

```yml
name: "pipeline-name"
source:
  # source configs
action:
  type: "hello_world"
```

<details>
  <summary>View All Configuration Options</summary>

| | de campo | requerido | predeterminada Descripción |
| --- | :-: | :-: | --- |
| `to_upper` | ❌| `False` | Si se van a imprimir eventos en mayúsculas. |

</details>

## Solución de problemas

N/A
