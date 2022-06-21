# Contribuyendo

Siempre damos la bienvenida a las contribuciones para ayudar a mejorar DataHub. Tómese un momento para leer este documento si desea contribuir.

## Proporcionar comentarios

¿Tiene ideas sobre cómo mejorar DataHub? Dirígete a [Solicitudes de características de DataHub](https://feature-requests.datahubproject.io/) y cuéntanos todo al respecto!

Muestre su apoyo a otras solicitudes votando; manténgase al día sobre los avances suscribiéndose a las actualizaciones por correo electrónico.

## Notificación de problemas

Usamos los problemas de GitHub para rastrear informes de errores y enviar solicitudes de extracción.

Si encuentras un error:

1.  Usa la búsqueda de problemas de GitHub para verificar si el error ya se ha informado.

2.  Si se ha solucionado el problema, intente reproducirlo utilizando la última rama maestra del repositorio.

3.  Si el problema persiste o aún no se ha informado, intente aislarlo antes de abrir un problema.

## Envío de una solicitud de comentarios (RFC)

Si tiene una característica sustancial o una discusión de diseño que le gustaría tener con la comunidad, siga el proceso de RFC descrito [aquí](./rfc.md)

## Envío de una solicitud de extracción (PR)

Antes de enviar su solicitud de extracción (PR), tenga en cuenta las siguientes directrices:

*   Busca en GitHub un PR abierto o cerrado que se relacione con tu envío. No quieres duplicar el esfuerzo.
*   Siga el [enfoque estándar de GitHub](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) para crear el PR. Por favor, siga también nuestro [formato de mensaje de confirmación](#commit-message-format).
*   Si hay algún cambio de ruptura, tiempo de inactividad potencial, obsolescencias o una gran característica, agregue una actualización en [Actualización de DataHub en Siguiente](how/updating-datahub.md).
*   ¡Eso es todo! ¡Gracias por su contribución!

## Formato de mensaje de confirmación

Por favor, siga el [Confirmaciones convencionales](https://www.conventionalcommits.org/) especificación para el formato de mensaje de confirmación. En resumen, cada mensaje de confirmación consta de un *encabezado*un *cuerpo* y un *pie de página*, separados por una sola línea en blanco.

    <type>[optional scope]: <description>

    [optional body]

    [optional footer(s)]

¡Cualquier línea del mensaje de confirmación no puede tener más de 88 caracteres! Esto permite que el mensaje sea más fácil de leer en GitHub, así como en varias herramientas de Git.

### Tipo

Debe ser uno de los siguientes (basado en el [Convención angular](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelines)):

*   *hazaña*: Una nueva característica
*   *arreglar*: Una corrección de errores
*   *refactorizar*: Un cambio de código que no corrige un error ni agrega una característica
*   *Docs*: La documentación solo cambia
*   *prueba*: Agregar pruebas faltantes o corregir pruebas existentes
*   *Perf*: Un cambio de código que mejora el rendimiento
*   *estilo*: Cambios que no afectan al significado del código (espacio en blanco, formato, punto y coma faltante, etc.)
*   *construir*: Cambios que afectan al sistema de compilación o a dependencias externas
*   *Ci*: Cambios en nuestros archivos de configuración y scripts de CI

Se puede proporcionar un alcance al tipo de confirmación, para proporcionar información contextual adicional y está contenida entre paréntesis, por ejemplo,

    feat(parser): add ability to parse arrays

### Descripción

Cada confirmación debe contener una descripción sucinta del cambio:

*   use el imperativo, tiempo presente: "cambio" no "cambiado" ni "cambios"
*   no pongas en mayúscula la primera letra
*   sin punto(.) al final

### Cuerpo

Al igual que en la descripción, use el imperativo, tiempo presente: "cambio" no "cambiado" ni "cambios". El cuerpo debe incluir la motivación para el cambio y contrastar esto con el comportamiento anterior.

### Pie de página

El pie de página debe contener cualquier información sobre *Cambios de última hora*, y también es el lugar para hacer referencia a los problemas de GitHub que este commit *Cierra*.

*Cambios de última hora* debe comenzar con las palabras `BREAKING CHANGE:` con un espacio o dos líneas nuevas. El resto del mensaje de confirmación se utiliza para esto.

### Revertir

Si la confirmación revierte una confirmación anterior, debe comenzar con `revert:`, seguido de la descripción. En el cuerpo debe decir: `Refs: <hash1> <hash2> ...`, donde los hashs son el SHA de las confirmaciones que se revierten, por ejemplo.

    revert: let us never again speak of the noodle incident

    Refs: 676104e, a215868
