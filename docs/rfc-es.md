# Proceso RFC de DataHub

## ¿Qué es un RFC?

El proceso "RFC" (solicitud de comentarios) está destinado a proporcionar una ruta consistente y controlada para nuevas características,
modificaciones significativas, o cualquier otra propuesta significativa para ingresar a DataHub y sus marcos relacionados.

Muchos cambios, incluidas correcciones de errores y mejoras en la documentación, se pueden implementar y revisar a través del GitHub normal
flujo de trabajo de solicitud de extracción.

Sin embargo, algunos cambios son "sustanciales", y pedimos que estos se sometan a un poco de un proceso de diseño y produzcan un
consenso entre los equipos principales de DataHub.

## El ciclo de vida del RFC

Un RFC pasa por las siguientes etapas:

*   *Discusión* (Opcional): Cree un problema con la etiqueta "RFC" para tener una discusión inicial más abierta en torno a
    su propuesta (útil si aún no tiene una propuesta concreta). Considere la posibilidad de publicar en #rfc en [Flojo](./slack.md)
    para una mayor visibilidad.
*   *Pendiente*: cuando el RFC se presenta como PR. Agregue la etiqueta "RFC" al PR.
*   *Activo*: cuando un RFC PR se fusiona y se somete a implementación.
*   *Aterrizó*: cuando los cambios propuestos por un RFC se envían en una versión real.
*   *Rechazado*: cuando un RFC PR se cierra sin fusionarse.

[Lista de RFC pendiente](https://github.com/datahub-project/datahub/pulls?q=is%3Apr+is%3Aopen+label%3Arfc+)

## Cuándo seguir este proceso

Debe seguir este proceso si tiene la intención de realizar cambios "sustanciales" en cualquier componente del repositorio git de DataHub,
su documentación, o cualquier otro proyecto bajo el ámbito de los equipos centrales de DataHub. Lo que constituye un "sustancial"
el cambio está evolucionando en función de las normas de la comunidad, pero puede incluir lo siguiente:

*   Una nueva característica que crea una nueva área de superficie de API y requeriría un indicador de característica si se introdujera.
*   La eliminación de características que ya se enviaron como parte del canal de lanzamiento.
*   La introducción de nuevos usos idiomáticos o convenciones, incluso si no incluyen cambios de código en dataHub.

Algunos cambios no requieren un RFC:

*   Reformulación, reorganización o refactorización
*   Adición o eliminación de advertencias
*   Adiciones que mejoran estrictamente los criterios objetivos y numéricos de calidad (aceleración)

Si envía una solicitud de extracción para implementar una característica nueva e importante sin pasar por el proceso de RFC, es posible que se cierre
con una solicitud cortés para presentar un RFC primero.

## Recopilación de comentarios antes de enviarlos

A menudo es útil obtener comentarios sobre su concepto antes de sumergirse en el nivel de detalle de diseño de API requerido para un
RFC. Puede abrir un problema en este repositorio para iniciar una discusión de alto nivel, con el objetivo de formular eventualmente un RFC
solicitud de extracción con el diseño de implementación específico. También recomendamos encarecidamente compartir borradores de RFC en #rfc en el
[DataHub Slack](./slack.md) para una retroalimentación temprana.

## El proceso

En resumen, para obtener una característica importante agregada a DataHub, primero se debe fusionar el RFC en el repositorio rfc como una rebaja.
archivo. En ese momento, el RFC está "activo" y puede implementarse con el objetivo de una eventual inclusión en DataHub.

*   Bifurque el repositorio de DataHub.
*   Copie el `000-template.md` archivo de plantilla para `docs/rfc/active/000-my-feature.md`Dónde `my-feature` es más
    descriptivo. No asigne un número RFC todavía.
*   Rellene el RFC. Pon cuidado en los detalles. *RFC que no presentan una motivación convincente, demuestran comprensión
    del impacto del diseño, o son falsos sobre el inconveniente o las alternativas tienden a ser mal recibidas.*
*   Envíe una solicitud de extracción. Como una solicitud de extracción, el RFC recibirá comentarios de diseño de la comunidad en general, y el
    el autor debe estar preparado para revisarlo en respuesta.
*   Actualice la solicitud de extracción para agregar el número del PR al nombre de archivo y agregue un vínculo al PR en el encabezado del RFC.
*   Construir consenso e integrar retroalimentación. Los RFC que tienen un amplio apoyo tienen muchas más probabilidades de progresar que aquellos
    que no reciben ningún comentario.
*   Eventualmente, el equipo de DataHub decidirá si el RFC es un candidato para la inclusión.
*   Los RFC que son candidatos para la inclusión completarán un "período de comentarios finales" que durará 7 días. El comienzo de esto
    El período se indicará con un comentario y una etiqueta en la solicitud de extracción. Además, se hará un anuncio en el
    \#rfc canal de Slack para una mayor visibilidad.
*   Un RFC se puede modificar en función de los comentarios del equipo y la comunidad de DataHub. Modificaciones significativas pueden desencadenar
    un nuevo período de comentarios finales.
*   Un RFC puede ser rechazado por el equipo de DataHub después de que se haya resuelto la discusión pública y se hayan hecho comentarios resumiendo
    la justificación del rechazo. El RFC entrará en un "período de comentarios finales para cerrar" que durará 7 días. Al final de la "FCP"
    para cerrar" período, el PR se cerrará.
*   Un autor de RFC puede retirar su propio RFC cerrándolo ellos mismos. Por favor, indique el motivo del retiro.
*   Un RFC puede ser aceptado al final de su período de comentarios finales. Un miembro del equipo de DataHub fusionará el RFC asociado
    solicitud de extracción, momento en el que el RFC se volverá 'activo'.

## Detalles sobre los RFC activos

Una vez que un RFC se activa, los autores pueden implementarlo y enviar la función como una solicitud de extracción al repositorio de DataHub.
Convertirse en "activo" no es un sello de goma, y en particular todavía no significa que la característica finalmente se fusionará; eso
significa que el equipo central lo ha aceptado en principio y está dispuesto a fusionarlo.

Además, el hecho de que un determinado RFC haya sido aceptado y esté "activo" no implica nada sobre qué prioridad se asigna.
a su implementación, ni si alguien está trabajando actualmente en ello.

Las modificaciones a los RFC activos se pueden hacer en PR de seguimiento. Nos esforzamos por escribir cada RFC de una manera que refleje
el diseño final de la característica; pero la naturaleza del proceso significa que no podemos esperar que cada RFC fusionado realmente
reflejar cuál será el resultado final en el momento de la próxima versión principal; por lo tanto, tratamos de mantener cada documento RFC
algo sincronizado con la función de idioma según lo planeado, rastreando dichos cambios a través de solicitudes de extracción de seguimiento al documento.

## Implementación de un RFC

El autor de un RFC no está obligado a implementarlo. Por supuesto, el autor de RFC (como cualquier otro desarrollador) es bienvenido
publicar una implementación para su revisión después de que el RFC haya sido aceptado.

Un RFC activo debe tener el enlace a los PR de implementación enumerados, si los hay. Retroalimentación a la
la implementación debe llevarse a cabo en el PR de implementación en lugar del PR original de RFC.

Si está interesado en trabajar en la implementación de un RFC "activo", pero no puede determinar si alguien más lo está
ya trabajando en ello, siéntase libre de preguntar (por ejemplo, dejando un comentario sobre el tema asociado).

## RFC implementados

Una vez que finalmente se haya implementado un RFC, en primer lugar, ¡felicitaciones! ¡Y gracias por su contribución! En segundo lugar, a
ayudar a rastrear el estado del RFC, por favor haga un PR final para mover el RFC de `docs/rfc/active` Para
`docs/rfc/finished`.

## Revisión de RFC

La mayoría del equipo de DataHub intentará revisar algún conjunto de solicitudes de extracción de RFC abiertas de forma regular. Si un DataHub
Miembro del equipo cree que un RFC PR está listo para ser aceptado en estado activo, pueden aprobar el PR usando GitHub
función de revisión para indicar su aprobación de los RFC.

*El proceso RFC de DataHub está inspirado en muchos otros, incluyendo [Vue.js](https://github.com/vuejs/rfcs) y
[Brasa](https://github.com/emberjs/rfcs).*
