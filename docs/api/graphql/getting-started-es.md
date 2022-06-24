# Empezar

### Mojarse los pies con la API DataHub GraphQL

## Introducción a GraphQL

La comunidad graphQL proporciona muchos recursos disponibles gratuitamente para aprender sobre GraphQL. Recomendamos comenzar con [Introducción a GraphQL](https://graphql.org/learn/),
que le presentará conceptos clave como [Consultas, mutaciones, variables, esquemas y más](https://graphql.org/learn/queries/).

Reiteraremos algunos puntos importantes antes de continuar:

*   Las operaciones de GraphQL se exponen a través de un único punto final de servicio, en el caso de DataHub ubicado en `/api/graphql`. Esto se describirá con más detalle a continuación.
*   GraphQL admite lecturas utilizando un nivel superior **Consulta** y escribe mediante un nivel superior **Mutación** objeto.
*   Compatibilidad con GraphQL [introspección de esquemas](https://graphql.org/learn/introspection/), en el que los clientes pueden consultar detalles sobre el propio esquema graphQL.

## Arreglo

Lo primero que necesitará para usar la API de GraphQL es una instancia implementada de DataHub con algunos metadatos ingeridos. ¿No está seguro de cómo hacerlo? Echa un vistazo a la [Inicio rápido de implementación](../../../docs/quickstart.md).

## El extremo DataHub GraphQL

Hoy en día, el punto final GraphQL de DataHub está disponible para su uso en múltiples lugares. El que elija usar depende de su caso de uso específico.

1.  **Servicio de metadatos**: El servicio de metadatos de DataHub (backend) es la fuente de la verdad para el extremo de GraphQL. El punto de conexión se encuentra en `/api/graphql` ruta de acceso de la dirección DNS
    donde su instancia de la `datahub-gms` se implementa el contenedor. Por ejemplo, en las implementaciones locales normalmente se encuentra en `http://localhost:8080/api/graphql`. De forma predeterminada,
    el servicio de metadatos no tiene comprobaciones de autenticación explícitas. Sin embargo, tiene *Comprobaciones de autorización*. Centro de datos [Políticas de acceso](../../../docs/policies.md) será aplicado por la API graphQL. Esto significa que deberá proporcionar una identidad de actor al consultar la API de GraphQL.
    Para ello, incluya el `X-DataHub-Actor` con una URN de usuario de Authorized Corp como valor de la solicitud. Dado que cualquier persona puede establecer el valor de este encabezado, se recomienda usar este punto de conexión solo en entornos de confianza, ya sea por los propios administradores o por programas que posean directamente.

2.  **Frontend Proxy**: El servicio de proxy frontend de DataHub (frontend) es un servidor web básico y proxy inverso al servicio de metadatos. Como tal, el
    El extremo graphQL también está disponible para consultas dondequiera que se implemente el proxy frontend. En las implementaciones locales, esto suele ser `http://localhost:9002/api/v2/graphql`. De forma predeterminada,
    el proxy frontend *hace* tener autenticación basada en cookies de sesión a través de la cookie de PLAY_SESSION establecida en la hora de inicio de sesión de la interfaz de usuario de DataHub. Esto significa
    que si una solicitud no tiene una cookie de PLAY_SESSION válida obtenida al iniciar sesión en la interfaz de usuario de DataHub, la solicitud será rechazada. Para utilizar esta API en un entorno que no es de confianza,
    necesitaría a) iniciar sesión en DataHub, b) extraer la cookie PLAY_SESSION que se establece al iniciar sesión, y c) proporcionar esta cookie en sus encabezados HTTP cuando
    llamando al extremo.

### Consulta del extremo

Hay algunas opciones cuando se trata de consultar el punto de conexión de GraphQL. La recomendación sobre cuál usar varía según el caso de uso.

**Ensayo**: [Cartero](https://learning.postman.com/docs/sending-requests/supported-api-frameworks/graphql/), GraphQL Explorer (descrito a continuación), CURL

**Producción**: GraphQL [SDK de cliente](https://graphql.org/code/) para el idioma de su elección, o un cliente HTTP básico.

> Importante: El punto de conexión de DataHub GraphQL solo admite solicitudes POST en este momento. No admite solicitudes GET. Si esto es algo
> necesitas, ¡háznoslo saber en Slack!

### En el horizonte

*   **Tokens de acceso al servicio**: En un futuro próximo, el equipo de DataHub tiene la intención de introducir usuarios del servicio, lo que proporcionará una forma de generar y utilizar el acceso a la API.
    tokens al consultar tanto el servidor proxy front-end como el servicio de metadatos. Si estás interesado en contribuir, por favor [comunícate con nuestro Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email).
*   **SDK de cliente de DataHub**: Bibliotecas que envuelven la API De DataHub GraphQL por idioma (según la demanda de la comunidad).

## Explorador de GraphQL

DataHub proporciona una herramienta GraphQL Explorer basada en navegador ([GraphiQL](https://github.com/graphql/graphiql)) para la interacción en vivo con la API de GraphQL. Hoy en día, esta herramienta está disponible para su uso en múltiples lugares (como el propio punto final de GraphQL):

1.  **Servicio de metadatos**: `http://<metadata-service-address>/api/graphiql`. Para implementaciones locales, `http://localhost:8080/api/graphiql`.
2.  **Frontend Proxy**: `http://<frontend-service-address>/api/graphiql`. Para implementaciones locales, `http://localhost:9002/api/graphiql`.

Esta interfaz le permite crear fácilmente consultas y mutaciones contra metadatos reales almacenados en su implementación de DataHub en vivo. Para obtener una guía de uso detallada,
Check-out [Cómo usar GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/).

Las mismas restricciones de autenticación descritas en la sección anterior también se aplican a estos puntos finales.

> **Consejo profesional**: Le recomendamos que agregue una extensión del navegador que le permita establecer encabezados HTTP personalizados (es decir. `Cookies` o `X-DataHub-Actor`) si planea utilizar GraphiQL para las pruebas. Nos gusta [ModHeader](https://chrome.google.com/webstore/detail/modheader/idgpnmonknjnojddfkpgkljpfnnfcklj?hl=en) para Google Chrome.

## A dónde ir desde aquí

Una vez que haya implementado la API y respondido, proceda a [Entidades de consulta](./querying-entities.md) para aprender a leer y escribir las Entidades
en su gráfico de metadatos.
Si estás interesado en acciones administrativas considerando echar un vistazo a [Gestión de tokens](./token-management.md) para aprender a generar, enumerar y revocar tokens de acceso para uso programático en DataHub.

## Comentarios, solicitudes de funciones y soporte

Visita nuestro [Canal de Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email) para hacer preguntas, decirnos qué podemos hacer mejor y hacer solicitudes de lo que le gustaría ver en el futuro. O simplemente
pásate a decir 'Hola'.
