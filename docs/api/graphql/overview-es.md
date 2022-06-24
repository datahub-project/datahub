# DataHub GraphQL API

### DataHub proporciona una amplia [GraphQL](https://graphql.org/) API para interactuar mediante programación con las entidades y relaciones que componen el gráfico de metadatos de su organización

## Empezar

Check-out [Empezar](./getting-started.md) para comenzar a usar la API De DataHub GraphQL de inmediato.

## Acerca de GraphQL

[GraphQL](https://graphql.org/) proporciona un lenguaje de consulta de datos y una API con las siguientes características:

*   Un **especificación validada**: La especificación GraphQL verifica un *esquema* en el servidor API. El servidor a su vez es responsable
    para validar las consultas entrantes de los clientes con ese esquema.
*   **Fuertemente tipado**: Un esquema GraphQL declara el universo de tipos y relaciones que componen la interfaz.
*   **Orientado a documentos y jerárquico**: GraphQL hace que sea fácil solicitar entidades relacionadas utilizando un documento JSON familiar
    estructura. Esto minimiza el número de solicitudes de API de ida y vuelta que un cliente debe realizar para responder a una pregunta en particular.
*   **Flexible y eficiente**: GraphQL proporciona una forma de solicitar solo los datos que desea, y eso es todo. Ignorar todo
    el resto. Le permite reemplazar varias llamadas REST con una llamada GraphQL.
*   **Gran ecosistema de código abierto**: Se han desarrollado proyectos de código abierto GraphQL para [prácticamente todos los lenguajes de programación](https://graphql.org/code/). Con un próspero
    comunidad, ofrece una base sólida sobre la que construir.

Por estas razones, entre otras, DataHub proporciona una API GraphQL sobre el Metadata Graph,
permitiendo una fácil exploración de las Entidades y Relaciones que lo componen.

Para obtener más información acerca de la especificación GraphQL, consulte [Introducción a GraphQL](https://graphql.org/learn/).

## Referencia del esquema GraphQL

Los documentos de referencia de la barra lateral se generan a partir del esquema DataHub GraphQL. Cada llamada a la `/api/graphql` el punto final es
validado con este esquema. Puede utilizar estos documentos para comprender los datos que están disponibles para la recuperación y las operaciones
que se puede realizar utilizando la API.

*   Operaciones disponibles: [Consultas](/graphql/queries.md) (Lecturas) y [Mutaciones](/graphql/mutations.md) (Escribe)
*   Tipos de esquema: [Objetos](/graphql/objects.md), [Objetos de entrada](/graphql/inputObjects.md), [Interfaces](/graphql/interfaces.md), [Uniones](/graphql/unions.md), [Enums](/graphql/enums.md), [Escalares](/graphql/scalars.md)

## En el horizonte

La API graphQL en continuo desarrollo. Algunas de las cosas que más nos entusiasman se pueden encontrar a continuación.

### Compatibilidad con casos de uso adicionales

DataHub planea admitir los siguientes casos de uso a través de la API de GraphQL:

*   **Creación de entidades**: Creación mediante programación de conjuntos de datos, paneles, gráficos, flujos de datos (canalizaciones), trabajos de datos (tareas) y más.
*   **Listado de entidades**: Enumerar todas las entidades de metadatos de un tipo determinado.
*   **Eliminación de entidades**: La capacidad de eliminar entidades de metadatos en DataHub.

Esto permitirá a las organizaciones crear flujos sobre DataHub para crear entidades de metadatos personalizadas,
extraer datos por lotes de DataHub y seleccionar las entidades que son visibles en la plataforma.

### SDK de cliente

DataHub planea desarrollar SDK de cliente de código abierto para Python, Java, Javascript, entre otros, además de esta API. Si te interesa
en la contribución, [únete a nosotros en Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email)!

### Autenticación basada en tokens

Agregue autenticación universal basada en tokens y deshágase del legado `PLAY_SESSION` cookie de sesión y `X-DataHub-Actor` usado
para transmitir información de identidad del actor.

## Comentarios, solicitudes de funciones y soporte

Visita nuestro [Canal de Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email) para hacer preguntas, decirnos qué podemos hacer mejor y hacer solicitudes de lo que le gustaría ver en el futuro. O simplemente
pásate a decir 'Hola'.
