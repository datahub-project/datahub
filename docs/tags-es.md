# Guía de Etiquetas

## ¿Qué es una etiqueta?

Las etiquetas son metadatos que se pueden agregar conjuntos de datos, esquemas de conjuntos de datos o contenedores. Proporcionan una manera fácil de etiquetar o categorizar entidades.

### Etiquetas vs. Términos del Glosario vs. Dominios

DataHub admite etiquetas, términos de glosario y dominios como distintos tipos de metadatos que son adecuados para fines específicos:

*   **Etiquetas**: Etiquetas informales y poco controladas que sirven como herramienta para la búsqueda y el descubrimiento. Los activos pueden tener varias etiquetas. Sin gestión formal y central.
*   **Términos del glosario**: Un vocabulario controlado, con jerarquía opcional. Los términos se utilizan normalmente para estandarizar tipos de atributos a nivel de hoja (es decir, campos de esquema) para la gobernanza. Por ejemplo, (EMAIL_PLAINTEXT)
*   **Dominios**: Un conjunto de categorías de nivel superior. Por lo general, alineado con las unidades de negocio / disciplinas para las que los activos son más relevantes. Gestión centralizada o distribuida. Asignación de dominio único por activo de datos.

## Agregar una etiqueta

Los usuarios deben tener el privilegio de metadatos llamado `Edit Tags` para agregar etiquetas en el nivel de entidad y el privilegio llamado `Edit Dataset Column Tags` para editar etiquetas en el nivel de columna. Estos privilegios
se puede conceder mediante la creación de un nuevo metadato [Política](./policies.md).

Para agregar una etiqueta a nivel de conjunto de datos o contenedor, simplemente navegue a la página de esa entidad y haga clic en el botón "Agregar etiqueta".

![](./imgs/add-tag.png)

A continuación, puede escribir el nombre de la etiqueta que desea agregar. Puede agregar una etiqueta que ya existe o agregar una nueva etiqueta por completo. El autocompletado abrirá la etiqueta si ya existe.

![](./imgs/add-tag-search.png)

¡Haga clic en el botón "Agregar" y verá que la etiqueta se ha agregado!

![](./imgs/added-tag.png)

Si desea agregar una etiqueta en el nivel de esquema, coloque el cursor sobre la columna "Etiquetas" de un esquema hasta que aparezca el botón "Agregar etiqueta" y, a continuación, siga el mismo flujo que el anterior.

![](./imgs/added-tag.png)

## Eliminación de una etiqueta

Para eliminar una etiqueta, simplemente haga clic en el botón "X" en la etiqueta. Luego haga clic en "Sí" cuando se le solicite que confirme la eliminación de la etiqueta.

## Búsqueda por etiqueta

Puede buscar una etiqueta en la barra de búsqueda y, si lo desea, incluso filtrar entidades por la presencia de una etiqueta específica.

![](./imgs/search-tag.png)

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, ¡comunícate con Slack!
