# Guía de dominios

## ¿Qué es un dominio?

A partir de la versión `0.8.25`, DataHub admite la agrupación de activos de datos en colecciones lógicas denominadas **Dominios**. Los dominios están curados, de nivel superior
carpetas o categorías donde los activos relacionados se pueden agrupar explícitamente. La administración de dominios se puede centralizar o distribuir
a los propietarios de dominios. Actualmente, un activo solo puede pertenecer a un dominio a la vez.

### Etiquetas vs. Términos del Glosario vs. Dominios

DataHub admite etiquetas, términos de glosario y dominios como distintos tipos de metadatos que son adecuados para fines específicos:

*   **Etiquetas**: Etiquetas informales y poco controladas que sirven como herramienta para la búsqueda y el descubrimiento. Los activos pueden tener varias etiquetas. Sin gestión formal y central.
*   **Términos del glosario**: Un vocabulario controlado, con jerarquía opcional. Los términos se utilizan normalmente para estandarizar tipos de atributos a nivel de hoja (es decir, campos de esquema) para la gobernanza. Por ejemplo, (EMAIL_PLAINTEXT)
*   **Dominios**: Un conjunto de categorías de nivel superior. Por lo general, alineado con las unidades de negocio / disciplinas para las que los activos son más relevantes. Gestión centralizada o distribuida. Asignación de dominio único por activo de datos.

## Creación de un dominio

Para crear un dominio, primero vaya al **Dominios** en el menú superior derecho de DataHub. Los usuarios deben tener el privilegio de plataforma
llamado `Manage Domains` para ver esta ficha, que se puede conceder mediante la creación de una nueva plataforma [Política](./policies.md).

![](./imgs/domains-tab.png)

Una vez que esté en la página Dominios, verá una lista de todos los Dominios que se han creado en DataHub. Además, puede
ver el número de entidades dentro de cada dominio.

![](./imgs/list-domains.png)

Para crear un nuevo dominio, haga clic en '+ Nuevo dominio'.

![](./imgs/create-domain.png)

Dentro del formulario, puede elegir un nombre para su nombre. La mayoría de las veces, esto se alineará con sus unidades de negocio o grupos, por ejemplo.
'Ingeniería de Plataformas' o 'Marketing Social'. También puede agregar una descripción opcional. No te preocupes, esto se puede cambiar más tarde.

#### Avanzado: Establecer un identificador de dominio personalizado

Haga clic en 'Avanzado' para mostrar la opción de establecer un ID de dominio personalizado. El identificador de dominio determina lo que aparecerá en la 'urna' de DataHub (clave principal)
para el dominio. Esta opción es útil si tiene la intención de hacer referencia a Dominios por un nombre común dentro de su código, o si desea el principal
clave para ser legible por humanos. Proceda con precaución: una vez que seleccione una identificación personalizada, no se puede cambiar fácilmente.

![](./imgs/set-domain-id.png)

De forma predeterminada, no necesita preocuparse por esto. DataHub generará automáticamente un ID de dominio único para usted.

Una vez que haya elegido un nombre y una descripción, haga clic en 'Crear' para crear el nuevo dominio.

## Asignación de un activo a un dominio

Para asignar un activo a un dominio, simplemente navegue a la página de perfil del activo. En la barra de menú inferior izquierda,
consulte la sección 'Dominio'. Haga clic en 'Establecer dominio' y, a continuación, busque el dominio al que desea agregar. Cuando haya terminado, haga clic en 'Agregar'.

![](./imgs/set-domain.png)

Para eliminar un recurso de un dominio, haga clic en el icono 'x' de la etiqueta Dominio.

> Aviso: Agregar o quitar un recurso de un dominio requiere el `Edit Domain` Privilegio de metadatos, que se puede conceder
> lyna [Política](./policies.md).

## Búsqueda por dominio

Una vez que hayas creado un dominio, puedes usar la barra de búsqueda para encontrarlo.

![](./imgs/search-domain.png)

Al hacer clic en el resultado de la búsqueda, accederá al perfil del dominio, donde
puede editar su descripción, agregar / eliminar propietarios y ver los activos dentro del Dominio.

![](./imgs/domain-entities.png)

Una vez que haya agregado activos a un dominio, puede filtrar los resultados de búsqueda para limitarlos a esos activos.
dentro de un dominio en particular utilizando los filtros de búsqueda del lado izquierdo.

![](./imgs/search-by-domain.png)

En la página de inicio, también encontrará una lista de los dominios más populares de su organización.

![](./imgs/browse-domains.png)

## API de dominios

Los dominios se pueden administrar a través del [GraphQL API](https://datahubproject.io/docs/api/graphql/overview/). A continuación encontrará las consultas y mutaciones relevantes
para lograrlo.

*   [dominio](https://datahubproject.io/docs/graphql/queries#domain) - Obtener los detalles de un dominio por Urna.
*   [listaDominios](https://datahubproject.io/docs/graphql/queries#listdomains) - Enumere todos los dominios dentro de su instancia de DataHub.
*   [createDomain](https://datahubproject.io/docs/graphql/mutations#createdomain) - Crear un nuevo Dominio.
*   [setDomain](https://datahubproject.io/docs/graphql/mutations#setdomain) - Añadir una entidad a un dominio
*   [unsetDomain](https://datahubproject.io/docs/graphql/mutations#unsetdomain) - Eliminar una entidad de un dominio.

### Ejemplos

**Creación de un dominio**

```graphql
mutation createDomain {
  createDomain(input: { name: "My New Domain", description: "An optional description" })
}
```

Esta consulta devolverá un `urn` que puede usar para obtener los detalles del dominio.

**Obtención de un dominio por urna**

```graphql
query getDomain {
  domain(urn: "urn:li:domain:engineering") {
    urn
    properties {
        name 
        description
    }
    entities {
			total
    }
  }
}
```

**Agregar un conjunto de datos a un dominio**

```graphql
mutation setDomain {
  setDomain(entityUrn: "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)", domainUrn: "urn:li:domain:engineering")
}
```

> ¡Consejo profesional! Puede probar las consultas de muestra visitando `<your-datahub-url>/api/graphiql`.

## Demo

Clic [aquí](https://www.loom.com/share/72b3bcc2729b4df0982fa63ae3a8cb21) para ver una demostración completa de la función Dominios.

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, ¡comunícate con Slack!
