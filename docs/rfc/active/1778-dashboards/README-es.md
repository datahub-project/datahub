*   Fecha de inicio: 2020-08-03
*   RFC PR: https://github.com/datahub-project/datahub/pull/1778
*   PR(s) de implementación: https://github.com/datahub-project/datahub/pull/1775

# Paneles

## Resumen

Agregar soporte para la catalogación de metadatos de paneles (y gráficos) y habilitar la búsqueda y el descubrimiento para ellos.
El diseño debe adaptarse a diferentes tableros ([Guapa](https://looker.com), [Redash](https://redash.io/)) herramientas utilizadas dentro de una empresa.

## Motivación

Los cuadros de mando son una pieza clave dentro de un ecosistema de datos de una empresa. Son utilizados por diferentes grupos de empleados en diferentes organizaciones.
Proporcionan una forma de visualizar algunos activos de datos (conjuntos de datos de seguimiento o métricas) al permitir la división y el corte de la fuente de datos de entrada.
Cuando una empresa escala, los activos de datos, incluidos los paneles, se enriquecen y crecen. Por lo tanto, es importante encontrar y acceder al panel de control correcto.

## Metas

Al tener paneles de control como una entidad de nivel superior en DataHub, logramos los siguientes objetivos:

*   Habilitación de la búsqueda y detección de activos de panel mediante el uso de metadatos de panel
*   Vincule los paneles a las fuentes de datos subyacentes y tenga una imagen más completa del linaje de datos

## No objetivos

DataHub solo servirá como catálogo para paneles donde los usuarios buscan paneles mediante el uso de palabras clave.
La página de entidad de un panel puede contener vínculos al panel para dirigir a los usuarios a ver el panel después de encontrarlo.
Sin embargo, DataHub no intentará mostrar el panel real ni ningún gráfico dentro de eso. Esto no se desea y no debe permitirse porque:

*   Los paneles o gráficos dentro de un panel pueden tener diferentes ACL que evitan que los usuarios sin el permiso necesario para mostrar el panel.
    En general, la fuente de la verdad para estas ACL son las herramientas de tablero.
*   Los orígenes de datos subyacentes también pueden tener algunas ACL. Una vez más, la fuente de verdad para estas ACL son plataformas de datos específicas.

## Diseño detallado

![high level design](high_level_design.png)

Como se muestra en el diagrama anterior, los paneles se componen de una colección de gráficos a un nivel muy alto. Estos gráficos
podría ser compartido por diferentes paneles. En el ejemplo esbozado anteriormente, `Chart_1`, `Chart_2` y `Chart_3` forman parte de
`Dashboard_A` y `Chart_3` y `Chart_4` forman parte de `Dashboard_B`.

### Entidades

Habrá 2 GMA de nivel superior [Entidades](../../../what/entity.md) en el diseño: cuadros de mando y gráficos.
Es importante hacer gráficos como una entidad de nivel superior porque los gráficos podrían compartirse entre diferentes paneles.
Tendremos que construir `Contains` relaciones entre las entidades Dashboard y Chart.

### Representación DE URN

Definiremos dos [Urnas](../../../what/urn.md): `DashboardUrn` y `ChartUrn`.
Estas URL deben permitir una identificación única para paneles y gráficos, incluso si hay múltiples herramientas de tableros.
se utilizan dentro de una empresa. La mayoría de las veces, los paneles y gráficos reciben identificadores únicos de la herramienta de tablero utilizada.
Un ejemplo de URN de panel para Looker se verá como se muestra a continuación:

    urn:li:dashboard:(Looker,<<dashboard_id>>)

Un ejemplo de URN de gráfico para Redash se verá como se muestra a continuación:

    urn:li:chart:(Redash,<<chart_id>>)

### Metadatos de gráficos

Las herramientas de dashboarding generalmente tienen una jerga diferente para denotar un gráfico.
Se llaman como [Mirar](https://docs.looker.com/exploring-data/saving-and-editing-looks) en Looker
y [Visualización](https://redash.io/help/user-guide/visualizations/visualization-types) en Redash.
Pero, independientemente del nombre, los gráficos son los diferentes mosaicos que existen en un tablero.
Los gráficos se utilizan principalmente para entregar cierta información visualmente para que sea fácilmente comprensible.
Es posible que estén utilizando uno o varios orígenes de datos y, por lo general, tengan una consulta asociada ejecutándose en
la fuente de datos subyacente para generar los datos que presentará.

A continuación se muestra una lista de metadatos que se pueden asociar con un gráfico:

*   Título
*   Descripción
*   Tipo (gráfico de barras, gráfico circular, gráfico de dispersión, etc.)
*   Fuentes de entrada
*   Consulta (y su tipo)
*   Nivel de acceso (público, privado, etc.)
*   Propiedad
*   Estado (eliminado o no)
*   Información de auditoría (última modificación, última actualización)

### Metadatos del panel

Además de contener un conjunto de gráficos, los paneles llevan metadatos adjuntos a ellos.
A continuación se muestra una lista de metadatos que se pueden asociar con un panel:

*   Título
*   Descripción
*   Lista de gráficos
*   Nivel de acceso (público, privado, etc.)
*   Propiedad
*   Estado (eliminado o no)
*   Información de auditoría (última modificación, última actualización)

### Gráfico de metadatos

![dashboards_graph](dashboards_graph.png)

Un gráfico de metadatos de ejemplo que muestra la imagen completa del linaje de datos se muestra arriba.
En esta imagen, `Dash_A` y `Dash_B` son paneles de control y están conectados a gráficos a través de `Contains` Bordes.
`C1`, `C2`, `C3` y `C4` son gráficos y están conectados a conjuntos de datos subyacentes a través de `DownstreamOf` Bordes.
`D1`, `D2` y `D3` son conjuntos de datos.

## Cómo enseñamos esto

Debemos crear/actualizar guías de usuario para educar a los usuarios para:

*   Experiencia de búsqueda y descubrimiento (cómo encontrar un panel en DataHub)
*   Experiencia de linaje (cómo encontrar conjuntos de datos ascendentes de un panel y cómo encontrar paneles generados a partir de un conjunto de datos)

## Estrategia de implementación / adopción

Se supone que el diseño es lo suficientemente genérico como para que cualquier usuario de DataHub pueda hacerlo fácilmente.
para incorporar los metadatos de su panel a DataHub, independientemente de su plataforma de paneles.

Lo único que los usuarios tendrán que hacer es escribir un script ETL personalizado para su
plataforma de dashboarding (si aún no se proporciona en dataHub repo). Este script ETL:

*   Extraiga los metadatos de todos los paneles y gráficos disponibles mediante las API de la plataforma de paneles
*   Construir y emitir estos metadatos en forma de [MCEs](../../../what/mxe.md)

## Preguntas no resueltas (tareas pendientes)

1.  Agregaremos funciones sociales como suscribirse y seguir más adelante. Sin embargo, está fuera del alcance de este RFC.
