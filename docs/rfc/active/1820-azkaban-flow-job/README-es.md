*   Fecha de inicio: 2020-08-25
*   RFC PR: https://github.com/datahub-project/datahub/pull/1820
*   Pr(s) de implementación: https://github.com/datahub-project/datahub/pull/1732

# Flujos y empleos de Azkaban

## Resumen

Adición de compatibilidad con [Azkaban](https://azkaban.github.io/) metadatos de trabajo y flujo y habilitación de la búsqueda y el descubrimiento de los mismos.

El diseño incluye los metadatos necesarios para representar los trabajos y flujos de Azkaban como entidades de trabajo de datos y sus relaciones con otros
entidades como Datasets.

## Motivación

Azkaban es un popular gestor de flujo de trabajo de código abierto creado y ampliamente utilizado en LinkedIn. Los metadatos de Azkaban son una pieza crítica
en el gráfico de metadatos, ya que los trabajos de procesamiento de datos son el principal impulsor del movimiento y la creación de datos.

Sin metadatos de trabajo, no es posible comprender el flujo de datos en una organización. Además, se necesitan puestos de trabajo en el
gráfico de linaje para mostrar metadatos operativos y tener una visión completa del movimiento y procesamiento de datos. Captura de trabajos y flujos
Los metadatos en el gráfico de linaje también permiten comprender la dependencia entre múltiples flujos y trabajos y la estructura de los datos
canalizaciones en flujo de datos de extremo a extremo.

## Requisitos

Los siguientes requisitos existen como parte de este rfc:

*   Definir el flujo de datos y el trabajo como entidades y metadatos de modelo para el trabajo y los flujos de datos de azkaban
*   Habilite la búsqueda y el descubrimiento de trabajos y flujos de datos
*   Vincule entidades DataJob a entidades existentes como Datasets para crear un gráfico de metadatos más completo
*   Derivar automáticamente el linaje ascendente del conjunto de datos a partir de metadatos de trabajo de datos (entradas y salidas)

## No requisitos

Azkaban tiene su propia aplicación para mostrar trabajos, flujos, metadatos operativos y registros de trabajos. DataHub no pretende ser
un reemplazo para él. Los usuarios aún deberán ir a la interfaz de usuario de Azkaban para ver los registros y los problemas de depuración. DataHub solo mostrará
metadatos importantes y de alto nivel en el contexto de la búsqueda, el descubrimiento y la exploración, incluido el linaje, y se vincularán a
Interfaz de usuario de Azkaban para una mayor depuración o información más detallada.

## Diseño detallado

![high level design](graph.png)

El diagrama gráfico anterior muestra las relaciones y los metadatos de alto nivel asociados con las entidades Trabajo de datos y Flujo.

Un flujo de Azkaban es un DAG de uno o más trabajos de Azkaban. Por lo general, la mayoría de los trabajos de procesamiento de datos consumen una o más entradas y
producir uno de más resultados (representados por conjuntos de datos en el diagrama). También puede haber otros tipos de trabajos de limpieza.
como trabajos de limpieza que no tienen ningún procesamiento de datos involucrado.

En el diagrama anterior, el nodo de trabajo de Azkaban consume conjuntos de datos `ds1` y `ds2` y produce `ds3`. También está vinculado a la
flujo del que forma parte. Como se muestra en el diagrama, el linaje ascendente del conjunto de datos se deriva de los metadatos del trabajo azkaban que resultan
en `ds1` y `ds2` estar aguas arriba de `ds3`.

### Entidades

Habrá 2 GMA de nivel superior [Entidades](../../../what/entity.md) en el diseño: DataJob y DataFlow.

### Representación DE URN

Definiremos dos [USNR](../../../what/urn.md): `DataJobUrn` y `DataFlowUrn`.
Estas URN deben permitir una identificación única para un trabajo y un flujo de datos, respectivamente.

DataFlow URN constará de las siguientes partes:

1.  Tipo de gestor de flujo de trabajo (por ejemplo, azkaban, flujo de aire, etc.)
2.  Identificador de flujo: identificador de un flujo único dentro de un clúster
3.  Clúster: clúster donde se implementa/ejecuta el flujo

DataJob URN constará de las siguientes partes:

1.  Urna de flujo: urna del flujo de datos del que forma parte este trabajo
2.  ID de trabajo: identificador único del trabajo dentro del flujo

Un ejemplo de URN de DataFlow se verá como se muestra a continuación:

    urn:li:dataFlow:(azkaban,flow_id,cluster)

Un ejemplo de DATAJob URN se verá como se muestra a continuación:

    urn:li:dataJob:(urn:li:dataFlow:(azkaban,flow_id,cluster),job_id)

### Metadatos de Azkaban Flow

A continuación se muestra una lista de metadatos que se pueden asociar con un flujo azkaban:

*   Proyecto para el flujo (el concepto de proyecto puede no existir para otros gestores de flujo de trabajo, por lo que puede no aplicarse en todos los casos)
*   Nombre del flujo
*   Propiedad

### Metadatos de Azkaban Job

A continuación se muestra una lista de metadatos que se pueden asociar con un trabajo de azkaban:

*   Nombre del puesto
*   Tipo de trabajo (podría ser spark, mapreduce, hive, presto. comando, etc.)
*   Entradas consumidas por el trabajo
*   Resultados producidos por el trabajo

## Estrategia de implementación / adopción

El diseño hace referencia a Azkaban de código abierto, por lo que es adoptable por cualquier persona que use Azkaban como su
gestor de flujo de trabajo.

## Labor futura

1.  Adición de metadatos operativos asociados a entidades de Azkaban
2.  Agregar referencias azkaban en el linaje Ascendente para que los trabajos aparezcan en el gráfico de linaje
