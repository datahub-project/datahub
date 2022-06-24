*   Fecha de inicio: (lléname con la fecha de hoy, 2022-02-22)
*   RFC PR: https://github.com/datahub-project/datahub/pull/4237
*   Problema de discusión: (problema de GitHub en el que se discutió antes del RFC, si lo hubiera)
*   PR(s) de implementación: (déjelo vacío)

# Extender modelo de datos a entidad de bloc de notas de modelo

## Fondo

[Libro de consultas](https://www.querybook.org/) es el IDE de big data de código abierto de Pinterest a través de una interfaz de portátil.
Nosotros (Salud incluida) lo aprovechamos como nuestra principal herramienta de consulta. Tiene una función, DataDoc, que organiza el texto enriquecido,
consultas y gráficos en un bloc de notas para documentar fácilmente los análisis. Las personas podrían trabajar en colaboración con otros en un
DataDoc y obtener actualizaciones en tiempo real. Creemos que sería valioso ingerir los metadatos de DataDoc a Datahub y hacer
es fácilmente buscable y descubrible por otros.

## Resumen

Este RFC propone el modelo de datos utilizado para modelar la entidad DataDoc. No habla de ninguna arquitectura, API u otra
detalles de implementación. Este RFC solo incluye un modelo de datos mínimo que podría cumplir con nuestro objetivo inicial. Si la comunidad
decide adoptar esta nueva entidad, se necesitan más esfuerzos.

## Diseño detallado

### Modelo DataDoc

![DataDoc High Level Model](DataDoc-high-level-model.png)

Como se muestra en el diagrama anterior, DataDoc es un documento que contiene una lista de celdas datadoc. Organiza texto enriquecido,
consultas y gráficos en un bloc de notas para documentar fácilmente los análisis. Pudimos ver que el modelo DataDoc es muy similar a
Cuaderno. DataDoc se vería como un subconjunto de Notebook. Por lo tanto, vamos a modelar Notebook en lugar de DataDoc.
Incluiremos el aspecto "subTypes" para diferenciar Notebook y DataDoc

### Modelo de datos de notebook

Esta sección habla sobre el modelo de datos mínimo de Notebook que podría satisfacer nuestras necesidades.

*   notebookKey (keyAspect)
    *   notebookTool: el nombre de la herramienta DataDoc como QueryBook, Notebook, etc.
    *   notebookId: Identificador único para DataDoc
*   notebookInfo
    *   title(Searchable): El título de este DataDoc
    *   description(Searchable): Descripción detallada sobre dataDoc
    *   lastModified: captura información sobre quién creó/modificó/eliminó por última vez este DataDoc y cuándo
*   notebookContenido
    *   contenido: El contenido de un DataDoc que está compuesto por una lista de DataDocCell
*   editableDataDocProperties
*   propiedad
*   estado
*   globalEtiquetas
*   institucionalMemoria
*   browsePaths
*   Dominios
*   Subtipos
*   dataPlatformInstance
*   glosarioTérminos

### Celdas de bloc de notas

La celda Notebook es la unidad que compone un Notebook. Hay tres tipos de celdas: Celda de texto, Celda de consulta, Celda de gráfico. Cada
el tipo de celda tiene sus propios metadatos. Dado que la celda solo vive dentro de un Notebook, modelamos las celdas como un aspecto de Notebook
en lugar de otra entidad. Estos son los metadatos de cada tipo de celda:

*   TextCell
    *   cellTitle: Título de la celda
    *   cellId: identificador único para la celda.
    *   lastModified: captura información sobre quién creó/modificó/eliminó por última vez esta celda del bloc de notas y cuándo
    *   texto: el texto real de un Objeto TextCell en un bloc de notas
*   QueryCell
    *   cellTitle: Título de la celda
    *   cellId: identificador único para la celda.
    *   lastModified: captura información sobre quién creó/modificó/eliminó por última vez esta celda del bloc de notas y cuándo
    *   rawQuery: consulta sin formato para explicar alguna lógica específica en un bloc de notas
    *   lastExecuted: captura información sobre quién ejecutó por última vez esta celda de consulta y cuándo
*   ChartCell
    *   cellTitle: Título de la celda
    *   cellId: identificador único para la celda.
    *   lastModified: captura información sobre quién creó/modificó/eliminó por última vez esta celda del bloc de notas y cuándo

## Labor futura

Querybook proporciona una característica incrustable. Podríamos incrustar una pestaña de consulta que utilice la función incrustada en Datahub
que proporcionan una experiencia de búsqueda y exploración al usuario.
