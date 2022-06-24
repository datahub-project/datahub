*   Fecha de inicio: 18/08/2020
*   RFC PR: https://github.com/datahub-project/datahub/pull/1812
*   Pr(s) de implementación: https://github.com/datahub-project/datahub/pull/1721

# Modelos de aprendizaje automático

## Resumen

Agregar soporte para modelos de aprendizaje automático entrenados y características de catalogación de metadatos y permitir la búsqueda y el descubrimiento sobre ellos. Este es un paso hacia la organización de los hechos esenciales de los modelos de aprendizaje automático de una manera estructurada que conduce a la democratización responsable de la máquina.
aprendizaje y tecnología de inteligencia artificial relacionada. El trabajo está inspirado en la tarjeta modelo de Google [papel](https://arxiv.org/pdf/1810.03993.pdf).

## Motivación

Necesitamos modelar los metadatos del modelo de ML para generar informes de modelos transparentes. A continuación se presentan algunas de las razones por las que es importante almacenar metadatos del modelo de aprendizaje automático:

*   Búsqueda y descubrimiento de modelos de ML entrenados en toda una organización.
*   Trazar límites en torno a las capacidades y limitaciones de un modelo: Existe la necesidad de almacenar las condiciones en las que un modelo funciona mejor y de manera más consistente y si tiene algunos puntos ciegos. Ayuda a los usuarios potenciales de los modelos a estar mejor informados sobre qué modelos son los mejores para sus propósitos específicos. Además, ayuda a minimizar el uso de modelos de aprendizaje automático en contextos para los que no son adecuados.
*   Métricas y limitaciones: El rendimiento de un modelo se puede medir de innumerables maneras, pero necesitamos catalogar las métricas que son más relevantes y útiles. Del mismo modo, existe la necesidad de almacenar las limitaciones potenciales de un modelo que son más útiles de cuantificar.
*   Garantizar la comparabilidad entre modelos de una manera bien informada: el modelado de metadatos de modelos de ML nos permite comparar los resultados de los modelos candidatos no solo a través de las métricas de evaluación tradicionales, sino también a lo largo de los ejes de ética, inclusión y equidad.
    Consideraciones.
*   Promover la reproducibilidad: A menudo, un modelo se entrena en datos transformados, hay algunos pasos de preprocesamiento involucrados en la transformación de los datos, por ejemplo, centrado, escalado, manejo de valores faltantes, etc. Estas transformaciones deben almacenarse como parte de los metadatos del modelo para garantizar la reproducibilidad.
*   Garantizar la gobernanza de los datos: La creciente preocupación pública por la privacidad del consumidor está dando lugar a nuevas leyes de datos, como GDPR y CCPA, lo que hace que las empresas fortalezcan sus esfuerzos de gobierno y cumplimiento de datos. Por lo tanto, existe la necesidad de almacenar información de cumplimiento de los modelos de ML que contienen PII o datos condidenciales (a través de etiquetas manuales o procesos automatizados) para eliminar el riesgo de exposición a datos confidenciales.

## Diseño detallado

![high level design](high_level_design.png)

Como se muestra en el diagrama anterior, los modelos de aprendizaje automático utilizan características de aprendizaje automático como entradas. Estas características de aprendizaje automático
podría compartirse a través de diferentes modelos de aprendizaje automático. En el ejemplo esbozado anteriormente, `ML_Feature_1` y `ML_Feature_2` se utilizan como entradas para `ML_Model_A` mientras `ML_Feature_2`, `ML_Feature_3` y `ML_Feature_4` son entradas para `ML_Model_B`.

### Representación DE URN

Definiremos dos [USNR](../../../what/urn.md): `MLModelUrn` y `MLFeatureUrn`.
Estas URN deben permitir la identificación única de modelos y características de aprendizaje automático, respectivamente. Los modelos de aprendizaje automático, como los conjuntos de datos, se identificarán mediante la combinación de la urna de plataforma estandarizada, el nombre del modelo y el tipo de tejido al que pertenece el modelo o donde se generó. Aquí la urna de la plataforma corresponde a la plataforma de datos para los modelos de ML (como TensorFlow): representar la plataforma como una urna nos permite adjuntarle metadatos específicos de la plataforma.

Una URN de modelo de aprendizaje automático se verá como se muestra a continuación:

    urn:li:mlModel:(<<platform>>,<<modelName>>,<<fabric>>)

Una característica de aprendizaje automático se identificará de forma única por su nombre y el espacio de nombres al que pertenece esta característica.
Una URN de característica de aprendizaje automático se verá como se muestra a continuación:

    urn:li:mlFeature:(<<namespace>>,<<featureName>>)

### Entidades

Habrá 2 GMA de nivel superior [Entidades](../../../what/entity.md) en el diseño: modelos de ML y características de ML.
Es importante hacer que las características de ML sean una entidad de nivel superior porque las características de ML podrían compartirse entre diferentes modelos de ML.

### Metadatos del modelo de aprendizaje automático

*   Propiedades del modelo: información básica sobre el modelo de aprendizaje automático
    *   Fecha del modelo
    *   Diseño del modelo
    *   Versión del modelo
    *   Tipo de modelo: Detalles básicos de la arquitectura del modelo, por ejemplo, si es un clasificador Naive Bayes, una red neuronal convolucional, etc.
    *   Características de ML utilizadas para el entrenamiento
    *   Hiperparámetros del modelo, utilizados para controlar el proceso de aprendizaje
    *   Etiquetas: Principalmente para mejorar la búsqueda y el descubrimiento de modelos de ML
*   Propiedad: Usuarios que poseen el modelo de ML, para ayudar a dirigir preguntas o comentarios sobre el modelo.
*   Uso previsto
    *   Principales casos de uso previstos
    *   Tipos de usuario principales previstos
    *   Casos de uso fuera del alcance
*   Factores del modelo: Factores que afectan el rendimiento del modelo, incluidos los grupos, la instrumentación y los entornos.
    *   Factores relevantes: Factores previsibles para los cuales el rendimiento del modelo puede variar
    *   Factores de evaluación: Factores que se informan
*   Métricas: Medidas del rendimiento del modelo que se informa, así como los umbrales de decisión (si los hay) utilizados.
*   Datos de entrenamiento: detalles sobre los conjuntos de datos utilizados para entrenar modelos de APRENDIZAJE
    *   Conjuntos de datos utilizados para entrenar el modelo de APRENDIZAJE
    *   Motivación detrás de la elección de estos conjuntos de datos
    *   Pasos de preprocesamiento involucrados: cruciales para la reproducibilidad
    *   Enlace al proceso/trabajo que captura la ejecución de la capacitación
*   Datos de evaluación: Refleja los datos de entrenamiento.
*   Análisis cuantitativos: Proporciona los resultados de la evaluación del modelo de acuerdo con las métricas elegidas mediante la vinculación al panel de control relevante.
*   Consideraciones éticas: Demostrar las consideraciones éticas que se introdujeron en el desarrollo del modelo, planteando desafíos éticos y soluciones a las partes interesadas.
*   Advertencias y recomendaciones: Captura preocupaciones adicionales con respecto al modelo
    *   ¿Los resultados sugirieron alguna prueba adicional?
    *   Grupos relevantes que no estaban representados en el conjunto de datos de evaluación
    *   Recomendaciones para el uso del modelo
    *   Características ideales de un conjunto de datos de evaluación
*   Código fuente: contiene el código fuente de la canalización de capacitación y evaluación, junto con el código fuente donde se define el modelo de aprendizaje automático.
*   Memoria Institucional: Conocimiento institucional para facilitar la búsqueda y el descubrimiento.
*   Estado: Captura si el modelo se ha eliminado por software o no.
*   Costo: Costo asociado con el modelo basado en el proyecto/componente al que pertenece este modelo.
*   Obsolescencia: captura si el modelo ha quedado obsoleto o no.

### Metadatos de la característica ML

*   Propiedades de la característica: información básica sobre la característica ML
    *   Descripción de la función
    *   Tipo de datos de la función, es decir, booleano, texto, etc. Estos también incluyen [tipos de datos](https://towardsdatascience.com/7-data-types-a-better-way-to-think-about-data-types-for-machine-learning-939fae99a689#:~:text=In%20the%20machine%20learning%20world,groups%20are%20often%20called%20out.) particularmente para los profesionales del aprendizaje automático.
*   Propiedad: Propietarios de la función ML.
*   Memoria Institucional: Conocimiento institucional para facilitar la búsqueda y el descubrimiento.
*   Estado: Captura si la función se ha eliminado suavemente o no.
*   Obsoleto: captura si la función ha quedado obsoleta o no.

### Gráfico de metadatos

![ml_model_graph](ml_model_graph.png)

Un gráfico de metadatos de ejemplo con una imagen completa del linaje de datos se muestra arriba. A continuación se muestran los bordes principales del gráfico

1.  El conjunto de datos de evaluación contiene datos utilizados para análisis cuantitativos y se utiliza para evaluar el modelo de aprendizaje automático, por lo tanto, el modelo de aprendizaje automático está conectado a los conjuntos de datos de evaluación a través de `EvaluatedOn` borde
2.  Los conjuntos de datos de entrenamiento contienen los datos de entrenamiento y se utilizan para entrenar el modelo de APRENDIZAJE, por lo tanto, el modelo de aprendizaje automático está conectado a los conjuntos de datos de entrenamiento a través de `TrainedOn` borde.
3.  El modelo de ML está conectado a `DataProcess` entidad que capta la ejecución de la formación a través de un (recientemente propuesto) `TrainedBy` borde.
4.  `DataProcess` La propia entidad utiliza los conjuntos de datos de entrenamiento (mencionados en 2) como entrada y, por lo tanto, está conectada a los conjuntos de datos de entrenamiento a través de `Consumes` borde.
5.  El modelo de ML está conectado a las funciones de ML a través de `Contains` borde.
6.  Los resultados del rendimiento del modelo de APRENDIZAJE se pueden ver en un panel de control y, por lo tanto, se conectan a `Dashboard` entidad a través de `Produces` borde.

## Cómo enseñamos esto

Debemos crear/actualizar guías de usuario para educar a los usuarios para:

*   Experiencia de búsqueda y descubrimiento (cómo encontrar un modelo de aprendizaje automático en DataHub)
*   Experiencia de linaje (cómo encontrar diferentes entidades conectadas al modelo de aprendizaje automático)

## Alternativas

Un modelo de aprendizaje automático también podría almacenar un ID de modelo que identifique de forma única un modelo de aprendizaje automático en el sistema de gestión del ciclo de vida del modelo de aprendizaje automático. Este puede ser entonces el único componente de `MLModelUrn` Sin embargo, necesitaríamos un sistema para recuperar el nombre del modelo dado el ID del modelo. Por lo tanto, elegimos el enfoque de modelado `MLModelUrn` similar a `DatasetUrn`.

## Estrategia de implementación / adopción

Se supone que el diseño es lo suficientemente genérico como para que cualquier usuario de DataHub pueda ser capaz de hacerlo fácilmente.
para incorporar su modelo de ML y metadatos de características de ML a DataHub, independientemente de su plataforma de aprendizaje automático.

Lo único que los usuarios deberán hacer es escribir un script ETL personalizado para su plataforma de aprendizaje automático (si aún no se proporciona en el repositorio de DataHub). Este script ETL construirá y emitirá metadatos de características de modelo de ML y ML en forma de [MCEs](../../../what/mxe.md).

## Labor futura

*   Este RFC no cubre la evolución / versiones del modelo, vinculando los modelos relacionados entre sí y cómo lo manejaremos, eso requerirá su propio RFC.
*   Este RFC no cubre el diseño de la interfaz de usuario del modelo de APRENDIZAJE y la característica de aprendizaje automático.
*   Este RFC no cubre las funciones sociales como suscribirse y seguir el modelo de ML y / o la función de ML.
