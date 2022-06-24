*   Fecha de inicio: 1/12/2020
*   RFC PR: 2055
*   Pr(s) de implementación: N/A

# Propuesta para incubar una nueva aplicación React

## Propuesta

En este documento, proponemos la incubación de una nueva aplicación React dentro del repositorio DataHub. La "incubación" implica un desarrollo iterativo por parte de la comunidad a lo largo del tiempo, a diferencia de una reescritura del Big Bang, que no es práctica dado el alcance del trabajo.

Comenzaremos describiendo las motivaciones para esta propuesta, seguido de una caracterización de los principios de diseño y los requisitos funcionales, y concluiremos con una mirada a la arquitectura propuesta. Omitiremos en gran medida los detalles específicos de implementación de este RFC, que se dejarán a los RFC + PR posteriores.

## Metas

El objetivo de este RFC es obtener la aceptación de la comunidad en el desarrollo de una aplicación React que existirá en paralelo a la aplicación Ember existente dentro del repositorio de DataHub.

## No objetivos

Lo siguiente se omite del alcance de este RFC

*   Implementación del lado del servidor de GraphQL (Play Server versus servidor separado)
*   Arquitectura de componentes React específica
*   Opciones específicas de tecnología / herramientas dentro del ecosistema React (administración estatal, cliente, etc.)

## Motivación

La principal motivación detrás del desarrollo de una nueva aplicación React es mejorar el alcance y la accesibilidad de DataHub. No es ningún secreto que React es una tecnología mucho más popular que Ember por los números:

*   Estrellas de React GitHub: ~160k
*   Estrellas de Ember GitHub: ~20k

La adopción de una pila más familiar facilitará una comunidad activa al reducir la barrera a la contribución, además de proporcionar acceso a un ecosistema más rico.

Una motivación secundaria es que un nuevo cliente nos permitiría abordar la deuda tecnológica presente en la aplicación Ember existente, incluyendo

*   **Código heredado y no utilizado**: Existe una lógica de manejo especial para admitir versiones heredadas de DataHub (es decir. WhereHows). Un ejemplo de esto se puede encontrar en [legacy.ts](https://github.com/datahub-project/datahub/blob/master/datahub-web/@datahub/data-models/addon/entity/dataset/utils/legacy.ts). Además, hay código que no se usa en el cliente OSS, como el relacionado con Dataset [conformidad](https://github.com/datahub-project/datahub/blob/master/datahub-web/packages/data-portal/app/utils/datasets/compliance-suggestions.ts). Un nuevo cliente proporcionará beneficios de legibilidad, careciendo de equipaje histórico.

*   **Dificultad de extensión**Dada la falta de orientación formal, la curva de aprendizaje empinada para Ember (y la estructura del complemento) y la presencia de código heredado / no utilizado, no es trivial extender el cliente web existente.

*   **Dificultad de personalización**: Hay una falta de palancas de personalización claras para modificar la aplicación Ember. Debido a que DataHub se implementa en una variedad de organizaciones diferentes, sería útil admitir la personalización de
    *   Tema: Cómo se ve (color, ux, activos, copia)
    *   Características: Cómo se comporta (habilitar / deshabilitar funciones)

        ¡fuera de la caja!

*   **Acoplamiento con GMA**: Conceptos GMA de [entidad](https://github.com/datahub-project/datahub/blob/master/datahub-web/@datahub/data-models/addon/entity/base-entity.ts) y [aspecto](https://github.com/datahub-project/datahub/blob/master/datahub-web/@datahub/data-models/addon/entity/utils/aspects.ts) están arraigados en el cliente Ember. Con el nuevo cliente, podemos revisar las abstracciones expuestas al lado del cliente y buscar oportunidades para simplificar.

Una pizarra limpia nos permitirá abordar estos elementos, mejorando la experiencia de desarrollo de frontend y facilitando la contribución de la comunidad.

Es importante tener en cuenta que no estamos proponiendo la obsolescencia del cliente de Ember en este momento. El mantenimiento y el desarrollo de funciones deben ser gratuitos para continuar en Ember a medida que la aplicación React evoluciona de forma aislada.

### Principios de diseño

Al desarrollar la nueva aplicación, es importante que tengamos un conjunto acordado de principios de diseño para guiar nuestras decisiones.

Tales principios deben promover la salud de la comunidad (por ejemplo, aumentando la probabilidad de contribución) y la propuesta de valor del producto DataHub para las organizaciones (por ejemplo, permitiendo la modificación específica del dominio de la implementación predeterminada).

Específicamente, el nuevo cliente debe ser

1.  **Extensible**

*   Arquitectura modular y componible
*   Orientación formal sobre la ampliación del cliente para satisfacer las necesidades específicas del dominio

2.  **Configurable**

*   Palancas claras, coherentes y documentadas para alterar el estilo y el comportamiento entre las implementaciones de DataHub
*   Admite la inyección de 'applets' o 'widgets' personalizados cuando corresponda

3.  **Escalable**

*   Una arquitectura adecuada para la escala, tanto a lo largo de las dimensiones de las personas como de las características.
*   ¡Fácil de contribuir!

Estos principios deben servir como criterios de evaluación utilizados por los autores y revisores de los cambios en la aplicación.

### Requisitos funcionales

#### A corto plazo

Inicialmente, nuestro objetivo es lograr la paridad funcional con el frontend Ember existente para casos de uso comunes. Específicamente, la aplicación React debería ser compatible

*   Autenticación de un usuario
*   Visualización de entidades de metadatos
*   Actualización de entidades de metadatos
*   Examinar entidades de metadatos
*   Búsqueda de entidades de metadatos
*   Administración de una cuenta de usuario

Los detalles más finos de qué entidades caen en cada cubo de características serán dictados por las necesidades de la comunidad, con el hito a corto plazo para lograr la paridad con las entidades que aparecen en el cliente Ember (Datasets, CorpUsers).

#### A largo plazo

A largo plazo, trabajaremos con la comunidad para definir una hoja de ruta funcional más extensa, que puede incluir

*   Proporcionar una ruta de migración de la aplicación Ember a la aplicación React
*   Nuevas entidades, aspectos, operaciones (por ejemplo. Cuadros de mando, gráficos, etc.)
*   'extensiones' o 'applets' personalizados y controlados por el servidor para mostrar en la interfaz de usuario
*   Panel de administración
*   Recopilación de métricas
*   Características sociales
    ¡Y más!

### Arquitectura

La siguiente figura muestra la arquitectura actualizada de DataHub dada esta propuesta:

![react-architecture](./react-architecture.png)

Donde las casillas delineadas en verde denotan componentes recién introducidos.

Tenga en cuenta que la aplicación será completamente independiente del cliente Ember existente, lo que significa que no hay riesgos de compatibilidad para las implementaciones existentes. Además, la aplicación React se comunicará exclusivamente con un servidor GraphQL (Ver [RFC 2042](https://github.com/datahub-project/datahub/pulls?q=is%3Apr+is%3Aclosed) para la propuesta). Esto mejorará la experiencia de desarrollo de frontend al proporcionar

*   un contrato de API claramente definido
*   administración de estado simplificada (a través del cliente Apollo GQL, no se requiere redux)
*   modelos generados automáticamente para consultas y tipos de datos

Ese es el alcance de los detalles técnicos que cubriremos por ahora. Estén atentos para una prueba de concepto de relaciones públicas próximamente que presentará un shell inicial de React.

## Cómo enseñamos esto

Un objetivo importante de esta iniciativa es desarrollar un cliente web frontend que pueda ser fácilmente extendido por la comunidad de DataHub. Con ese fin, proporcionaremos documentación que detalle el proceso de cambio del cliente frontend para hacer cosas como:

*   Agregar una nueva página de entidad
*   Ampliar una página de entidad existente
*   Habilitar / deshabilitar funciones específicas
*   Modificar configuraciones
*   Probar nuevos componentes
    ¡Y más!

## Alternativas

### Evolucione la aplicación Ember en su lugar

*¿Qué?*: Iterar en el appclient Ember existente.

*¿Por qué no?* En primer lugar, en realidad no consideramos que esto sea mutuamente excluyente con la introducción de una aplicación React separada. En cualquier caso, hay beneficios al adoptar una tecnología más accesible como React que no cambian con mejoras en la aplicación Ember existente.

### Mezclando Ember & React

*¿Qué?*: Migre de Ember a React de forma incremental reemplazando incrementalmente los componentes de Ember con componentes de React.

*¿Por qué no?*El estado intermedio de una aplicación de mitad de reacción y mitad de brasa es algo en lo que preferimos no pensar, es aterrador y triste. Nos gustaría evitar degradar la experiencia del desarrollador del lado del cliente con este tipo de complejidad. Dado que esta migración llevará algún tiempo, creemos que es más productivo iterar de forma independiente.

## Estrategia de implementación / adopción

Como se describió anteriormente, el despliegue del frontend de React será iterativo. A corto plazo, las implementaciones existentes continuarán utilizando Ember. A largo plazo, las organizaciones serán libres de validar y migrar al nuevo cliente a su propio ritmo.

## Preguntas abiertas

**¿Podemos reutilizar el código del cliente Ember?**

Gran pregunta :) Sí, debemos tratar activamente de extraer la mayor cantidad de código común posible de Ember (probablemente componentes de interfaz de usuario compartidos), siempre y cuando se ajuste a los principios establecidos anteriormente. Se espera que esto acelere el proceso de desarrollo y permita mejoras en ambos clientes al mismo tiempo.

**¿Qué entidades de GMS deberían aparecer en el nuevo frontend? ¿Qué operaciones de actualización?**

¡Esto es algo que buscaremos en la comunidad para ayudar a definir! Inicialmente, apuntaremos a la paridad funcional con la aplicación Ember, que hoy es compatible

*   leer Dataset & CorpUser
*   escribir ciertos aspectos del conjunto de datos (por ejemplo, propiedad)
