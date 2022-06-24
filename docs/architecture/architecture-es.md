***

## título: "Overview"

# Descripción general de la arquitectura de DataHub

DataHub es un [3ª generación](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained) Plataforma de metadatos que permite el descubrimiento de datos, la colaboración, la gobernanza y la observabilidad de extremo a extremo
que está diseñado para la pila de datos moderna. DataHub emplea una filosofía de modelo primero, con un enfoque en desbloquear la interoperabilidad entre
herramientas y sistemas dispares.

La siguiente figura describe la arquitectura de alto nivel de DataHub.

![datahub-architecture](../imgs/datahub-architecture.png)

Para una visión más detallada de los componentes que componen la arquitectura, consulte [Componentes](../components.md).

## Aspectos destacados de la arquitectura

Hay tres aspectos destacados principales de la arquitectura de DataHub.

### Enfoque de esquema primero para el modelado de metadatos

El modelo de metadatos de DataHub se describe mediante un [lenguaje agnóstico de serialización](https://linkedin.github.io/rest.li/pdl_schema). Ambos [REPOSO](../../metadata-service) así como [GraphQL API-s](../../datahub-web-react/src/graphql) son compatibles. Además, DataHub admite un [API basada en AVRO](../../metadata-events) sobre Kafka para comunicar los cambios de metadatos y suscribirse a ellos. Nuestro [hoja de ruta](../roadmap.md) incluye un hito para admitir ediciones de modelos de metadatos sin código muy pronto, lo que permitirá una mayor facilidad de uso, al tiempo que conserva todos los beneficios de una API tipada. Lea acerca del modelado de metadatos en [modelado de metadatos][metadata modeling].

### Plataforma de metadatos en tiempo real basada en secuencias

La infraestructura de metadatos de DataHub está orientada a la transmisión, lo que permite que los cambios en los metadatos se comuniquen y reflejen dentro de la plataforma en cuestión de segundos. También puede suscribirse a los cambios que se producen en los metadatos de DataHub, lo que le permite crear sistemas basados en metadatos en tiempo real. Por ejemplo, puede crear un sistema de control de acceso que pueda observar un conjunto de datos previamente legible en todo el mundo agregando un nuevo campo de esquema que contenga PII y bloquee ese conjunto de datos para revisiones de control de acceso.

### Servicio de metadatos federados

DataHub viene con una sola [servicio de metadatos (GMS)](../../metadata-service) como parte del repositorio de código abierto. Sin embargo, también admite servicios de metadatos federados que pueden ser propiedad y operados por diferentes equipos, de hecho, así es como LinkedIn ejecuta DataHub internamente. Los servicios federados se comunican con el índice y el gráfico de búsqueda central mediante Kafka, para admitir la búsqueda y el descubrimiento globales al tiempo que permiten la propiedad disociada de los metadatos. Este tipo de arquitectura es muy susceptible para las empresas que están implementando [malla de datos](https://martinfowler.com/articles/data-monolith-to-mesh.html).

[metadata modeling]: ../modeling/metadata-model.md

[PDL]: https://linkedin.github.io/rest.li/pdl_schema

[metadata architectures blog post]: https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained

[datahub-serving]: metadata-serving.md

[datahub-ingestion]: metadata-ingestion.md

[react-frontend]: ../../datahub-web-react/README.md
