# Hoja de ruta de DataHub

## [¡La hoja de ruta de DataHub tiene un nuevo hogar!](https://feature-requests.datahubproject.io/roadmap)

Por favor, consulte el [nueva hoja de ruta de DataHub](https://feature-requests.datahubproject.io/roadmap) para obtener los detalles más actualizados de lo que estamos trabajando!

*Si tiene sugerencias sobre lo que debemos considerar en ciclos futuros, no dude en enviar un [solicitud de características](https://feature-requests.datahubproject.io/) y / o votar las solicitudes de funciones existentes para que podamos tener una idea del nivel de importancia.*

## Hoja de ruta histórica

*A continuación se muestra el progreso realizado en los puntos históricos de la hoja de ruta a partir de enero de 2022. Para los elementos incompletos de la hoja de ruta, hemos creado solicitudes de funciones para medir el interés y el impacto actual de la comunidad que se considerarán en ciclos futuros. Si ve algo que todavía es de gran interés para usted, vote a través del enlace del portal de solicitud de funciones y suscríbase a la publicación para obtener actualizaciones a medida que avanzamos en el trabajo en ciclos futuros.*

### Q4 2021 \[Octubre - Dic 2021]

#### Integración del ecosistema del lago de datos

*   \[ ] Lago Spark Delta - [Ver en Feature Reqeust Portal](https://feature-requests.datahubproject.io/b/feedback/p/spark-delta-lake)
*   \[ ] Apache Iceberg - [Incluido en la hoja de ruta del primer trimestre de 2022: fuentes de ingesta de metadatos impulsadas por la comunidad](https://feature-requests.datahubproject.io/roadmap/540)
*   \[ ] Apache Hudi - [Ver en el portal de solicitud de características](https://feature-requests.datahubproject.io/b/feedback/p/apachi-hudi-ingestion-support)

#### Marco de activación de metadatos

[Ver en el portal de solicitud de características](https://feature-requests.datahubproject.io/b/User-Experience/p/ability-to-subscribe-to-an-entity-to-receive-notifications-when-something-changes)

*   \[ ] Sensores con estado para el flujo de aire
*   \[ ] Recibir eventos para que usted envíe alertas, correo electrónico
*   \[ ] Integración con Slack

#### Ecosistema de ML

*   \[x] Características (Fiesta)
*   \[x] Modelos (Sagemaker)
*   \[ ] Blocs de notas - Ver en el portal de solicitud de características]\(https://feature-requests.datahubproject.io/admin/p/jupyter-integration)

#### Ecosistema de métricas

[Ver en el portal de solicitud de características](https://feature-requests.datahubproject.io/b/User-Experience/p/ability-to-define-metrics-and-attach-them-to-entities)

*   \[ ] Medidas, Dimensiones
*   \[ ] Relaciones con conjuntos de datos y paneles

#### Características orientadas a Data Mesh

*   \[ ] Modelado de productos de datos
*   \[ ] Análisis para habilitar la malla de datos

#### Colaboración

[Ver en Feature Reqeust Portal](https://feature-requests.datahubproject.io/b/User-Experience/p/collaboration-within-datahub-ui)

*   \[ ] Conversaciones en la plataforma
*   \[ ] Puestos de conocimiento (Gdocs, Gslides, Gsheets)

### Q3 2021 \[Julio - Septiembre 2021]

#### Creación de perfiles de datos y vistas previas de conjuntos de datos

Caso de uso: Consulte los datos de muestra para un conjunto de datos y las estadísticas sobre la forma de los datos (distribución de columnas, anulabilidad, etc.)

*   \[x] Compatibilidad con la generación de perfiles de datos y la extracción de vista previa a través de la canalización de ingestión (muestras de columnas, no filas)

#### Calidad de los datos

Incluido en la hoja de ruta del primer trimestre de 2022 - [Mostrar comprobaciones de calidad de datos en la interfaz de usuario](https://feature-requests.datahubproject.io/roadmap/544)

*   \[x] Compatibilidad con perfiles de datos y vistas de series temporales
*   \[ ] Soporte para la visualización de la calidad de los datos
*   \[ ] Soporte para la puntuación de estado de los datos basada en los resultados de calidad de los datos y la observabilidad de la tubería
*   \[ ] Integración con sistemas como Great Expectations, AWS deequ, dbt test, etc.

#### Control de acceso detallado para metadatos

*   \[x] Compatibilidad con el control de acceso basado en roles para editar metadatos
*   Alcance: Control de acceso a nivel de entidad, nivel de aspecto y también dentro de los aspectos.

#### Linaje a nivel de columna

Incluido en la hoja de ruta del primer trimestre de 2022 - [Linaje a nivel de columna](https://feature-requests.datahubproject.io/roadmap/541)

*   \[ ] Modelo de metadatos
*   \[ ] Análisis SQL

#### Metadatos operativos

*   \[ ] Conjuntos de datos particionados - - [Ver en el portal de solicitud de características](https://feature-requests.datahubproject.io/b/User-Experience/p/advanced-dataset-schema-properties-partition-support)
*   \[x] Soporte para señales operativas como integridad, frescura, etc.

### Q2 2021 (Abr - Jun 2021)

#### Implementación en la nube

*   \[X] Gráficos de Helm de grado de producción para la implementación basada en Kubernetes
*   \[ ] Guías prácticas para implementar DataHub en todos los principales proveedores de nube
    *   \[x] AWS
    *   \[ ] Azure
    *   \[x] GCP

#### Análisis de productos para DataHub

*   \[x] Ayudarle a comprender cómo interactúan sus usuarios con DataHub
*   \[x] Integración con sistemas comunes como Google Analytics, etc.

#### Información basada en el uso

*   \[x] Mostrar conjuntos de datos de uso frecuente, etc.
*   \[ ] Mejora de la relevancia de la búsqueda a través de los datos de uso

#### Control de acceso basado en roles

*   Soporte para control de acceso detallado para operaciones de metadatos (lectura, escritura, modificación)
*   Alcance: Control de acceso a nivel de entidad, nivel de aspecto y también dentro de los aspectos.
*   Esto proporciona la base para el control de acceso de Tag Governance, Dataset Preview, etc.

#### Adiciones de modelos de metadatos sin código

Caso de uso: Los desarrolladores deben poder agregar nuevas entidades y aspectos al modelo de metadatos fácilmente

*   \[x] No es necesario escribir ningún código (en Java o Python) para almacenar, recuperar, buscar y consultar metadatos
*   \[ ] No es necesario escribir ningún código (en GraphQL o UI) para visualizar los metadatos

### Q1 2021 \[Enero - Mar 2021]

#### Interfaz de usuario de React

*   \[x] Crear una nueva interfaz de usuario basada en React
*   \[x] Dejar de usar la compatibilidad de código abierto con la interfaz de usuario de Ember

#### Integración de metadatos basada en Python

*   \[x] Crear un marco de ingestión basado en Python
*   \[x] Admite repositorios de personas comunes (LDAP)
*   \[x] Admite repositorios de datos comunes (Kafka, bases de datos SQL, AWS Glue, Hive)
*   \[x] Admite fuentes de transformación comunes (dbt, Looker)
*   \[x] Compatibilidad con la emisión de metadatos basada en push desde Python (por ejemplo, DAG de flujo de aire)

#### Paneles y gráficos

*   \[x] Compatibilidad con el panel de control y la página de entidad de gráfico
*   \[x] Soporte de navegación, búsqueda y descubrimiento

#### SSO para autenticación

*   \[x] Soporte para autenticación (inicio de sesión) utilizando proveedores OIDC (Okta, Google, etc.)

#### Etiquetas

Caso de uso: Soporte para etiquetas globales de forma libre para colaboración social y ayuda al descubrimiento

*   \[x] Editar / Crear nuevas etiquetas
*   \[x] Adjunte etiquetas a construcciones relevantes (por ejemplo, conjuntos de datos, paneles, usuarios schema_fields)
*   \[x] Buscar usando etiquetas (por ejemplo, encontrar todos los conjuntos de datos con esta etiqueta, encontrar todas las entidades con esta etiqueta)

#### Glosario de Negocios

*   \[x] Soporte para el modelo de glosario de negocios (definición + almacenamiento)
*   \[ ] Examinar taxonomía
*   \[x] Compatibilidad con la interfaz de usuario para adjuntar términos empresariales a entidades y campos

#### Trabajos, Flujos / Tuberías

Caso de uso: Busque y descubra sus canalizaciones (por ejemplo, DAG de flujo de aire) y comprenda el linaje con conjuntos de datos

*   \[x] Soporte para modelos de metadatos + implementación de backend
*   \[x] Integraciones de metadatos con sistemas como Airflow.

#### Creación de perfiles de datos y vistas previas de conjuntos de datos

Caso de uso: Consulte los datos de muestra para un conjunto de datos y las estadísticas sobre la forma de los datos (distribución de columnas, anulabilidad, etc.)

*   \[ ] Soporte para la creación de perfiles de datos y la extracción de vista previa a través de la canalización de ingestión
*   Fuera del alcance de Q1: Control de acceso de perfiles de datos y datos de muestra
