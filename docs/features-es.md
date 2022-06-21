***

## título: "Características"

# Descripción general de las características de DataHub

DataHub es un catálogo de datos moderno creado para permitir el descubrimiento de datos de extremo a extremo, la observabilidad de datos y el gobierno de datos. Esta plataforma de metadatos extensible está diseñada para que los desarrolladores domen la complejidad de sus ecosistemas de datos en rápida evolución y para que los profesionales de datos aprovechen todo el valor de los datos dentro de su organización.

Aquí hay una descripción general de la funcionalidad actual de DataHub. Echa un vistazo a nuestro [hoja de ruta](https://feature-requests.datahubproject.io/roadmap) a ver lo que está por venir.

***

## Búsqueda y descubrimiento

### **Buscar en todos los rincones de su pila de datos**

La experiencia de búsqueda unificada de DataHub muestra resultados en bases de datos, datalakes, plataformas de BI, almacenes de características de ML, herramientas de orquestación y más.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-search-all-corners-of-your-datastack.gif"/>
</p>

### **Trazar linaje de extremo a extremo**

Comprenda fácilmente el recorrido de extremo a extremo de los datos mediante el seguimiento del linaje entre plataformas, conjuntos de datos, canalizaciones ETL / ELT, gráficos y paneles, y más.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-end-to-end-lineage.png"/>
</p>

### **Comprender el impacto de romper los cambios en las dependencias posteriores**

Identifique de forma proactiva qué entidades pueden verse afectadas por un cambio radical mediante el análisis de impacto.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-impact-analysis.gif"/>
</p>

### **Ver metadatos 360 de un vistazo**

Combinar *técnico* y *lógico* metadatos para proporcionar una vista robusta de 360º de sus entidades de datos.

Generar **Estadísticas del conjunto de datos** para comprender la forma y distribución de los datos

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-dataset-stats.png"/>
</p>

Captura histórico **Resultados de la validación de datos** de herramientas como Great Expectations

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/44Pr_55Qkik" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

Aproveche datahub **Historial de versiones del esquema** Para realizar un seguimiento de los cambios en la estructura física de los datos a lo largo del tiempo

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/IYaV7r5HjZY" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

***

## Gobierno de datos moderno

### **Gobierna en tiempo real**

[El marco de acción](./actions/README.md) potencia los siguientes casos de uso en tiempo real:

*   **Notificaciones:** Genere notificaciones específicas de la organización cuando se realice un cambio en DataHub. Por ejemplo, envíe un correo electrónico al equipo de gobierno cuando se agregue una etiqueta "PII" a cualquier activo de datos.
*   **Integración de flujo de trabajo:** Integre DataHub en los flujos de trabajo internos de su organización. Por ejemplo, crea un ticket de Jira cuando se propongan etiquetas o términos específicos en un conjunto de datos.
*   **Sincronización:** Sincronización de los cambios realizados en DataHub en un sistema de 3ª parte. Por ejemplo, reflejar las adiciones de etiquetas en DataHub en Snowflake.
*   **Auditoría:** Audite quién está haciendo qué cambios en DataHub a través del tiempo.

<p align="center">
    <iframe width="560" height="315" src="https://www.youtube.com/embed/yeloymkK5ow" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### **Administrar la propiedad de la entidad**

Asigne rápida y fácilmente la propiedad de la propiedad a los usuarios y/o grupos de usuarios.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-entity-owner.png"/>
</p>

### **Gobernar con etiquetas, términos del glosario y dominios**

Capacite a los propietarios de datos para que gobiernen sus entidades de datos con:

1.  **Etiquetas:** Etiquetas informales y poco controladas que sirven como herramienta para la búsqueda y el descubrimiento. Sin gestión formal y central.
2.  **Términos del glosario:** Un vocabulario controlado con jerarquía opcional, comúnmente utilizado para describir conceptos y /o medidas comerciales centrales.
3.  **Dominios:** Carpetas o categorías seleccionadas de nivel superior, comúnmente utilizadas en Data Mesh para organizar entidades por departamento (es decir, Finanzas, Marketing) y / o Productos de datos.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-tags-terms-domains.png"/>
</p>

***

## Administración de DataHub

### **Crear usuarios, grupos y políticas de acceso**

Los administradores de DataHub pueden crear directivas para definir quién puede realizar qué acción contra qué recurso. Cuando cree una nueva directiva, podrá definir lo siguiente:

*   **Plataforma de tipo de directiva** (privilegios de la plataforma DataHub de nivel superior, es decir, administrar usuarios, grupos y políticas) o metadatos (capacidad para manipular la propiedad, las etiquetas, la documentación y más)
*   **Tipo de recurso** - Especifique el tipo de recurso, como Datasets, Dashboards, Pipelines, etc.
*   **Privilegios** - Elija el conjunto de permisos, como Editar propietarios, Editar documentación, Editar enlaces
*   **Usuarios y/o Grupos** - Asignar Usuarios y/o Grupos relevantes; También puede asignar la directiva a los propietarios de recursos, independientemente del grupo al que pertenezcan

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-manage-policies.png"/>
</p>

### **Ingerir metadatos desde la interfaz de usuario**

Cree, configure, programe y ejecute la ingesta de metadatos por lotes utilizando la interfaz de usuario de DataHub. Esto facilita la obtención de metadatos en DataHub al minimizar la sobrecarga requerida para operar canalizaciones de integración personalizadas.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-managed-ingestion-config.png"/>
</p>
