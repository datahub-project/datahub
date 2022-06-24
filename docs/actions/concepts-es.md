# Conceptos de acciones de DataHub

El marco de actions incluye componentes conectables para filtrar, transformar y reaccionar a DataHub importantes, como

*   Adiciones / Eliminaciones de Etiquetas
*   Adiciones / eliminaciones de términos del glosario
*   Adiciones / eliminaciones de campos de esquema
*   Adiciones / Mudanzas del Propietario

y más, en tiempo real.

DataHub Actions viene con una biblioteca abierta de Transformers, Acciones, Eventos y más disponibles gratuitamente.

Finalmente, el marco es altamente configurable y escalable. Los aspectos más destacados incluyen:

*   **Acciones distribuidas**: Capacidad de escalar horizontalmente el procesamiento para una sola acción. Compatibilidad con la ejecución de la misma configuración de acción en varios nodos para equilibrar la carga del tráfico del flujo de eventos.
*   **Al menos una entrega una vez**: Soporte nativo para el estado de procesamiento independiente para cada acción a través de acking de posprocesamiento para lograr al menos una semántica de una vez.
*   **Manejo robusto de errores**: Directivas de error configurables con reintento de eventos, cola de letra muerta y política de continuación de eventos fallidos para lograr las garantías requeridas por su organización.

### Casos de uso

Los casos de uso en tiempo real se dividen en general en las siguientes categorías:

*   **Notificaciones**: Genera notificaciones específicas de la organización cuando se realiza un cambio en DataHub. Por ejemplo, envíe un correo electrónico al equipo de gobierno cuando se agregue una etiqueta "PII" a cualquier activo de datos.
*   **Integración de flujo de trabajo**: Integre DataHub en los flujos de trabajo internos de su organización. Por ejemplo, crea un ticket de Jira cuando se propongan etiquetas o términos específicos en un conjunto de datos.
*   **Sincronización**: Sincronización de los cambios realizados en DataHub en un sistema de 3rd party. Por ejemplo, reflejar las adiciones de etiquetas en DataHub en Snowflake.
*   **Auditoría**: Audite quién está haciendo los cambios en DataHub a través del tiempo.

¡y más!

## Conceptos

El Marco de Acciones consta de algunos conceptos básicos:

*   **Tuberías**
*   **Eventos** y **Orígenes de eventos**
*   **Transformadores**
*   **Acciones**

Cada uno de estos se describirá en detalle a continuación.

![](imgs/actions.png)
*En el Marco de acciones, los eventos fluyen continuamente de izquierda a derecha.*\*

### Tuberías

Un **Tubería** es un proceso en continua ejecución que realiza las siguientes funciones:

1.  Sondeos de eventos desde un origen de eventos configurado (que se describe a continuación)
2.  Aplica Transformación + Filtrado configurado al evento
3.  Ejecuta la acción configurada en el evento resultante

además de manejar la inicialización, errores, reintentos, registros y más.

Cada archivo de configuración de acciones corresponde a una canalización única. En la práctica,
cada canalización tiene su propio origen de eventos, transformaciones y acciones. Esto facilita el mantenimiento del estado de las acciones de misión crítica de forma independiente.

Es importante destacar que cada acción debe tener un nombre único. Esto sirve como un identificador estable en toda la ejecución de la tubería que puede ser útil para salvar el estado del consumidor de la tubería (es decir, resiliencia + confiabilidad). Por ejemplo, el origen de eventos de Kafka (predeterminado) utiliza el nombre de la canalización como identificador del grupo de consumidores de Kafka. Esto le permite escalar fácilmente sus acciones ejecutando varios procesos con el mismo archivo de configuración exacto. Cada uno simplemente se convertirá en diferentes consumidores en el mismo grupo de consumidores, compartiendo el tráfico del flujo de eventos de DataHub.

### Eventos

**Eventos** son objetos de datos que representan los cambios que se han producido en DataHub. Estrictamente hablando, el único requisito que impone el marco de Acciones es que estos objetos deben ser

un. Convertible a JSON
b. Convertible desde JSON

De modo que, en caso de errores de procesamiento, los eventos se pueden escribir y leer desde un archivo de eventos fallidos.

#### Tipos de eventos

Cada instancia de evento dentro del marco corresponde a una sola **Tipo de evento**, que es un nombre común (por ejemplo, "EntityChangeEvent_v1") que se puede utilizar para comprender la forma del Evento. Esto se puede considerar como un nombre de "tema" o "corriente". Dicho esto, no se espera que los eventos asociados con un solo tipo cambien de manera descendente a través de versons.

### Orígenes de eventos

Los eventos son producidos en el marco por **Orígenes de eventos**. Los orígenes de eventos pueden incluir sus propias garantías, configuraciones, comportamientos y semántica. Por lo general, producen un conjunto fijo de tipos de eventos.

Además de obtener eventos, los orígenes de eventos también son responsables de acelerar el procesamiento exitoso de un evento mediante la implementación de la `ack` método. Esto es invocado por el marco una vez que se garantiza que el evento ha alcanzado la acción configurada correctamente.

### Transformadores

**Transformadores** son componentes conectables que toman un evento como entrada y producen un evento (o nada) como salida. Esto se puede utilizar para enriquecer la información de un Evento antes de enviarla a una Acción.

Se pueden configurar múltiples transformadores para que se ejecuten en secuencia, filtrando y transformando un evento en varios pasos.

Los transformadores también se pueden utilizar para generar un tipo de evento completamente nuevo (es decir, registrado en tiempo de ejecución a través del Registro de eventos) que posteriormente puede servir como entrada a una acción.

Los transformadores se pueden personalizar y conectar fácilmente para cumplir con los requisitos básicos de una organización. Para obtener más información sobre el desarrollo de un transformador, consulte [Desarrollo de un transformador](guides/developing-a-transformer.md)

### Acción

**Acciones** son componentes conectables que toman un evento como entrada y realizan alguna lógica de negocio. Los ejemplos pueden ser enviar una notificación de Slack, iniciar sesión en un archivo,
o crear un ticket de Jira, etc.

Cada canalización se puede configurar para que tenga una sola acción que se ejecute después de que se hayan producido el filtrado y las transformaciones.

Las acciones se pueden personalizar y conectar fácilmente para cumplir con los requisitos básicos de una organización. Para obtener más información sobre el desarrollo de una acción, consulte [Desarrollo de una acción](guides/developing-an-action.md)
