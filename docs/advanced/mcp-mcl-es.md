# MetadataChangeProposal & MetadataChangeRegistros de registro

## Visión general y visión

A partir de la versión v0.8.7, se han introducido dos nuevas secuencias de eventos importantes: MetadataChangeProposal y MetadataChangeLog. Estos temas sirven como versiones más genéricas (y con nombres más apropiados) de los eventos clásicos MetadataChangeEvent y MetadataAuditEvent, utilizados para a) proponer y b) registrar cambios en el gráfico de metadatos de DataHub.

Con estos eventos, avanzamos hacia un mundo más genérico, en el que los modelos de metadatos no son partes fuertemente tipadas de los esquemas de eventos en sí. Esto proporciona flexibilidad, lo que permite que los modelos principales que componen el gráfico de metadatos se agreguen y cambien dinámicamente, sin requerir actualizaciones estructurales de los esquemas de API de Kafka o REST utilizados para ingerir y servir metadatos.

Además, nos hemos centrado en el "aspecto" como la unidad atómica de escritura en DataHub. MetadataChangeProposal & MetadataChangeLog con llevan un solo aspecto en su carga útil, a diferencia de la lista de aspectos que lleva el MCE y MAE de hoy. Esto refleja con mayor precisión el contrato de atomicidad del modelo de metadatos, con la esperanza de disminuir la confusión sobre las garantías transaccionales para las escrituras de múltiples aspectos, además de simplificar la sintonía con los cambios de metadatos que le importan a un consumidor.

Hacer que estos eventos sean más genéricos no es gratis; renunciamos a algunos en forma de validación de esquemas nativos de Restli y Kafka y diferimos esta responsabilidad a DataHub, que es el único ejecutor de los contratos del modelo de gráficos. Además, añadimos un paso extra a la desagregación de los metadatos reales al requerir una doble deserialización: la del propio cuerpo evento/respuesta y otra del aspecto de metadatos anidados.

Para mitigar estas desventajas, nos comprometemos a proporcionar bibliotecas de cliente multilingües capaces de hacer el trabajo duro por usted. Tenemos la intención de publicarlos como artefactos fuertemente tipados generados a partir del conjunto de modelos "predeterminados" con el que se envía DataHub. Esto se suma a una iniciativa para introducir una capa OpenAPI en el backend (gms) de DataHub que proporcionaría un modelo fuertemente tipado.

En última instancia, tenemos la intención de realizar un estado en el que los esquemas de Entidades y Aspecto se puedan alterar sin requerir código generado y sin mantener un solo esquema de megamodelo (mirándolo, Snapshot.pdl). La intención es que los cambios en el modelo de metadatos sean aún más fáciles de lo que son hoy en día.

## Modelado

Una propuesta de cambio de metadatos se define (en PDL) de la siguiente manera:

```protobuf
record MetadataChangeProposal {

  /**
   * Kafka audit header. See go/kafkaauditheader for more info.
   */
  auditHeader: optional KafkaAuditHeader

  /**
   * Type of the entity being written to
   */
  entityType: string

  /**
   * Urn of the entity being written
   **/
  entityUrn: optional Urn,

  /**
   * Key aspect of the entity being written
   */
  entityKeyAspect: optional GenericAspect
	
  /**
   * Type of change being proposed
   */
  changeType: ChangeType

  /**
   * Aspect of the entity being written to
   * Not filling this out implies that the writer wants to affect the entire entity
   * Note: This is only valid for CREATE and DELETE operations.
   **/
  aspectName: optional string

  aspect: optional GenericAspect

  /**
   * A string->string map of custom properties that one might want to attach to an event
   **/
  systemMetadata: optional SystemMetadata

}
```

Cada propuesta comprende lo siguiente:

1.  entityType

    Se refiere al tipo de entidad, por ejemplo, conjunto de datos, gráfico

2.  entityUrn

    Urna de la entidad que se está actualizando. Nota **exactamente uno** de entityUrn o entityKeyAspect debe rellenarse para identificar correctamente una entidad.

3.  entityKeyAspect

    Aspecto clave de la entidad. En lugar de tener una URN de cadena, admitiremos la identificación de entidades por sus estructuras de aspecto clave. Tenga en cuenta que esto no es compatible a partir de ahora.

4.  changeType

    Tipo de cambio que está proponiendo: uno de

    *   UPSERT: Insertar si no existe, actualizar de lo contrario
    *   CREAR: Insertar si no existe, falla de lo contrario
    *   ACTUALIZACIÓN: Actualizar si existe, fallar de lo contrario
    *   ELIMINAR: Eliminar
    *   PARCHE: Parchear el aspecto en lugar de hacer un reemplazo completo

    Solo UPSERT es compatible a partir de ahora.

5.  aspectName

    Nombre del aspecto. Debe coincidir con el nombre en la anotación "@Aspect".

6.  aspecto

    Para admitir aspectos fuertemente tipados, sin tener que realizar un seguimiento de una unión de todos los aspectos existentes, introdujimos un nuevo objeto llamado GenericAspect.

    ```xml
    record GenericAspect {
        value: bytes
        contentType: string
    }
    ```

    Contiene el tipo de serialización y el valor serializado. Tenga en cuenta que actualmente solo admitimos "application/json" como contentType, pero agregaremos más formas de serialización en el futuro. La validación del objeto serializado se realiza en GMS contra el esquema que coincide con aspectName.

7.  systemMetadata

    Metadatos adicionales sobre la propuesta, como run_id o marca de tiempo actualizada.

GMS procesa la propuesta y produce el registro de cambios de metadatos, que se ve así.

```protobuf
record MetadataChangeLog includes MetadataChangeProposal {

  previousAspectValue: optional GenericAspect

  previousSystemMetadata: optional SystemMetadata

}
```

Incluye todos los campos de la propuesta, pero también tiene la versión anterior del valor del aspecto y los metadatos del sistema. Esto permite que el procesador MCL conozca el valor anterior antes de decidir actualizar todos los índices.

## Temas

Tras el cambio en nuestros modelos de eventos, presentamos 4 nuevos temas. Los temas antiguos quedarán obsoletos a medida que migremos completamente a este modelo.

1.  **MetadataChangeProposal_v1, FailedMetadataChangeProposal_v1**

    De manera análoga al tema MCE, las propuestas que se producen en el tema MetadataChangeProposal_v1, se ingieren a GMS de forma asincrónica, y cualquier ingestión fallida producirá un MCP fallido en el FailedMetadataChangeProposal_v1 tema.

2.  **MetadataChangeLog_Versioned_v1**

    De manera análoga al tema MAE, los MCL para aspectos versionados se producirán en este tema. Dado que los aspectos versionados tienen una fuente de verdad de la que se puede hacer una copia de seguridad por separado, la retención de este tema es corta (por defecto 7 días). Tenga en cuenta que tanto este como el siguiente tema son consumidos por el mismo procesador MCL.

3.  **MetadataChangeLog_Timeseries_v1**

    De manera análoga a los temas MAE, los MCL para aspectos de series temporales se producirán en este tema. Dado que los aspectos de la serie temporal no tienen una fuente de verdad, sino que se ingieren directamente a elasticsearch, establecemos que la retención de este tema sea más larga (90 días). Puede hacer una copia de seguridad del aspecto de las series temporales reproduciendo este tema.

## Configuración

Con MetadataChangeProposal y MetadataChangeLog, introduciremos un nuevo mecanismo para configurar la asociación entre entidades y aspectos de metadatos. Específicamente, el modelo Snapshot.pdl ya no codificará esta información a través de [Rest.li](http://rest.li) unión. En su lugar, un archivo yaml más explícito proporcionará estos enlaces. Este archivo se aprovechará en tiempo de ejecución para construir el Registro de entidades en memoria que contiene el esquema de metadatos global junto con algunos metadatos adicionales.

Un ejemplo del archivo de configuración que se utilizará para MCP & MCL, que define una entidad "dataset" que está asociada a dos aspectos: "datasetKey" y "datasetProfile".

    # entity-registry.yml

    entities:
      - name: dataset
        keyAspect: datasetKey
        aspects:
          - datasetProfile
