# ¿Cómo agregar un nuevo aspecto de metadatos?

Agregar nuevos metadatos [aspecto](../what/aspect.md) es una de las formas más comunes de extender un [entidad](../what/entity.md).
Usaremos el [CorpUserEditableInfo](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/identity/CorpUserEditableInfo.pdl) como ejemplo aquí.

1.  Agregue el modelo de aspecto al espacio de nombres correspondiente (por ejemplo, [`com.linkedin.identity`](https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/identity))

2.  Ampliar la unión de aspectos de la entidad para incluir el nuevo aspecto (por ejemplo, [`CorpUserAspect`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/CorpUserAspect.pdl))

3.  Reconstruir el rest.li [IDL e instantánea](https://linkedin.github.io/rest.li/modeling/compatibility_check) Ejecutando el siguiente comando desde la raíz del proyecto

<!---->

    ./gradlew :gms:impl:build -Prest.model.compatibility=ignore

4.  Para mostrar el nuevo aspecto en el nivel superior [extremo de recursos](https://linkedin.github.io/rest.li/user_guide/restli_server#writing-resources), ampliar el modelo de datos de recursos (por ejemplo, [`CorpUser`](https://github.com/datahub-project/datahub/blob/master/gms/api/src/main/pegasus/com/linkedin/identity/CorpUser.pdl)) con un campo opcional (por ejemplo, [`editableInfo`](https://github.com/datahub-project/datahub/blob/master/gms/api/src/main/pegasus/com/linkedin/identity/CorpUser.pdl#L21)). También deberá extender el `toValue` & `toSnapshot` métodos del recurso de nivel superior (por ejemplo, [`CorpUsers`](https://github.com/datahub-project/datahub/blob/master/gms/impl/src/main/java/com/linkedin/metadata/resources/identity/CorpUsers.java)) para convertir entre los modelos de instantánea y valor.

5.  (Opcional) Si es necesario actualizar el aspecto a través de la API (en lugar de/ además de MCE), agregue un [sub-recurso](https://linkedin.github.io/rest.li/user_guide/restli_server#sub-resources) punto final para el nuevo aspecto (por ejemplo, [`CorpUsersEditableInfoResource`](https://github.com/datahub-project/datahub/blob/master/gms/impl/src/main/java/com/linkedin/metadata/resources/identity/CorpUsersEditableInfoResource.java)). El endpiont de sub-recurso también le permite recuperar versiones anteriores del aspecto, así como metadatos adicionales, como el sello de auditoría.

6.  Después de reconstruir y reiniciar [gms](https://github.com/datahub-project/datahub/tree/master/gms), [mce-consumidor-trabajo](https://github.com/datahub-project/datahub/tree/master/metadata-jobs/mce-consumer-job) & [mae-consumidor-trabajo](https://github.com/datahub-project/datahub/tree/master/metadata-jobs/mae-consumer-job),
    Debería poder comenzar a emitir [MCE](../what/mxe.md) con el nuevo aspecto y tenerlo automáticamente ingerido y almacenado en DB.
