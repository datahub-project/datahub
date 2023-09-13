# How to add a new metadata aspect?

Adding a new metadata [aspect](../what/aspect.md) is one of the most common ways to extend an existing [entity](../what/entity.md).
We'll use the CorpUserEditableInfo as an example here.

1. Add the aspect model to the corresponding namespace (e.g. [`com.linkedin.identity`](https://github.com/datahub-project/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/identity))

2. Extend the entity's aspect union to include the new aspect.

3. Rebuild the rest.li [IDL & snapshot](https://linkedin.github.io/rest.li/modeling/compatibility_check) by running the following command from the project root
```
./gradlew :metadata-service:restli-servlet-impl:build -Prest.model.compatibility=ignore
```

4. To surface the new aspect at the top-level [resource endpoint](https://linkedin.github.io/rest.li/user_guide/restli_server#writing-resources), extend the resource data model with an optional field. You'll also need to extend the `toValue` & `toSnapshot` methods of the top-level resource (e.g. [`CorpUsers`](https://github.com/datahub-project/datahub/blob/master/gms/impl/src/main/java/com/linkedin/metadata/resources/identity/CorpUsers.java)) to convert between the snapshot & value models.

5. (Optional) If there's need to update the aspect via API (instead of/in addition to MCE), add a [sub-resource](https://linkedin.github.io/rest.li/user_guide/restli_server#sub-resources) endpoint for the new aspect (e.g. `CorpUsersEditableInfoResource`). The sub-resource endpiont also allows you to retrieve previous versions of the aspect as well as additional metadata such as the audit stamp.

6. After rebuilding & restarting gms, [mce-consumer-job](https://github.com/datahub-project/datahub/tree/master/metadata-jobs/mce-consumer-job) & [mae-consumer-job](https://github.com/datahub-project/datahub/tree/master/metadata-jobs/mae-consumer-job),z
you should be able to start emitting [MCE](../what/mxe.md) with the new aspect and have it automatically ingested & stored in DB.
