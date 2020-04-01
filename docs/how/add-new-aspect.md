# How to add a a new metadata aspect?

Adding a new metadata [aspect](https://github.com/linkedin/datahub/blob/master/docs/what/aspect.md) is one of the most common ways to extend an existing [entity](https://github.com/linkedin/datahub/blob/master/docs/what/entity.md).
We'll use the [CorpUserEditableInfo](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/identity/CorpUserEditableInfo.pdsc) as an example here.

1. Add the aspect model to the corresponding namespace (i.e. [com.linkedin.identity](https://github.com/linkedin/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/identity) in this case)
2. Extend the entity's aspect union to include the new aspect (i.e. [CorpUserAspect.pdsc](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/CorpUserAspect.pdsc) in this case)
3. Rebuild the rest.li [IDL & snapshot](https://linkedin.github.io/rest.li/modeling/compatibility_check) by running the following command from the project root
```
./gradlew :gms:build -Prest.model.compatibility=ignore
```
4. After rebuilding & restarting [gms](https://github.com/linkedin/datahub/tree/master/gms), [mce-consumer-job](https://github.com/linkedin/datahub/tree/master/metadata-jobs/mce-consumer-job) & [mae-consumer-job](https://github.com/linkedin/datahub/tree/master/metadata-jobs/mae-consumer-job),
you should be able to start emitting [MCE](https://github.com/linkedin/datahub/blob/master/docs/what/mxe.md) with the new aspect and have it automatically ingested & stored in DB.
