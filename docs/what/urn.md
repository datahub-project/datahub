# What is URN?

[URN] (Uniform Resource Name) is the chosen scheme of [URI] to uniquely define any resource in DataHub. It has a form as below:
```
urn:<<namespace>>:<<entity-type>>:<<content>>
```
Any [entity onboarding](../how/entity-onboarding.md) in [GMA](gma.md) starts with modelling an URN specific to that entity.
You can check URN models for already availabile entities [here](../../li-utils/src/main/java/com/linkedin/common/urn).

### Namespace
All URNs available in DataHub are using `li` as their namespace. 
This can be easily changed to a different namespace for your organization if you fork DataHub.

### Entity Type
Entity type for URN is different than [entity](entity.md) in GMA context. This can be thought of as the object type of
any resource for which you need unique identifier for its each instance. While you can create URNs for GMA entities such as
[DatasetUrn] with entity type `dataset`, you can also define URN for data platforms, [DataPlatformUrn].

### Content
Content is the locally unique identifier part of an URN. It's unique for a specific entity type within a specific namespace.
Content could contain a single field or multi-field (complex URN). Complex URNs could even contain another URN as one of their content fields.
Example URNs with a single field content:

```
urn:li:dataPlatform:kafka
urn:li:corpuser:jdoe
```

[DatasetUrn] is an example of a complex URN (multi-field content). It contains 3 fields: `platform`, `name` & `fabric`, respectively.
Below are some examples:
```
urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)
urn:li:dataset:(urn:li:dataPlatform:hdfs,PageViewEvent,EI)
```

[URN]: https://en.wikipedia.org/wiki/Uniform_Resource_Name
[URI]: https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
[DatasetUrn]: ../../li-utils/src/main/java/com/linkedin/common/urn/DatasetUrn.java
[DataPlatformUrn]: ../../li-utils/src/main/java/com/linkedin/common/urn/DataPlatformUrn.java