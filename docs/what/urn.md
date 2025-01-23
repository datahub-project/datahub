# What is URN?

URN ([Uniform Resource Name](https://en.wikipedia.org/wiki/Uniform_Resource_Name)) is the chosen scheme of [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) to uniquely define any resource in DataHub. It has the following form
```
urn:<Namespace>:<Entity Type>:<ID>
```
[Onboarding a new entity](../modeling/metadata-model.md) to [GMA](gma.md) starts with modelling an URN specific to that entity.
You can use the existing [URN models](../../li-utils/src/main/javaPegasus/com/linkedin/common/urn) for built-in entities as a reference.

## Namespace
All URNs available in DataHub are using `li` as their namespace.
This can be easily changed to a different namespace for your organization if you fork DataHub.

## Entity Type
Entity type for URN is different than [entity](entity.md) in GMA context. This can be thought of as the object type of
any resource for which you need unique identifier for its each instance. While you can create URNs for GMA entities such as
[DatasetUrn] with entity type `dataset`, you can also define URN for data platforms, [DataPlatformUrn].

## ID
ID is the unique identifier part of a URN. It's unique for a specific entity type within a specific namespace.
ID could contain a single field, or multi fields in the case of complex URNs. A complex URN can even contain other URNs as ID fields. This type of URN is also referred to as nested URN. For non-URN ID fields, the value can be either a string, number, or [Pegasus Enum](https://linkedin.github.io/rest.li/pdl_schema#enum-type).

Here are some example URNs with a single ID field:

```
urn:li:dataPlatform:kafka
urn:li:corpuser:jdoe
```

[DatasetUrn](../../li-utils/src/main/javaPegasus/com/linkedin/common/urn/DatasetUrn.java) is an example of a complex nested URN. It contains 3 ID fields: `platform`, `name` and `fabric`, where `platform` is another [URN](../../li-utils/src/main/javaPegasus/com/linkedin/common/urn/DataPlatformUrn.java). Here are some examples
```
urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)
urn:li:dataset:(urn:li:dataPlatform:hdfs,PageViewEvent,EI)
```

## Restrictions

There are a few restrictions when creating an URN:

The following characters are not allowed anywhere in the URN

1. Parentheses are reserved characters in URN fields: `(` or `)`
2. The "unit separator" unicode character `␟` (U+241F)

The following characters are not allowed within an URN tuple.

1. Commas are reserved characters in URN tuples: `,`

Example: `urn:li:dashboard:(looker,dashboards.thelook)` is a valid urn, but `urn:li:dashboard:(looker,dashboards.the,look)` is invalid.

Please do not use these characters when creating or generating urns. One approach is to use URL encoding for the characters.
