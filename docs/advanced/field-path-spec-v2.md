# SchemaFieldPath Specification (Version 2)

This document outlines the formal specification for the fieldPath member of
the [SchemaField](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/schema/SchemaField.pdl)
model. This specification (version 2) takes into account the unique requirements of supporting a wide variety of nested
types, unions and optional fields and is a substantial improvement over the current implementation (version 1).

## Requirements

The `fieldPath` field is currently used by datahub for not just rendering the schema fields in the UI, but also as a
primary identifier of a field in other places such
as [EditableSchemaFieldInfo](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/schema/EditableSchemaFieldInfo.pdl#L12),
usage stats and data profiles. Therefore, it must satisfy the following requirements.

* must be unique across all fields within a schema.
* make schema navigation in the UI more intuitive.
* allow for identifying the type of schema the field is part of, such as a `key-schema` or a `value-schema`.
* allow for future-evolution

## Existing Convention(v1)

The existing convention is to simply use the field's name as the `fieldPath` for simple fields, and use the `dot`
delimited names for nested fields. This scheme does not satisfy the [requirements](#requirements) stated above. The
following example illustrates where the `uniqueness` requirement is not satisfied.

### Example: Ambiguous field path

Consider the following `Avro` schema which is a `union` of two record types `A` and `B`, each having a simple field with
the same name `f` that is of type `string`. The v1 naming scheme cannot differentiate if a `fieldPath=f` is referring to
the record type `A` or `B`.

```
[
    {
        "type": "record",
        "name": "A",
        "fields": [{ "name": "f", "type": "string" } ]
    }, {
        "type": "record",
        "name": "B",
        "fields": [{ "name": "f", "type": "string" } ]
    }
]
```

## The FieldPath encoding scheme(v2)

The syntax for V2 encoding of the `fieldPath` is captured in the following grammar. The `FieldPathSpec` is essentially
the type annotated path of the member, with each token along the path representing one level of nested member,
starting from the most-enclosing type, leading up to the member. In the case of `unions` that have `one-of` semantics,
the corresponding field will be emitted once for each `member` of the union as its `type`, along with one path
corresponding to the `union` itself.

### Formal Spec:

```
<SchemaFieldPath> := <VersionToken>.<PartOfKeySchemaToken>.<FieldPathSpec>  // when part of a key-schema
                   | <VersionToken>.<FieldPathSpec> // when part of a value schema
<VersionToken> := [version=<VersionId>] // [version=2.0] for v2
<PartOfKeySchemaToken> := [key=True]  // when part of a key schema
<FieldPathSpec> := <FieldToken>+  // this is the type prefixed path field (nested if repeats).
<FieldToken> := <TypePrefixToken>.<name_of_the_field> // type prefixed path of a field.
<TypePrefixToken> := <NestedTypePrefixToken>.<SimpleTypeToken> | <SimpleTypeToken>
<NestedTypePrefixToken> := [type=<NestedType>]
<SimpleTypeToken> := [type=<SimpleType>]
<NestedType> := <name of a struct/record> | union | array | map
<SimpleType> := int | float | double | string | fixed | enum
```

For the [example above](#example-ambiguous-field-path), this encoding would produce the following 2 unique paths
corresponding to the `A.f` and `B.f` fields.

```python
unique_v2_field_paths = [
 "[version=2.0].[type=union].[type=A].[type=string].f",
 "[version=2.0].[type=union].[type=B].[type=string].f"
]
```

NOTE:

- this encoding always ensures uniqueness within a schema since the full type annotation leading to a field is encoded
  in the fieldPath itself.
- processing a fieldPath, such as from UI, gets simplified simply by walking each token along the path from
  left-to-right.
- adding PartOfKeySchemaToken allows for identifying if the field is part of key-schema.
- adding VersionToken allows for future evolvability.
- to represent `optional` fields, which sometimes are modeled as `unions` in formats like `Avro`, instead of treating it
  as a `union` member, set the `nullable` member of `SchemaField` to `True`.

## Examples

### Primitive types

```python
avro_schema = """
{
  "type": "string"
}
"""
unique_v2_field_paths = [
  "[version=2.0].[type=string]"
]
```
### Records
**Simple Record**
```python
avro_schema = """
{
  "type": "record",
  "name": "some.event.E",
  "namespace": "some.event.N",
  "doc": "this is the event record E"
  "fields": [
    {
      "name": "a",
      "type": "string",
      "doc": "this is string field a of E"
    },
    {
      "name": "b",
      "type": "string",
      "doc": "this is string field b of E"
    }
  ]
}
"""

unique_v2_field_paths = [
    "[version=2.0].[type=E].[type=string].a",
    "[version=2.0].[type=E].[type=string].b",
]
```
**Nested Record**
```python
avro_schema = """
{
    "type": "record",
    "name": "SimpleNested",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "nestedRcd",
        "type": {
            "type": "record",
            "name": "InnerRcd",
            "fields": [{
                "name": "aStringField",
                 "type": "string"
            } ]
        }
    }]
}
"""

unique_v2_field_paths = [
  "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd",
  "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd.[type=string].aStringField",
]
```

**Recursive Record**
```python
avro_schema = """
{
    "type": "record",
    "name": "Recursive",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "r",
        "type": {
            "type": "record",
            "name": "R",
            "fields": [
                { "name" : "anIntegerField", "type" : "int" },
                { "name": "aRecursiveField", "type": "com.linkedin.R"}
            ]
        }
    }]
}
"""

unique_v2_field_paths = [
  "[version=2.0].[type=Recursive].[type=R].r",
  "[version=2.0].[type=Recursive].[type=R].r.[type=int].anIntegerField",
  "[version=2.0].[type=Recursive].[type=R].r.[type=R].aRecursiveField"
]
```

```python
avro_schema ="""
{
    "type": "record",
    "name": "TreeNode",
    "fields": [
        {
            "name": "value",
            "type": "long"
        },
        {
            "name": "children",
            "type": { "type": "array", "items": "TreeNode" }
        }
    ]
}
"""
unique_v2_field_paths = [
 "[version=2.0].[type=TreeNode].[type=long].value",
 "[version=2.0].[type=TreeNode].[type=array].[type=TreeNode].children",
]
```
### Unions
```python
avro_schema = """
{
    "type": "record",
    "name": "ABUnion",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "a",
        "type": [{
            "type": "record",
            "name": "A",
            "fields": [{ "name": "f", "type": "string" } ]
            }, {
            "type": "record",
            "name": "B",
            "fields": [{ "name": "f", "type": "string" } ]
            }
        ]
    }]
}
"""
unique_v2_field_paths: List[str] = [
    "[version=2.0].[key=True].[type=ABUnion].[type=union].a",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=A].a",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=A].a.[type=string].f",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=B].a",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=B].a.[type=string].f",
]
```
### Arrays
```python
avro_schema = """
{
    "type": "record",
    "name": "NestedArray",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "ar",
        "type": {
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    "null",
                    {
                        "type": "record",
                        "name": "Foo",
                        "fields": [ {
                            "name": "a",
                            "type": "long"
                        } ]
                    }
                ]
            }
        }
    }]
}
"""
unique_v2_field_paths: List[str] = [
  "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar",
  "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar.[type=long].a",
]
```
### Maps
```python
avro_schema = """
{
  "type": "record",
  "name": "R",
  "namespace": "some.namespace",
  "fields": [
    {
      "name": "a_map_of_longs_field",
      "type": {
        "type": "map",
        "values": "long"
      }
    }
  ]
}
"""
unique_v2_field_paths = [
  "[version=2.0].[type=R].[type=map].[type=long].a_map_of_longs_field",
]


```
### Mixed Complex Type Examples
```python
# Combines arrays, unions and records.
avro_schema = """
{
    "type": "record",
    "name": "ABFooUnion",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "a",
        "type": [ {
            "type": "record",
            "name": "A",
            "fields": [{ "name": "f", "type": "string" } ]
            }, {
            "type": "record",
            "name": "B",
            "fields": [{ "name": "f", "type": "string" } ]
            }, {
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    "null",
                    {
                        "type": "record",
                        "name": "Foo",
                        "fields": [{ "name": "f", "type": "long" }]
                    }
                ]
            }
    }]
    }]
}
"""

unique_v2_field_paths: List[str] = [
  "[version=2.0].[type=ABFooUnion].[type=union].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f",
]
```

For more examples, see
the [unit-tests for AvroToMceSchemaConverter](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/unit/test_schema_util.py).

### Backward-compatibility

While this format is not directly compatible with the v1 format, the v1 equivalent can easily be constructed from the v2
encoding by stripping away all the v2 tokens enclosed in the square-brackets `[<new_in_v2>]`.
