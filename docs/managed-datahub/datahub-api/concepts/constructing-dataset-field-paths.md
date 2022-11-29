---
description: >-
  The page explains how to construct standardized identifiers for specific
  fields within a Dataset, also known as "field paths".
---

# Dataset Field Paths

## Introduction

DataHub uses its own standardized format, similar to JSON Path, to identify fields within a Dataset Schema. These identifiers can be used when performing actions for Dataset field sub-resources, such as adding a Tag or editing a description.&#x20;

The technical specification of the Field Path format can be found in [SchemaFieldPath Specification](https://datahubproject.io/docs/advanced/field-path-spec-v2/). Before proceeding, we recommend getting familiar with the specification.&#x20;

### Examples

We illustrate the creation of Field Paths based on example, considering JSON schemas as well as SQL table definitions.

#### JSON

Take the following JSON object:

```
{
   "strField": "sample string value",
   "numField": 0,
   "structField": {
       "subStrField": "sample struct string value",
       "subNumField": 10
   },
   "arrayField": [ "val1", "val2", "val3" ]
}

```

The corresponding DataHub field specifications would include

```
  "[version=2.0].[type=string].strField",
  "[version=2.0].[type=int].numField",
  "[version=2.0].[type=struct].structField.[type=string].subStrField",
  "[version=2.0].[type=struct].structField.[type=int].subNumField",
  "[version=2.0].[type=array].arrayField.[type=string]"
```

As you can see, the specification supports field reference and type annotation via the "." operator.

#### SQL

Take the following **CREATE TABLE** statement:&#x20;

```
CREATE TABLE metadata_aspect_v2 (
  id                            VARCHAR(500) NOT NULL,
  name                          VARCHAR(200) NOT NULL,
  version                       bigint(20) NOT NULL,
  data                          longtext NOT NULL,
  createdon                     datetime(6) NOT NULL,
  createdby                     VARCHAR(255) NOT NULL,
  CONSTRAINT PRIMARY KEY (id)
);
```

The corresponding DataHub field specifications would include

```
"[version=2.0].[type=string].id",
"[version=2.0].[type=string].name",
"[version=2.0].[type=long].version",
"[version=2.0].[type=string].data",
"[version=2.0].[type=long].createdon",
"[version=2.0].[type=string].createdby",
```
