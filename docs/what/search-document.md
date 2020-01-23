# What is a search document?

[Search documents](https://en.wikipedia.org/wiki/Search_engine_indexing) are also modeled using [PDSC](https://linkedin.github.io/rest.li/DATA-Data-Schema-and-Templates) explicitly. 
In many ways, the model for a Document is very similar to an [Entity](entity.md) and [Relationship](relationship.md) model, 
where each attribute/field contains a value that’s derived from various metadata aspects. 
However, a search document is also allowed to have array type of attribute that contains only primitives or enum items. 
This is because most full-text search engines supports membership testing against an array field, e.g. an array field containing all the terms used in a document.

One obvious use of the attributes is to perform search filtering, e.g. give me all the `User` whose first name or last name is similar to “Joe” and reports up to `userFoo`. 
Since the document is also served as the main interface for the search API, the attributes can also be used to format the search snippet. 
As a result, one may be tempted to add as many attributes as needed. This is acceptable as the underlying search engine is designed to index a large number of fields.

Below shows an example schema for the `User` search document. Note that:
1. Each search document is required to have a type-specific `urn` field, generally maps to an entity in the [graph](graph.md).
2. Similar to `Entity`, each document has an optional `removed` field for "soft deletion". 
This is captured in [BaseDocument](../../metadata-models/src/main/pegasus/com/linkedin/metadata/search/BaseDocument.pdsc), which is expected to be included by all documents.
3. Similar to `Entity`, all remaining fields are made `optional` to support partial updates.
4. `management` shows an example of a string array field.
5. `ownedDataset` shows an example on how a field can be derived from metadata [aspects](aspect.md) associated with other types of entity (in this case, `Dataset`).

```json
{
  "type": "record",
  "name": "BaseDocument",
  "namespace": "com.linkedin.metadata.search",
  "doc": "Common fields that apply to all documents",
  "fields": [
    {
      "name": "removed",
      "type": "boolean",
      "doc": "Whether the entity has been removed or not",
      "optional": true,
      "default": false
    }
  ]
}
```

```json
{
 "type": "record",
 "name": "UserDocument",
 "namespace": "com.linkedin.metadata.search",
 "doc": "Data model for user entity search",
 "include": [
   "BaseDocument"
 ],
 "fields": [
   {
     "name": "urn",
     "type": "com.linkedin.common.CorpuserUrn",
     "doc": "Urn for the user"
   },
   {
     "name": "firstName",
     "type": "string",
     "doc": "First name of the user",
     "optional": true
   },
   {
     "name": "lastName",
     "type": "string",
     "doc": "Last name of the user",
     "optional": true
   },
   {
     "name": "management",
     "type": {
       "type": "array",
       "items": "com.linkedin.common.CorpuserUrn"
     },
     "doc": "The chain of management all the way to CEO",
     "default": [],
     "optional": true
   },
   {
     "name": "costCenter",
     "type": "int",
     "doc": "Code for the cost center",
     "optional": true
   },
   {
     "name": "ownedDatasets",
     "type": {
       "type": "array",
       "items": "com.linkedin.common.DatasetUrn"
     },
     "doc": "The list of dataset the user owns",
     "default": [],
     "optional": true
   }
 ]
}
```