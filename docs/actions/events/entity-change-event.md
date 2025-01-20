# Entity Change Event V1

## Event Type

`EntityChangeEvent_v1`

## Overview

This Event is emitted when certain changes are made to an entity (dataset, dashboard, chart, etc) on DataHub.

## Event Structure

Entity Change Events are generated in a variety of circumstances, but share a common set of fields. 

### Common Fields

| Name             | Type   | Description                                                                                                                                                                                            | Optional |
|------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| entityUrn        | String | The unique identifier for the Entity being changed. For example, a Dataset's urn.                                                                                                                      | False    |
| entityType       | String | The type of the entity being changed. Supported values include dataset, chart, dashboard, dataFlow (Pipeline), dataJob (Task), domain, tag, glossaryTerm, corpGroup, & corpUser.                       | False    |
| category         | String | The category of the change, related to the kind of operation that was performed. Examples include TAG, GLOSSARY_TERM, DOMAIN, LIFECYCLE, and more.                                                     | False    |
| operation        | String | The operation being performed on the entity given the category. For example, ADD ,REMOVE, MODIFY. For the set of valid operations, see the full catalog below.                                         | False    |
| modifier         | String | The modifier that has been applied to the entity. The value depends on the category. An example includes the URN of a tag being applied to a Dataset or Schema Field.                                  | True     |
| parameters       | Dict   | Additional key-value parameters used to provide specific context. The precise contents depends on the category + operation of the event. See the catalog below for a full summary of the combinations. | True     |
| auditStamp.actor | String | The urn of the actor who triggered the change.                                                                                                                                                         | False    |
| auditStamp.time  | Number | The timestamp in milliseconds corresponding to the event.                                                                                                                                              | False    |



In following sections, we will provide sample events for each scenario in which Entity Change Events are fired.


### Add Tag Event

This event is emitted when a Tag has been added to an entity on DataHub.

#### Sample Event

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "ADD",
  "modifier": "urn:li:tag:PII",
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Remove Tag Event

This event is emitted when a Tag has been removed from an entity on DataHub.
Header

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TAG",
  "operation": "REMOVE",
  "modifier": "urn:li:tag:PII",
  "parameters": {
    "tagUrn": "urn:li:tag:PII"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Add Glossary Term Event

This event is emitted when a Glossary Term has been added to an entity on DataHub.
Header

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "GLOSSARY_TERM",
  "operation": "ADD",
  "modifier": "urn:li:glossaryTerm:ExampleNode.ExampleTerm",
  "parameters": {
    "termUrn": "urn:li:glossaryTerm:ExampleNode.ExampleTerm"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Remove Glossary Term Event

This event is emitted when a Glossary Term has been removed from an entity on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "GLOSSARY_TERM",
  "operation": "REMOVE",
  "modifier": "urn:li:glossaryTerm:ExampleNode.ExampleTerm",
  "parameters": {
    "termUrn": "urn:li:glossaryTerm:ExampleNode.ExampleTerm"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Add Domain Event

This event is emitted when Domain has been added to an entity on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOMAIN",
  "operation": "ADD",
  "modifier": "urn:li:domain:ExampleDomain",
  "parameters": {
    "domainUrn": "urn:li:domain:ExampleDomain"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Remove Domain Event

This event is emitted when Domain has been removed from an entity on DataHub.
Header

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DOMAIN",
  "operation": "REMOVE",
  "modifier": "urn:li:domain:ExampleDomain",
  "parameters": {
     "domainUrn": "urn:li:domain:ExampleDomain"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Add Owner Event

This event is emitted when a new owner has been assigned to an entity on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "OWNER",
  "operation": "ADD",
  "modifier": "urn:li:corpuser:jdoe",
  "parameters": {
     "ownerUrn": "urn:li:corpuser:jdoe",
     "ownerType": "BUSINESS_OWNER"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Remove Owner Event

This event is emitted when an existing owner has been removed from an entity on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "OWNER",
  "operation": "REMOVE",
  "modifier": "urn:li:corpuser:jdoe",
  "parameters": {
    "ownerUrn": "urn:li:corpuser:jdoe",
    "ownerType": "BUSINESS_OWNER"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Add Structured Property Event

This event is emitted when a Structured Property has been added to an entity on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "STRUCTURED_PROPERTY",
  "operation": "ADD",
  "modifier": "urn:li:structuredProperty:prop1",
  "parameters": {
    "propertyUrn": "urn:li:structuredProperty:prop1",
    "propertyValues": "[\"value1\"]"
  },
  "version": 0,
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Remove Structured Property Event

This event is emitted when a Structured Property has been removed from an entity on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "STRUCTURED_PROPERTY",
  "operation": "REMOVE",
  "modifier": "urn:li:structuredProperty:prop1",
  "version": 0,
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Modify Structured Property Event

This event is emitted when a Structured Property's values have been modified on an entity in DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "STRUCTURED_PROPERTY",
  "operation": "MODIFY",
  "modifier": "urn:li:structuredProperty:prop1",
  "parameters": {
    "propertyUrn": "urn:li:structuredProperty:prop1",
    "propertyValues": "[\"value1\",\"value2\"]"
  },
  "version": 0,
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```

### Modify Deprecation Event

This event is emitted when the deprecation status of an entity has been modified on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "DEPRECATION",
  "operation": "MODIFY",
  "modifier": "DEPRECATED",
  "parameters": {
    "status": "DEPRECATED"
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Add Dataset Schema Field Event

This event is emitted when a new field has been added to a Dataset Schema. 

#### Sample Event

```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TECHNICAL_SCHEMA",
  "operation": "ADD",
  "modifier": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
  "parameters": {
    "fieldUrn": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
    "fieldPath": "newFieldName",
    "nullable": false
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Remove Dataset Schema Field Event

This event is emitted when a new field has been remove from a Dataset Schema. 

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "TECHNICAL_SCHEMA",
  "operation": "REMOVE",
  "modifier": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
  "parameters": {
    "fieldUrn": "urn:li:schemaField:(urn:li:dataset:abc,newFieldName)",
    "fieldPath": "newFieldName",
    "nullable": false
  },
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Entity Create Event

This event is emitted when a new entity has been created on DataHub. 
Header

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "CREATE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Entity Soft-Delete Event 

This event is emitted when a new entity has been soft-deleted on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "SOFT_DELETE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```


### Entity Hard-Delete Event

This event is emitted when a new entity has been hard-deleted on DataHub.

#### Sample Event
```json
{
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "category": "LIFECYCLE",
  "operation": "HARD_DELETE",
  "auditStamp": {
    "actor": "urn:li:corpuser:jdoe",
    "time": 1649953100653   
  }
}
```
