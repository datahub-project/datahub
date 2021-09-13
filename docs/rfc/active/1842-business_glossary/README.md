- Start Date: 08/28/2020
- RFC PR: 1842
- Implementation PR(s): TBD

# Business Glossary

## Summary

Adding Support for Business Glossary enhances the value of metadata and brings the business view. This helps to document the business terms used across the business and provides the common vocabulary to entire data stakeholders/community. This encourages/motivates the business community to interact with Data Catalog to discover the relevant data assets of interest. This also enables finding the relationship in the data assets through business terms that belong to them. This following link illustrates the importance of business glossary [article](https://dataedo.com/blog/business-glossary-vs-data-dictionary).

## Motivation

We need to model Business Glossary, where the business team can define the business terms and link them to the data elements being onboarded to Data Platforms/data catalogs. This gives the following benefits :
- Define and enable common vocabulary in the organizations and enable easy collaborations with the business & technical communities
- Organizations can leverage the existing industry taxonomies where they can import the definitions and can enhance or define there specific terms/definitions
- the crux and use of business glossary will be by linking the dataset/elements to Business Terms, so that business/consumers can discover the interested datasets easily with the helps of business terms
- Promote the usage and reduce the redundancy: Business Glossary helps to discover the datasets quickly through business terms and this also helps reducing unnecessary onboarding the same/similar datasets by different consumers.  

## Detailed design

### What is Business Glossary 
**Business Glossary**, is a list of business terms with their definitions. It defines business concepts for an organization or industry and is independent from any specific database or platform or vendor.

**Data Dictionary** is a description of a data set, provides the details about the attributes and data types

### Relationship 
Even though Data Dictionary and Business Glossary are separate entities, they work nicely together to describe different aspects and levels of abstraction of the data environment of an organization.
Business terms can be linked to specific entities/tables and columns in a data asset/data dictionary to provide more context and consistent approved definition to different instances of the terms in different platforms/databases.


### Sample Business Glossary Definition
|URN|Business Term |Definition  | Domain/Namespace | Owner | Ext Source| Ext Reference |
|--|--|--|--|--|--|--|
|urn:li:glossaryTerm:instrument.cashInstrument | instrument.cashInstrument| time point including a date and a time, optionally including a time zone offset| Foundation | abc@domain.com | fibo | https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/CashInstrument |
|urn:li:glossaryTerm:common.dateTime | common.dateTime| a financial instrument whose value is determined by the market and that is readily transferable (highly liquid)| Finance | xyz@domain.com | fibo | https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/DateTime |
|urn:li:glossaryTerm:market.bidSize | market.bidSize| The bid size represents the quantity of a security that investors are willing to purchase at a specified bid price| Trading | xyz@domain.com | - | - | - |
|--|--|--|--|--|--|--|
| | | | | | | |

### Business Term  & Dataset - Relationship

| Attribute Name| Data Type| Nullable?| **Business Term**| Description|
|--|--|--|--|--|
| recordId| int| N| | |n the case of FX QuoteData the RecordId is equal to the UIC from SymbolsBase|
| arrivalTime| TimestampTicks| N| | Time the price book was received by the TickCollector. 100s of Nanoseconds since 1st January 1970 (ticks)|
| bid1Price| com.xxxx.yyy.schema.common.Price| N| **common.monetoryAmount**|The bid price with rank 1/29.|
| bid1Size| int| N| market.bidSize| The amount the bid price with rank 5/29 is good for.|
|--|--|--|--|--|--|--|
| | | | | | | |

### Stiching Together


Business Glossary will be a first class entity where one can define the `GlossaryTerm`s and this will be similar to entities like Dataset, CorporateUser etc. Business Term can be linked to other entities like Dataset, DatasetField. In future Business terms can be linked to Dashboards, Metrics etc


![high level design](business_glossary_rel.png)

The above diagram illustrates how Business Terms will be connected to other entities entities like Dataset, DatasetField. The above example depicts business terms are `Term-1`, `Term-2`, .. `Term-n` and how they are linked to `DatasetField` and `Dataset`. 
Dataset (`DS-1`) fields `e11` is linked to Business Term `Term-2` and `e12` is linked to `Term-1`. 
Dataset (`DS-2`) element `e23` linked the Business Term `Term-2`, `e22` with `Term-3` and `e24` with `Term-5`. Dataset (DS-2) is linked to business term `Term-4`
Dataset (`DS-2`) it-self linked to Business Term `Term-4`

## Metadata Model Enhancements 

There will be 1 top level GMA [entities](../../../what/entity.md) in the design: glossaryTerm (Business Glossary).
It's important to make glossaryTerm as a top level entity because it can exist without a Dataset and can be defined independently by the business team.

### URN Representation
We'll define a [URNs](../../../what/urn.md): `GlossaryTermUrn`.
These URNs should allow for unique identification of business term.  

A business term  URN (GlossaryTermUrn) will look like below:
```
urn:li:glossaryTerm:<<name>>
```

### New Snapshot Object
There will be new snapshot object to onboard business terms along with definitions

Path : metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot/
```java
/**
 * A metadata snapshot for a specific GlossaryTerm entity.
 */
record GlossaryTermSnapshot {

  /**
   * URN for the entity the metadata snapshot is associated with.
   */
  urn: GlossaryTermUrn

  /**
   * The list of metadata aspects associated with the dataset. Depending on the use case, this can either be all, or a selection, of supported aspects.
   */
  aspects: array[GlossaryTermAspect]
}
```

Path : metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/

### GlossaryTermAspect
There will be new aspect defined to capture the required attributes & ownership information

```
/**
 * A union of all supported metadata aspects for a GlossaryTerm
 */
typeref GlossaryTermAspect = union[
  GlossaryTermInfo,
  Ownership
]
```

Business Term Entity Definition
```java
/**
 * Data model for a Business Term entity
 */
record GlossaryTermEntity includes BaseEntity {

  /**
   * Urn for the dataset
   */
  urn: GlossaryTermUrn

  /**
   * Business Term native name e.g. CashInstrument
   */
  name: optional string

}
```

### Entity GlossaryTermInfo

```java
/**
 * Properties associated with a GlossaryTerm
 */
record GlossaryTermInfo {

  /**
   * Definition of business term
   */
  definition: string

  /**
   * Source of the Business Term (INTERNAL or EXTERNAL) with default value as INTERNAL
   */
  termSource: string

  /**
   * External Reference to the business-term (URL)
   */
  sourceRef: optional string

  /**
   * The abstracted URI such as https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/CashInstrument.
   */
  sourceUrl: optional Url

  /**
   * A key-value map to capture any other non-standardized properties for the glossary term
   */
  customProperties: map[string, string] = { }

}

```

### Business Term Realationship with Owner
Business Terms will be owened by certain business users

```
/**
 * A generic model for the Owned-By relationship
 */
@pairings = [ {
  "destination" : "com.linkedin.common.urn.CorpuserUrn",
  "source" : "com.linkedin.common.urn.GlossaryTermUrn"
}, {
   "destination" : "com.linkedin.common.urn.GlossaryTermUrn",
   "source" : "com.linkedin.common.urn.CorpuserUrn"
} ]
record OwnedBy includes BaseRelationship {

  /**
   * The type of the ownership
   */
  type: OwnershipType
}
```

### Business Glossary Aspect
Business Term can be asociated with Dataset Field as well as Dataset. Defning the aspect that can be asociated with Dataset and DatasetField 

```
record GlossaryTerms {
  /**
   * The related business terms
   */
  terms: array[GlossaryTermAssociation]

  /**
   * Audit stamp containing who reported the related business term
   */
  auditStamp: AuditStamp
}

record GlossaryTermAssociation {
   /**
    * Urn of the applied glossary term
    */
    urn: GlossaryTermUrn
}
```

Proposed to have the following changes to the SchemaField to associate (optionally) with Business Glossary (terms)

```
record SchemaField {
  ...
  /**
   * Tags associated with the field
   */
  globalTags: optional GlobalTags

 +/**
 + * Glossary terms associated with the field
 + */
 +glossaryTerms: optional GlossaryTerms
}
```


Proposed to have the following changes to the Dataset aspect to associate (optionally) with Business Glossary (terms)

```
/**
 * A union of all supported metadata aspects for a Dataset
 */
typeref DatasetAspect = union[
  DatasetProperties,
  DatasetDeprecation,
  UpstreamLineage,
  InstitutionalMemory,
  Ownership,
  Status,
  SchemaMetadata
+ GlossaryTerms
]
```

## Metadata Graph

This might not be a crtical requirement, but nice to have.

1. Users should be able to search for Business Terms and would like to see all the Datasets that have elements that linked to that Business term.  

## How we teach this

We should create/update user guides to educate users for:
 - Importance and value that Business Glossary bringing to the Data Catalog
 - Search & discovery experience through business terms (how to find a relevant datasets quickly in DataHub)

## Alternatives
This is a new feature in Datahub that brings the common vocabulry across data stake holders and also enable better discoverability to the datasets. I see there is no clear alternative to this feature, at the most users can document the `business term` outside the `Data Catalog` and can reference/assosciate those terms as an additional property to Dataset column.


## Rollout / Adoption Strategy

The design is supposed to be generic enough that any user of DataHub should easily be able to onboard their Business Glossary (list of terms and definitions) to DataHub irrespective of their industry. Some organizations can subscribe/download industry standard taxonomy with slight modelling and integration should be able to bring the business glossary quickly

While onboarding datasets, business/tech teams need to link the business terms to the data elements, once users see the value of this will be motivated to link the elements with appropriate business terms.

## Unresolved questions

- This RFC does not cover the UI design for Business Glossary Definition.
