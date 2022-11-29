---
description: >-
  This page describes unique resource names, or URNs, which are used by DataHub
  to uniquely identify Metadata Entities.
---

# URNs

## Introduction

DataHub leverages unique resource names, or URNs to uniquely identify Metadata Entities on the platform. URNs always have the following format:

```
urn:li:<entity-type>:<urn-parts>
```

where `<entity-type>` denotes the type of the entity, for examples `Datasets` or `Tags` and `<urn-parts>` denotes a set of attributes that uniquely identify the Metadata Entity.

For URNs having a single part, we can omit the parenthesis. For those having multiple, parens are required.&#x20;

For some entities, primarily those which are created based on state stored in 3rd party systems like Databases or Data Warehouses, the URN is used to encode the coordinates of a particular Entity. For example, Dataset URNs today contain reference to the a) Data Platform Type and b) Dataset identifier within that platform as its unique coordinates:

```
urn:li:dataset:(urn:li:dataPlatform:mysql,db.tableName,env)
```

As you can see, URNs sometimes appear nested within other URNs.&#x20;

For the remainder of this document, we will outline the required URN format for each DataHub Metadata Entity type.&#x20;

## Dataset URNs

Dataset URNs are composed of the following attributes:

* **dataPlatformUrn: String -** A Data Platform URN (see below)&#x20;
* **name: String -** A dot-separated name that uniquely identifies the Dataset within the Data Platform. The exact structure can depend on the Data Platform Type, but roughly translates to `<schema>.<database>.<table name>`.&#x20;
* **environment**: **String** - An environment associated with the table. The can be used to denote "deployment environments", such as `PROD` or `DEV`, or can be used to identify a particular instance of the Data Platform Type (e.g. mysql\_1)&#x20;

Each value should be URL-encoded and embedded into a Dataset URN string as follows:

```
urn:li:dataset:(dataPlatformUrn,name,environment)
```

For example:

```
urn:li:dataset:(urn:li:dataPlatform:mysql,product.sales,PROD)
```

## Data Platform URNs

Data Platform URNs are composed of the following attributes:

* **type: String -** A standard name associated with the Dataset's Data Platform. **** Valid values include:
  * mysql
  * snowflake
  * redshift
  * bigquery
  * hdfs
  * hive
  * s3
  * kafka
  * mongodb
  * oracle
  * postgres
  * pinot
  * presto
  * mssql
  * druid

Check out the full list of supported Data Platforms [here](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/examples/mce\_files/data\_platforms.json).&#x20;

The format of a DataPlatform URN is as follows:

```
urn:li:dataPlatform:name
```

For example:

```
urn:li:dataPlatform:mysql
```

## Dataset URNs

Data Platform URNs are composed of the following attributes:

* **dataPlatform: Urn -** A Data Platform URN (see below)&#x20;
* **name: String -** A dot-separated name that uniquely identifies the Dataset within the Data Platform. The exact structure can depend on the Data Platform Type, but roughly translates to `<schema>.<database>.<table name>`.&#x20;
* **environment**: **String** - An environment associated with the table. The can be used to denote "deployment environments", such as `PROD` or `DEV`, or can be used to identify a particular instance of the Data Platform Type (e.g. mysql\_1)&#x20;

