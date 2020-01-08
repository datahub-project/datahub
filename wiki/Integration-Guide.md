1. <a href="#introduction">Introduction</a>
2. <a href="#push">Push data through API</a>
3. <a href="#newETLtypes">New ETL job types</a>

<a name="introduction">

## Introduction

WhereHows' most valuable part is that it ingests information from different systems, integrating the data to mine it for insights. The more source systems it can collect, the higher value it brings.

There are several built-in ETL types, which only need configure some connection and running environment information then they are good to go. See the details in [Set Up New Jobs](Set-Up-New-Metadata-ETL-Jobs.md). A user can also push the data through an API or even create new ETL job types as needed.

<a name="push">
## Push data through API

Because the ETL process can be highly customized, a user can do their own ETL process completely outside, and then push the result into WhereHows repository by using the post API. The benefit of using the API to integrate a new data source is that our system will reflect the data in real time. The common use case is when there is an external service that executes the ETL process, and posts the metadata once the ETL job is done or a milestone/checkpoint is reached.

We provide Post APIs for Dataset and Lineage:

- Dataset: [Add a dataset record](Backend-API.md#dataset-post-api)
- Lineage: [Add a lineage record](Backend-API.md#job-lineage-post-api)


# Add a New Metadata ETL Type

Besides customized build-in ETL types, you can also implement your own types. For the ease of integration, we define some *data interfaces* for every ETL step. They are basically schemas that map to our data models. We recommend using the data interfaces to write to a file, then do batch loading into the database.

Here are the coarse-grained steps to add a new Job type:

  1. Extend the EtlJob abstract class with customized extract(), transform(), load() function

  2. Create a new EtlJobName in backend-service.

  3. Add the new EtlJob subclass option into EtlJobFactory in backend-service

We are making progress at integrating more source systems, and definitely welcome pull requests for integration of new source systems. Below is a list of other systems that we plan to integrate.

### Future Plan

|Type|System|
|---|---|
|Dataset|Oracle|
||MySQL|
||Pinot*|
|Execution|Appworx*|
||Informatica*|
|Lineage|Oozie|
||Appworx*|
||Informatica*|

>\* We already support this function in LinkedIn's internal version, but not yet in the open source version due to some library license limitations.
