# DataHub FAQs

## Who are the major contributors in the community?
Currently just LinkedIn. We’re receiving more and more PRs from individual companies.

## How big is the community?
We had couple of meetings/discussions with external parties who are interested in DataHub such as:
- [City of New York, DoITT](https://www1.nyc.gov/site/doitt/index.page)
- [Experian](https://www.experian.com/)
- [Geotab](https://www.geotab.com/)
- [Instructure](https://www.instructure.com/)
- [Intuit](https://www.intuit.com/)
- [Itaú Unibanco](https://www.itau.com/)
- [LavaMap](https://lavamap.com/)
- [Microsoft](https://www.microsoft.com/)
- [Morgan Stanley](https://www.morganstanley.com/)
- [Paypal](https://www.paypal.com/)
- [Prophecy.io](https://prophecy.io/)
- [SAXO Bank](https://www.home.saxo/)
- [Sysco AS](https://sysco.no/)
- [Tenable](https://www.tenable.com/)
- [Thoughtworks](https://www.thoughtworks.com/)
- [TypeForm](https://www.typeform.com/)
- [Valassis](https://www.valassis.com/)
- [Vrbo (Expedia Group)](https://www.vrbo.com/)

## Is there a contributor selection criteria?
We welcome contributions from everyone in the community. More guidelines https://github.com/linkedin/datahub/blob/master/CONTRIBUTING.md. In general, we will review PRs as we review our internal code - that's how we maintain quality.

## How does LinkedIn plan to engage with the community?
We plan to organize public town hall meetings at bi-weekly cadence - could change depending upon interest from the community. Also we recently deprecated Gitter & migrated to [Slack](https://datahubspace.slack.com/join/shared_invite/zt-cl60ng6o-6odCh_I~ejZKE~a9GG30PA).

## Should this be the platform we decide upon, we’d like to fully engage and work with LinkedIn and the community. What’s the best way and what level of engagement/involvement should we expect?
The best way to engage is through the [Slack channel](https://datahubspace.slack.com/join/shared_invite/zt-cl60ng6o-6odCh_I~ejZKE~a9GG30PA). You’ll get to interact with the developers and the community. We are fairly responsive. If you have a question, you should expect a response from LinkedIn developers within 24 hrs - during our normal work hours 9:00 - 17:00 Pacific Time, Monday - Friday. For reproducible technical issues, bugs and code contributions, Github [issues](https://github.com/linkedin/datahub/issues) and [PRs](https://github.com/linkedin/datahub/pulls) are preferred.

## What’s the best way to ramp up the product knowledge to properly test and evaluate Datahub?
[Github](https://github.com/linkedin/datahub) is the best resource - we have documented the steps to install and test DataHub thoroughly there. The blog post on [open sourcing Datahub](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) and another on our [product](https://engineering.linkedin.com/blog/2019/data-hub) should be useful resources as well.

## Where can I learn about the roadmap?
You can learn more about the roadmap at https://github.com/linkedin/datahub/blob/master/docs/roadmap.md.

## Where can I learn about the current list of features/functionalities?
You can learn more about the current list of features at https://github.com/linkedin/datahub/blob/master/docs/features.md.

## Are the product strategy/vision/roadmap driven by the LinkedIn Engineering team or community or collaboration effort?
Mixed of both LinkedIn team and community. Roadmap will be a joint effort of both LinkedIn and the community. However, we’ll prioritize the tasks aligning with the community feedback.

## Does DataHub connect with Google Cloud Platform?
LinkedIn team is not using BigQuery platform so we cannot commit to building and testing that functionality. However, we’ll be happy to accept any contributions for that. We’re not aware of any use cases at this point but you can create an issue on GitHub. This will help connect with people which have the same dependencies in the community. Also, our Slack channel and regularly scheduled town hall meetings are a good opportunity to meet with people from different companies who have similar requirements and might be contributing these features.

## How approachable would LinkedIn be to provide insights/support or collaborate on a functionality?
Please take a look at our [roadmap](https://github.com/linkedin/datahub/blob/master/docs/roadmap.md) & [features](https://github.com/linkedin/datahub/blob/master/docs/features.md) to get a sense of what’s being open sourced in the near future. If there’s something missing from the list we’re open to discussion, and town hall would actually be the perfect venue for that.

## How do LinkedIn Engineering team and the community ensure the quality of the community code for Datahub?
All PRs are reviewed by the LinkedIn team. Any extension/contribution coming from the community which LinkedIn team doesn’t have any expertise on will first be put into a staging directory (/contrib) for a while (think of it as incubation). Once it’s blessed by the community and adopted, we’ll graduate it from staging and streamline it into the repo.

In the beginning, LinkedIn will play a big role in terms of stewardship of the code base. We’ll re-evaluate this strategy based on the amount of engagement and energy we see among our contributors. As mentioned in the [blog post](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) as well, you should expect big dumps of code (in batches) instead of every commit within LinkedIn going through a PR. There is a large backlog of capabilities that we have on the internal Datahub - in terms of the entities we support as well as the relationships. So we are going through this effort of generalizing things and moving them across. That will lead to big contributions coming in batches in the near future until the process becomes smoother.

## How are the ETL processes scheduled at LinkedIn?
It varies depending on the data platform. Hive is scheduled as twice a week. HDFS, MySQL, Oracle, LDAP (users) are scheduled on a daily basis. We involve real-time push models in the ingestion for a couple of data platforms such as Kafka, [Espresso](https://engineering.linkedin.com/espresso/introducing-espresso-linkedins-hot-new-distributed-document-store), [Ambry](https://github.com/linkedin/ambry) etc.

## What are the options for the Kafka Key for MCE, MAE and FailedMCE Topic?
URN is the only sensible option.

## How is the Data Quality of the Ingestion messages handled?
In addition to leverage the Kafka schema validation to ensure the MXEs output from metadata producer, we also actively monitor the ingestion streaming pipeline on the snapshot level with status.

## Can you give a high level overview about how Data Governance is handled? What privacy/governance use cases are supported in LinkedIn through DataHub?
This talk ([slides](https://www.slideshare.net/ShirshankaDas/taming-the-everevolving-compliance-beast-lessons-learnt-at-linkedin-strata-nyc-2017), [video](https://www.youtube.com/watch?v=O1DI0fuY8PM)) describes the role of metadata (DataHub née WhereHows) in the Data Governance / Privacy space at LinkedIn. Field-level, dataset-level classification, governed data movement, automatic data deletion, export etc. are the supported use cases. We have plans to open source the compliance capabilities, listed as part of our [roadmap](https://github.com/linkedin/datahub/blob/master/docs/roadmap.md#compliance-management-for-datasets).

## When using Kafka and Confluent Schema Registry, does Datahub support multiple schemas for the same topic?
You can [configure](https://docs.confluent.io/current/schema-registry/develop/api.html#compatibility) compatibility level per topic at Confluent Schema Registry. The default being used is “Backward”. So, you’re only allowed to make backward compatible changes on the topic schema. You can also change this configuration and flex compatibility check. However, as a best practice, we would suggest not doing backward incompatible changes on the topic schema because this will fail your old metadata producers’ flows. Instead, you might consider creating a new Kafka topic (new version).

## How do we better document and map transformations within an ETL process? How do we create institutional knowledge and processes to help create a paradigm for tribal knowledge?
We plan to add “fine-grain lineage” in the near future, which should cover the transformation documentation. DataHub currently has a simple “Docs” feature that allows capturing of tribal knowledge. We also plan to expand it significantly going forward.

## How do we advance the product from a Data Catalog Browser to a Data Collaboration environment like Alation?
We are adding some “social features” and documentation captures to DataHub. However, we do welcome the community to contribute in this area.

## Can you share how the catalog looks in LinkedIn production?
It’s very similar to what you see on the community version. We have added screenshots of the internal version of the catalog in our [blog post](https://engineering.linkedin.com/blog/2019/data-hub).

## Does the roadmap have provision for capturing the Data Quality Information of the Dataset?
We’re working on a similar [feature](https://engineering.linkedin.com/blog/2020/data-sentinel-automating-data-validation) internally. Will evaluate and update the roadmap once we have a better idea of the timeline.

## Is DataHub capturing/showing column level constraints set at table definition?
Assuming constraints here refer to the [SQL constraints](https://www.w3schools.com/sql/sql_constraints.asp) - our [SchemaField](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/schema/SchemaField.pdsc) model currently does not store any property/field corresponding to constraints defined in the table definition. It should be possible to extend the model and add the constraint field.

## How does DataHub manage extracting metadata from stores residing in different security zones?
MCE is the ideal way to push metadata from different security zones, assuming there is a common Kafka infrastructure that aggregates the events from various security zones.

## What all data stores does DataHub backend support presently?
Currently, DataHub supports all major database providers that are supported by Ebean as the document store i.e. Oracle, Postgres, MySQL, H2. We support [Espresso](https://engineering.linkedin.com/espresso/introducing-espresso-linkedins-hot-new-distributed-document-store) as well, only at LinkedIn. Other than that, we support Elasticsearch and Neo4j for search and graph use cases, respectively. However, data stores in the backend are all abstracted and accessed through DAOs, you should be able to easily support your own data stores by plugging your DAO implementations. You can refer to https://github.com/linkedin/datahub/blob/master/docs/architecture/metadata-serving.md.

## For which stores, you have discovery services?
Supported data sources are listed at https://github.com/linkedin/datahub/tree/master/metadata-ingestion. To onboard your own data source which is not listed there, you can refer to the onboarding guide https://github.com/linkedin/datahub/blob/master/docs/how/data-source-onboarding.md.


