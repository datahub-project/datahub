import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Browse

<FeatureAvailability/>

Browse is one of the primary entrypoints for discovering different Datasets, Dashboards, Charts and other DataHub Entities.

Browsing is useful for finding data entities based on a hierarchical structure set in the source system. Generally speaking, that hierarchy will contain the following levels:

* Entity Type (Dataset, Dashboard, Chart, etc.)
* Environment (prod vs. dev)
* Platform Type (Snowflake, dbt, Looker, etc.)
* Container (Warehouse, Schema, Folder, etc.)
* Entity Name

For example, a user can easily browse for Datasets within the PROD Snowflake environment, the long_tail_companions warehouse, and the analytics schema:

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/browseVid.gif"/>
</p>

## Using Browse

Browse is accessible by clicking on an Entity Type on the front page of the DataHub UI.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/browse1.png"/>
</p>

This will take you into the folder explorer view for browse in which you can drill down to your desired sub categories to find the data you are looking for.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/browse2.png"/>
</p>

## Additional Resources

### GraphQL

* [browse](../graphql/queries.md#browse)
* [browsePaths](../graphql/queries.md#browsePaths)

## FAQ and Troubleshooting

**How are BrowsePaths created?**

BrowsePaths are automatically created for ingested entities based on separator characters that appear within an Urn.

**How can I customize browse paths?**

BrowsePaths are an Aspect similar to other components of an Entity. They can be customized by ingesting custom paths for specified Urns.

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*

### Related Features

* [Search](./how/search.md)
