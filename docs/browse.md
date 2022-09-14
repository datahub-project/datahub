import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Browse

<FeatureAvailability/>

Browse is one of the primary entrypoints for discovering different Datasets, Dashboards, Charts and other DataHub Entities.

Browsing is useful for finding data based on a hierarchical structure. A user might want to find all of their Datasets of a particular platform
under a particular naming prefix for an organization, for example finding all Snowflake Datasets under the long_tail_companion organization organized under the analytics prefix.

## Using Browse

Browse is accessible by clicking into an entity on the front page of the DataHub UI.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/browse1.png"/>
</p>

This will take you into the folder explorer view for browse in which you can drill down to your desired sub categories to find the data you are looking for.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/browse2.png"/>
</p>

## Additional Resources

### GraphQL

* [browse](../../graphql/queries.md#browse)
* [browsePaths](../../graphql/queries.md#browsePaths)

## FAQ and Troubleshooting

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*

### Related Features

* [Search](./docs/how/search.md)