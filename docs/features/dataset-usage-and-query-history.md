import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Dataset Usage & Query History

<FeatureAvailability/>

Dataset Usage & Query History can give dataset-level information about the top queries which referenced a dataset.

Usage data can help identify the top users who probably know the most about the dataset and top queries referencing this dataset.
You can also get an overview of the overall number of queries and distinct users.
In some sources, column level usage is also calculated, which can help identify frequently used columns.

With sources that support usage statistics, you can collect Dataset, Dashboard, and Chart usages.

## Dataset Usage & Query History Setup, Prerequisites, and Permissions

To ingest Dataset Usage & Query History data, you should check first on the specific source doc
if it is supported by the Datahub source and how to enable it.

You can validate this on the Datahub source's capabilities section:
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/source-snowflake-capabilities.png"/>
</p>

There are some sources where you have to use a different usage specific source for usage ingestion. In this
case it is noted on the capabilities summary like in the example below.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/source-redshift-capabilities.png"/>
</p>

Please, always check the usage prerequisities page if the source has as it can happen you have to add additional
permissions which only needs for usage.

## Using Dataset Usage & Query History

After successful ingestion, the Query tab will be enabled on datasets with any usage.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-queries-tab.png"/>
</p>

On the query tab, you can see the top 5 queries which referenced this dataset.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-query-history-page.png"/>
</p>

On the Stats tab, you can see the top users who run the most queries which referenced this dataset
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-usage-stats-tab.png"/>
</p>

With the collected usage data, you can even see column-level usage statistics (on sources that support this):
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-usage-stats-tab.png"/>
</p>

## Additional Resources

### Videos

**DataHub 101: Data Profiling and Usage Stats 101**
<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/d4S7RgWUg5U?start=254" title="DataHub 101: Data Profiling" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

- <https://datahubproject.io/docs/graphql/objects#usageaggregationmetrics>
- <https://datahubproject.io/docs/graphql/objects#userusagecounts>
- <https://datahubproject.io/docs/graphql/objects#dashboardstatssummary>
- <https://datahubproject.io/docs/graphql/objects#dashboarduserusagecounts>

## FAQ and Troubleshooting

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
