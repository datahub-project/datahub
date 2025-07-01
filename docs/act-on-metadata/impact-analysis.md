import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Lineage Impact Analysis

<FeatureAvailability/>

Lineage Impact Analysis is a powerful workflow for understanding the complete set of upstream and downstream dependencies of a Dataset, Dashboard, Chart, and many other DataHub Entities.

This allows Data Practitioners to proactively identify the impact of breaking schema changes or failed data pipelines on downstream dependencies, rapidly discover which upstream dependencies may have caused unexpected data quality issues, and more.

Lineage Impact Analysis is available via the DataHub UI and GraphQL endpoints, supporting manual and automated workflows.

## Lineage Impact Analysis Setup, Prerequisites, and Permissions

Lineage Impact Analysis is enabled for any Entity that has associated Lineage relationships with other Entities and does not require any additional configuration.

Any DataHub user with “View Entity Page” permissions is able to view the full set of upstream or downstream Entities and export results to CSV from the DataHub UI.

## Using Lineage Impact Analysis

Follow these simple steps to understand the full dependency chain of your data entities.

1. On a given Entity Page, select the **Lineage** tab

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-lineage-tab.png"/>
</p>

2. Easily toggle between **Upstream** and **Downstream** dependencies

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-choose-upstream-downstream.png"/>
</p>

3. Choose the **Degree of Dependencies** you are interested in. The default filter is “1 Degree of Dependency” to minimize processor-intensive queries.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-filter-dependencies.png"/>
</p>

4. Slice and dice the result list by Entity Type, Platform, Owner, and more to isolate the relevant dependencies

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-apply-filters.png"/>
</p>

5. Export the full list of dependencies to CSV

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-export-full-list.png"/>
</p>

6. View the filtered set of dependencies via CSV, with details about assigned ownership, domain, tags, terms, and quick links back to those entities within DataHub

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-view-export-results.png"/>
</p>

### Known Issues

Impact Analysis is a powerful feature that can place significant demands on the system. To maintain high performance when handling large result sets, we've implemented "Lightning Cache" - an alternate processing path that delivers results more quickly. By default, this cache activates with simple queries when there are more than 300 assets in the result set. You can customize this threshold by setting the environment variable `CACHE_SEARCH_LINEAGE_LIGHTNING_THRESHOLD` in your GMS pod.

However, the Lightning Cache has a limitation: it may include assets that are soft-deleted or no longer exist in the DataHub database. This occurs because lineage references may contain "ghost entities" (URNs without associated data).

Note that when you download Impact Analysis results, our system properly filters out these soft-deleted and non-existent assets. As a result, you might notice differences between what appears in the UI and what appears in your downloaded results.

## Additional Resources

### Videos

**DataHub 201: Impact Analysis**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/BHG_kzpQ_aQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

- [searchAcrossLineage](../../graphql/queries.md#searchacrosslineage)
- [searchAcrossLineageInput](../../graphql/inputObjects.md#searchacrosslineageinput)

Looking for an example of how to use `searchAcrossLineage` to read data lineage? Look [here](../api/tutorials/lineage.md#read-lineage)

### DataHub Blog

- [Dependency Impact Analysis, Data Validation Outcomes, and MORE! - Highlights from DataHub v0.8.27 & v.0.8.28](https://medium.com/datahub-project/dependency-impact-analysis-data-validation-outcomes-and-more-1302604da233)

### FAQ and Troubleshooting

**The Lineage Tab is greyed out - why can’t I click on it?**

This means you have not yet ingested Lineage metadata for that entity. Please see the Lineage Guide to get started.

**Why is my list of exported dependencies incomplete?**

We currently limit the list of dependencies to 10,000 records; we suggest applying filters to narrow the result set if you hit that limit.

### Related Features

- [DataHub Lineage](../generated/lineage/lineage-feature-guide.md)
