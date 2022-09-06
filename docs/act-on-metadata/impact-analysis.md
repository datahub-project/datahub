import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Lineage Impact Analysis

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

4. Slice and dice the result list by Entity Type, Platfrom, Owner, and more to isolate the relevant dependencies

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

## Additional Resources

### Videos

**DataHub 201: Impact Analysis**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/BHG_kzpQ_aQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

* [searchAcrossLineage](../../graphql/queries.md#searchacrosslineage)
* [searchAcrossLineageInput](../../graphql/inputObjects.md#searchacrosslineageinput)

### DataHub Blog

* [Dependency Impact Analysis, Data Validation Outcomes, and MORE! - Highlights from DataHub v0.8.27 & v.0.8.28](https://blog.datahubproject.io/dependency-impact-analysis-data-validation-outcomes-and-more-1302604da233)


### FAQ and Troubleshooting

**The Lineage Tab is greyed out - why can’t I click on it?**

This means you have not yet ingested Lineage metadata for that entity. Please see the Lineage Guide to get started.

**Why is my list of exported dependencies incomplete?**

We currently limit the list of dependencies to 10,000 records; we suggest applying filters to narrow the result set if you hit that limit.

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*

### Related Features

* [DataHub Lineage](./docs/lineage/intro.md)
