import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Generating Browse Paths (V2)

<FeatureAvailability/>

## Introduction

Browse (V2) is a way for users to explore and dive deeper into their data. Its integration with the search experience allows users to combine search queries and filters with entity type and platform nested folders.

Most entities should have a browse path that allows users to navigate the left side panel on the search page to find groups of entities under different folders that come from these browse paths. Below, you can see an example of the sidebar with some new browse paths.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/browseV2/browseV2Sidebar.png"/>
</p>

This new browse sidebar always starts with Entity Type, then optionally shows Environment (PROD, DEV, etc.) if there are 2 or more Environments, then Platform. Below the Platform level, we render out folders that come directly from entity's [browsePathsV2](https://datahubproject.io/docs/generated/metamodel/entities/dataset#browsepathsv2) aspects.

## Generating Custom Browse Paths

A `browsePathsV2` aspect has a field called `path` which contains a list of `BrowsePathEntry` objects. Each object in the path represents one level of the entity's browse path where the first entry is the highest level and the last entry is the lowest level.

If an entity has this aspect filled out, their browse path will show up in the browse sidebar so that you can navigate its folders and select one to filter search results down.

For example, in the browse sidebar on the left of the image above, there are 10 Dataset entities from the BigQuery Platform that have `browsePathsV2` aspects that look like the following:

```
[ { id: "bigquery-public-data" }, { id: "covid19_public_forecasts" } ]
```

The `id` in a `BrowsePathEntry` is required and is what will be shown in the UI unless the optional `urn` field is populated. If the `urn` field is populated, we will try to resolve this path entry into an entity object and display that entity's name. We will also show a link to allow you to open up the entity profile.

The `urn` field should only be populated if there is an entity in your DataHub instance that belongs in that entity's browse path. This makes most sense for Datasets to have Container entities in the browse paths as well as some other cases such as a DataFlow being part of a DataJob's browse path. For any other situation, feel free to leave `urn` empty and populate `id` with the text you want to be shown in the UI for your entity's path.

## Additional Resources

### GraphQL

* [browseV2](../../graphql/queries.md#browsev2)

## FAQ and Troubleshooting

**How are browsePathsV2 aspects created?**

We create `browsePathsV2` aspects for all entities that should have one by default when you ingest your data if this aspect is not already provided. This happens based on separator characters that appear within an Urn.

Our ingestion sources are also producing `browsePathsV2` aspects since CLI version v0.10.5.

### Related Features

* [Search](../how/search.md)
