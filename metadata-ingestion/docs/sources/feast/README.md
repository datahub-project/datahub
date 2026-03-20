## Overview

Feast is a machine learning platform. Learn more in the [official Feast documentation](https://feast.dev/).

The DataHub integration for Feast covers ML entities such as models, features, and related lineage metadata. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

- Entities as `MLPrimaryKey`
- Fields as `MLFeature`
- Feature views and on-demand feature views as `MLFeatureTable`
- Batch and stream source details as `Dataset`
- Column types associated with each entity and feature

Use this mapping to align feature-store metadata with existing ML entity governance patterns in DataHub.
