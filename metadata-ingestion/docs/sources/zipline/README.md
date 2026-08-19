## Overview

Zipline is the machine learning feature platform built on top of [Chronon](https://chronon.ai/). It defines feature pipelines (GroupBys), training-set joins (Joins), and pre-processing steps (StagingQueries) as code, then compiles them into a repository of thrift-as-JSON configuration files.

The DataHub integration for Zipline reads that compiled repository from disk and captures feature tables, individual features, primary keys, the backing warehouse and streaming sources, and the Joins and StagingQueries that produce feature data. It also supports table-level lineage (including SQL-parsed StagingQuery lineage), tag and ownership extraction, and stateful deletion detection.

## Concept Mapping

- GroupBys as `MLFeatureTable`
- Aggregation and derivation outputs as `MLFeature`
- Key columns as `MLPrimaryKey`
- Backing batch tables and streaming topics as `Dataset`
- Joins and StagingQueries as `DataJob`, grouped per team as a `DataFlow`

Use this mapping to align feature-store metadata with existing ML entity governance patterns in DataHub.
