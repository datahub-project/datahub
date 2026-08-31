## Overview

Aerospike is a high-performance, distributed NoSQL key-value database designed for real-time applications. Learn more in the [official Aerospike documentation](https://aerospike.com/docs/).

The DataHub integration for Aerospike extracts namespaces as containers and sets as datasets, including schema inference from sampled records. It also supports platform instance mapping, stateful ingestion for stale entity removal, and optional XDR (Cross-Datacenter Replication) metadata.

## Concept Mapping

| Source Concept | DataHub Concept   | Notes                                             |
| -------------- | ----------------- | ------------------------------------------------- |
| Namespace      | Container         | Top-level grouping of sets within a cluster.      |
| Set            | Dataset           | Primary ingested entity, analogous to a table.    |
| Record bins    | SchemaField       | Inferred from sampled records via schema probing. |
| Cluster        | Platform Instance | Optional; use when ingesting multiple clusters.   |
