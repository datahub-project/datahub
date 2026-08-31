### Overview

The `demo-data` source loads curated data packs into DataHub. By default it loads the **bootstrap** sample data (datasets, dashboards, users, and tags) with original timestamps.

It also supports loading named packs from the DataHub registry (e.g. `showcase-ecommerce`, `covid-bigquery`) or custom URLs, with optional time-shifting to make timestamps appear recent.

Use this source for demos, testing, or bootstrapping a DataHub instance with realistic metadata.

### Prerequisites

A running DataHub instance. No external credentials or network access beyond the pack URL is required.
