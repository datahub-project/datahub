<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

### Setup

This source pulls dbt metadata directly from the dbt Cloud APIs.

Create a [service account token](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens) with the "Metadata Only" permission.
This is a read-only permission.

#### Operating Modes

The dbt Cloud source supports two modes of operation:

##### 1. Explicit Mode (Default)

Specify a single dbt Cloud job to ingest metadata from. You'll need to have a dbt Cloud job set up to run your dbt project, and "Generate docs on run" should be enabled.

Note: As this is ingesting only one job, we expect it to process all/most of the models, or else multiple job ingestion might be required.

To get the required IDs, go to the job details page (this is the one with the "Run History" table), and look at the URL.
It should look something like this: https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094.
In this example, the account ID is 107298, the project ID is 175705, and the job ID is 148094.

##### 2. Auto-Discovery Mode

Automatically discovers and ingests metadata from all eligible jobs in a dbt Cloud project. This mode:

- Discovers all jobs in the specified project's **production environment only**
- Filters to jobs with **"Generate docs on run" enabled** (`generate_docs=True`)
- Always uses the **latest run** for each job (ignores `run_id` configuration)
- Supports optional regex-based filtering to include/exclude specific job IDs
- Ingests metadata from multiple jobs in a single run

**When to use auto-discovery:**

- You have multiple dbt Cloud jobs in a project and want to ingest all of them
- You want to automatically pick up new jobs without updating configuration

**Requirements:**

- Jobs must be in the production environment
- Jobs must have "Generate docs on run" enabled
