### Overview

The `dbt-cloud` module ingests metadata from Dbt into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Setup

Extracts dbt metadata from dbt Cloud APIs.

Create a [service account token](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens) with "Metadata Only" permission (read-only).

#### Operating modes

The dbt Cloud source reads one project in one of two modes.

##### Explicit mode (default)

Specify a single dbt Cloud job to ingest metadata from. The job must have "Generate docs on run" enabled and should process all/most models. The source reads the latest run of that job only (or the run given by `run_id`): models the job does not build still appear, because dbt Cloud returns the whole project, but without build status, compiled code or test results.

To get the required IDs, go to the job details page (this is the one with the "Run History" table), and look at the URL.
It should look something like this: https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094.
In this example, the account ID is 107298, the project ID is 175705, and the job ID is 148094.

##### Auto-discovery mode

Automatically discovers the jobs of a dbt Cloud project and ingests metadata from all of them. This mode:

- Discovers all jobs in the project's **production environment only**
- Keeps the jobs allowed by `job_id_pattern` and `job_type_pattern` (both allow every job by default)
- With `require_generate_docs: true`, keeps only jobs with **"Generate docs on run"** enabled
- Always uses the **latest run** of each job (`run_id` is ignored)
- Ingests metadata from all discovered jobs in a single run

**How the results of several jobs are combined.** A job's latest run returns every node of the project, not only the ones that run built. When several jobs return the same node, a node's metadata comes from the most recent run that actually built it, falling back to the most recent run that saw it (the most recently started run, which carries the freshest code). dbt test results from every job are kept, so an assertion shows the latest result of each job that ran the test. Each exposure is ingested once. `node_merge_strategy: last_job_wins` restores the previous behaviour, where the copy from whichever job dbt Cloud listed last replaced the others.

**When to use auto-discovery:**

- No single job builds the whole project, for example one job per domain or per schedule
- You want new jobs to be picked up without changing the recipe

**Choosing between the modes.** A pinned job is predictable and cheaper: the source sends one set of Discovery API queries instead of one per job. Auto-discovery is the better fit when no single job builds the whole project. Every discovered job adds one Discovery API query per node type, so exclude jobs that add nothing, such as CI or merge jobs, with `job_type_pattern` (dbt Cloud job types include `scheduled`, `merge`, `ci` and `other`).

**Requirements:**

- Jobs must be in the production environment
- With `require_generate_docs: true`, jobs must have "Generate docs on run" enabled

#### Platform Instance

A dbt Cloud recipe ingests one dbt project. If you ingest more than one dbt project into DataHub, from dbt Cloud or dbt Core, set `platform_instance` in each recipe to that project's name (the `name:` in its `dbt_project.yml`). This keeps the dbt URNs of different projects distinct, including models from a package that several projects install, and it is the same value that dbt Core recipes and multi-project glob ingestion use, so the same project gets the same dataset, assertion and semantic model URNs whichever way it is ingested. Query entities from `meta.queries` are the exception: under a glob their URNs also include the project name.

If you only ever ingest one dbt project, `platform_instance` can be left unset. Choose the value before the first ingestion: changing it later changes every dbt URN of the project, and metadata curated in DataHub on the old entities is not carried over.
