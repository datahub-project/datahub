### Setup

This source pulls dbt metadata directly from the dbt Cloud APIs.

Create a [service account token](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens) with the "Metadata Only" permission.
This is a read-only permission.

You'll need to have a dbt Cloud job set up to run your dbt project, and "Generate docs on run" should be enabled.

To get the required IDs, go to the job details page (this is the one with the "Run History" table), and look at the URL.
It should look something like this: https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094.
In this example, the account ID is 107298, the project ID is 175705, and the job ID is 148094.
