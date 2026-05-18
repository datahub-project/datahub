# BigQuery WIF Live Test Setup

This guide explains how to configure the live Workload Identity Federation integration test
(`tests/integration/bigquery_wif/test_live_wif.py`) and the optional GitHub Actions workflow
(`.github/workflows/bigquery-wif-live-test.yml.template`).

The test is skipped automatically in normal CI and local runs — it only executes when both
`BIGQUERY_WIF_CONFIG_PATH` and `BIGQUERY_WIF_PROJECT_ID` are set.

---

## Prerequisites

- A GCP project with the BigQuery API enabled
- `gcloud` CLI authenticated with an account that has IAM admin rights in the project
- A GitHub repository (for the Actions workflow variant)

---

## 1 — Create the Workload Identity Pool and Provider

```bash
PROJECT_ID=my-project
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
POOL_ID=datahub-wif-test
PROVIDER_ID=github-actions

# Create the pool
gcloud iam workload-identity-pools create $POOL_ID \
  --project=$PROJECT_ID \
  --location=global \
  --display-name="DataHub WIF test pool"

# Create the OIDC provider for GitHub Actions
gcloud iam workload-identity-pools providers create-oidc $PROVIDER_ID \
  --project=$PROJECT_ID \
  --location=global \
  --workload-identity-pool=$POOL_ID \
  --display-name="GitHub Actions" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='YOUR_ORG/YOUR_REPO'"
```

Replace `YOUR_ORG/YOUR_REPO` with your GitHub repository (e.g. `datahub-project/datahub`).

---

## 2 — Create or choose a service account

```bash
SA_NAME=datahub-wif-test-sa
SA_EMAIL=$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com

gcloud iam service-accounts create $SA_NAME \
  --project=$PROJECT_ID \
  --display-name="DataHub WIF test service account"

# Grant BigQuery read access (enough for SELECT 1)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.user"
```

---

## 3 — Allow GitHub Actions to impersonate the service account

```bash
gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL \
  --project=$PROJECT_ID \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/attribute.repository/YOUR_ORG/YOUR_REPO"
```

---

## 4 — Configure GitHub repository variables

In your GitHub repository go to **Settings → Secrets and variables → Actions → Variables** and
add the following **repository variables** (not secrets — these values are not sensitive):

| Variable name            | Value                                                                                                                    |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `GCP_WIF_PROVIDER`       | `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID`                          |
| `GCP_WIF_SERVICE_ACCOUNT`| `SA_EMAIL` (e.g. `datahub-wif-test-sa@my-project.iam.gserviceaccount.com`)                                              |
| `GCP_PROJECT_ID`         | `my-project`                                                                                                             |

---

## 5 — Activate the workflow

Rename the template file to enable the workflow:

```bash
mv .github/workflows/bigquery-wif-live-test.yml.template \
   .github/workflows/bigquery-wif-live-test.yml
```

The workflow runs on `workflow_dispatch` and on pull requests that touch BigQuery or WIF source
files. It is skipped automatically on PRs from forks (repository variables are not exposed to
fork workflows by GitHub policy).

---

## Running the test locally

Export the WIF config file path and project ID, then run pytest directly:

```bash
export BIGQUERY_WIF_CONFIG_PATH=/path/to/wif-credentials.json
export BIGQUERY_WIF_PROJECT_ID=my-project

cd metadata-ingestion
pytest tests/integration/bigquery_wif/test_live_wif.py -v
```

The credentials file is the JSON produced by
`gcloud iam workload-identity-pools create-cred-config` or by
`google-github-actions/auth` (the `credentials_file_path` output).
