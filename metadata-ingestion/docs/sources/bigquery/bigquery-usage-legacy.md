### Prerequisites

The Google Identity must have one of the following OAuth scopes granted to it: 

- https://www.googleapis.com/auth/logging.read
- https://www.googleapis.com/auth/logging.admin
- https://www.googleapis.com/auth/cloud-platform.read-only
- https://www.googleapis.com/auth/cloud-platform

And should be authorized on all projects you'd like to ingest usage stats from. 

### Compatibility

The source was last most recently confirmed compatible with the [December 16, 2021](https://cloud.google.com/bigquery/docs/release-notes#December_16_2021)
release of BigQuery. 