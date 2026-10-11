### Overview

The `looker` module ingests metadata from Looker into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Ingestion through UI

The following video shows you how to get started with ingesting Looker metadata through the UI.

:::note

You will need to run `lookml` ingestion through the CLI after you have ingested Looker metadata through the UI. Otherwise you will not be able to see Looker Views and their lineage to your warehouse tables.

:::

<div
  style={{
    position: "relative",
    paddingBottom: "57.692307692307686%",
    height: 0
  }}
>
  <iframe
    src="https://www.loom.com/embed/b8b9654e02714d20a44122cc1bffc1bb"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

### Prerequisites

#### Set up the right permissions

Grant the following permissions:

```
access_data
explore
manage_models
see_datagroups
see_lookml
see_lookml_dashboards
see_looks
see_pdts
see_queries
see_schedules
see_sql
see_system_activity
see_user_dashboards
see_users
```

Here is an example permission set after configuration.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/looker_datahub_permission_set.png"/>
</p>

#### Get an API key

Generate an API key (client ID and secret) for the account with the above permissions. See [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk).
