### Pre-Requisites

#### Set up the right permissions
You need to provide the following permissions for ingestion to work correctly. 
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
![Looker DataHub Permission Set](./looker_datahub_permission_set.png)

#### Get an API key

You need to get an API key for the account with the above privileges to perform ingestion. See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.


### Ingestion through UI

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


