#### Configuration Notes

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret. 
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

