# Restoring Search and Graph Indices from Local Database

If search or graph services go down or you have made changes to them that require reindexing, you can restore them from
the aspects stored in the local database.

When a new version of the aspect gets ingested, GMS initiates an MAE event for the aspect which is consumed to update
the search and graph indices. As such, we can fetch the latest version of each aspect in the local database and produce
MAE events corresponding to the aspects to restore the search and graph indices.

## Quickstart

If you're using the quickstart images, you can use the `datahub` cli to restore indices.

```
datahub docker quickstart --restore-indices
```
See [this section](../quickstart.md#restoring-only-the-index-use-with-care) for more information. 

## Docker-compose

If you are on a custom docker-compose deployment, run the following command (you need to checkout [the source repository](https://github.com/datahub-project/datahub)) from the root of the repo to send MAE for each aspect in the Local DB.

```
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices
```

If you need to clear the search and graph indices before restoring, add `-a clean` to the end of the command.

Refer to this [doc](../../docker/datahub-upgrade/README.md#environment-variables) on how to set environment variables
for your environment.

## Kubernetes

Run `kubectl get cronjobs` to see if the restoration job template has been deployed. If you see results like below, you
are good to go.

```
NAME                                          SCHEDULE    SUSPEND   ACTIVE   LAST SCHEDULE   AGE
datahub-datahub-cleanup-job-template          * * * * *   True      0        <none>          2d3h
datahub-datahub-restore-indices-job-template  * * * * *   True      0        <none>          2d3h
```

If not, deploy latest helm charts to use this functionality.

Once restore indices job template has been deployed, run the following command to start a job that restores indices.

```
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

Once the job completes, your indices will have been restored. 

## Through API

You can do a HTTP POST request to `/gms/aspects?action=restoreIndices` endpoint with the `urn` as part of JSON Payload to restore indices for the particular URN.

```
curl --location --request POST 'https://demo.datahubproject.io/api/gms/aspects?action=restoreIndices' \
--header 'Authorization: Bearer TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "urn": "YOUR_URN"
}'
```