# Prefect Integration

DataHub supports integration of

- Prefect flow and task metadata
- Flow run and Task run information as well as
- Lineage information when present

## What is Prefect Datahub Block?

Blocks are primitive within Prefect that enable the storage of configuration and provide an interface for interacting with external systems. We integrated [prefect-datahub](https://prefecthq.github.io/prefect-datahub/) block which use [Datahub Rest](../../metadata-ingestion/sink_docs/datahub.md#datahub-rest) emitter to emit metadata events while running prefect flow.

## Prerequisites to use Prefect Datahub Block

1. You need to use either Prefect Cloud (recommended) or the self hosted Prefect server.
2. Refer [Cloud Quickstart](https://docs.prefect.io/2.10.13/cloud/cloud-quickstart/) to setup Prefect Cloud.
3. Refer [Host Prefect server](https://docs.prefect.io/2.10.13/host/) to setup self hosted Prefect server.
4. Make sure the Prefect api url is set correctly. You can check it by running below command:
```shell
prefect profile inspect
```
5. If you are using Prefect Cloud, the API URL should be set as `https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>`.
6. If you are using a self-hosted Prefect server, the API URL should be set as `http://<host>:<port>/api`.

## Setup

For setup detail please refer [prefct-datahub](https://prefecthq.github.io/prefect-datahub/).

## How to validate saved block and emit of metadata

1. Go and check in Prefect UI at Blocks menu if you can see the datahub emitter.
2. Run a Prefect workflow. In the flow logs, you should see Datahub related log messages like:

```
Emitting flow to datahub...
Emitting tasks to datahub...
```
## Debugging

### Incorrect Prefect API URL

If your Prefect API URL aren't being generated correctly or set incorrectly, then in that case you can set the Prefect API URL manually as show below:

```shell
prefect config set PREFECT_API_URL='http://127.0.0.1:4200/api'
```

### Connection error for Datahub Rest URL
If you get ConnectionError: HTTPConnectionPool(host='localhost', port=8080), then in that case your GMS service is not up.