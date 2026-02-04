# CODEX Context (DataHub)

This file captures the current working context for a new agent.
Facts are tagged with evidence and verification method when known.
Anything not directly verified is labeled [Unverified].

## Repos & Paths

- **DataHub repo:** `/Users/jchiu/workspace/datahub`.  
  Evidence: workspace root in session context. Verification: environment context.
- **Deploy script repo:** `/Users/jchiu/workspace/product/searchai/data_platform/datahub/deploy-script-capi/prod`.  
  Evidence: file reads in this session. Verification: `read_file`.

## Frontend Dev Server (localhost:3000)

- **Start command (Node 18 required):**  
  `PATH=/opt/homebrew/opt/node@18/bin:$PATH /opt/twitter_mde/package/yarn/5ff867ae23750614b5890201ff757ce6defcd68e07d5ea8f932ca457bfbf76d5/bin/yarn --cwd datahub-web-react start`  
  Evidence: Vite log showed `SyntaxError: Unexpected token '||='` when Node < 18; resolved by adding Node 18 to PATH. Verification: `read_file` of `/tmp/vite_3000.log`, `curl`/`lsof`.
- **Vite proxy target:** `http://localhost:9002` for `/api/v2/graphql` and `/openapi/v1/tracking/track`.  
  Evidence: `datahub-web-react/vite.config.ts`. Verification: `read_file`.

## Backend / Port-Forwards

- **GMS port-forward:** `kubectl -n datapipeline port-forward svc/datahub-datahub-gms 8080:8080`.  
  Evidence: active port-forward in this session. Verification: `curl http://localhost:8080/health` -> `HTTP_200`.
- **Frontend service port-forward:** `kubectl -n datapipeline port-forward svc/datahub 9002:9002`.  
  Evidence: active port-forward in this session. Verification: `curl http://localhost:9002/health` -> `HTTP_200`.
- **Port-forward logs:**  
  `/tmp/portforward_datahub_gms_8080.log`, `/tmp/portforward_datahub_9002_svc.log`.  
  Evidence: commands used to start port-forwards. Verification: `read_file` on the logs.

## Managed Ingestion (Iceberg Profiling)

### Trigger Run (GraphQL)
```
mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
  createIngestionExecutionRequest(input: $input)
}
```
Example variables:
```
{"input":{"ingestionSourceUrn":"urn:li:dataHubIngestionSource:bc8dbcd0-6e5a-48de-a278-5f2996788633"}}
```
Evidence: used in session. Verification: `read_file` of `/tmp/graphql_create_exec_request_out.json`.

### Check Run Status (GraphQL)
```
query executionRequest($urn: String!) {
  executionRequest(urn: $urn) {
    urn
    result { status startTimeMs durationMs }
  }
}
```
Evidence: used in session. Verification: `read_file` of `/tmp/graphql_exec_request_status_out.json`.

## DataHub Actions Image & Resources

- **Actions image tag in values:** `v1.3.0.1.18`.  
  Evidence: `datahub-values-capi.yaml`. Verification: `read_file`.
- **Actions resources (requests/limits):**  
  Requests `cpu: 8`, `memory: 64Gi`; Limits `cpu: 32`, `memory: 256Gi`.  
  Evidence: `datahub-values-capi.yaml` and `kubectl get deployment ... -o jsonpath`. Verification: `read_file`, `run_terminal_cmd`.

### Deploy Values (CAPI script)
From `deploy-datahub-capi.sh`:
```
helm template datahub datahub/datahub \
  --namespace datapipeline \
  --values ./datahub-values-capi.yaml \
  --set-string global.kafka.bootstrap.server="${BOOTSTRAP_SERVERS_ESCAPED}" \
  | kubectl -n datapipeline apply --server-side --force-conflicts -f -
```
Evidence: `deploy-datahub-capi.sh`. Verification: `read_file`.

## Iceberg Profiling Internals (Current)

- **Iceberg profiling uses `pyiceberg` manifest scanning (not Spark).**  
  Evidence: `metadata-ingestion/src/datahub/ingestion/source/iceberg/iceberg_profiler.py` imports `pyiceberg`. Verification: `read_file`.
- **Profiles emitted:** `DatasetProfile` and `LatestPartitionProfiles`.  
  Evidence: `yield dataset_profile` and `yield LatestPartitionProfilesClass(...)` in `iceberg_profiler.py`. Verification: `read_file`.

## Spark Profiling Context

- **S3 profiling uses Spark + Deequ (existing pattern).**  
  Evidence: `metadata-ingestion/src/datahub/ingestion/source/s3/profiling.py` imports `pyspark` and `pydeequ`. Verification: `read_file`.
- **Unity Catalog profiling uses Databricks SQL `ANALYZE TABLE`.**  
  Evidence: `_analyze_table` in `metadata-ingestion/src/datahub/ingestion/source/unity/proxy_profiling.py`. Verification: `read_file`.

## Known Issues & Fixes (Observed)

- **Vite dev server fails with `SyntaxError: Unexpected token '||='` under Node < 18.**  
  Evidence: `/tmp/vite_3000.log`. Verification: `read_file`.
- **Port-forwards die; restarting service port-forwards restores connectivity.**  
  Evidence: successful `curl` after restart; logs in `/tmp/portforward_datahub_*`. Verification: `run_terminal_cmd`.

## How to Rebuild & Push datahub-actions Image (Local)

[Unverified] The following steps were used in this session; verify paths and tooling before reusing.
```
docker buildx build --platform linux/amd64 --push \
  -t docker-releases-local.artifactory.twitter.biz/data-platform/x-datahub-actions:v1.3.0.1.18 \
  --build-arg APP_ENV=slim \
  --build-arg RELEASE_VERSION=1.3.0.1.18 \
  --build-arg BUNDLED_CLI_VERSION=1.3.0.1.18 \
  --build-arg BUNDLED_VENV_PLUGINS=none \
  --build-arg SKIP_VERSION_UPDATE=1 \
  -f docker/datahub-actions/Dockerfile .
```
Evidence: command output in session logs. Verification: `run_terminal_cmd` history.

## Safety / Production Notes

- **Do not delete or modify production resources without explicit approval.**  
  Evidence: user rule set. Verification: system instructions.

