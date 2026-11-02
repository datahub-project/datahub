### Capabilities

#### Processes and deployments

The source plugin queries `ComponentMetadata` for current, non-deleted components of type `process`. For each process it reads the component XML to get the description, then emits:

- A DataFlow keyed by the component ID, with the process name as its display name and the custom properties `componentId`, `version`, `branchName`, `modifiedDate`, `folderFullPath` and `deployments`.
- One DataJob with the ID `process`. Its inputs and outputs are empty in this release.

Active deployments come from `DeployedPackage`, joined to environment names. `environment_pattern` controls which environments count. With `include_undeployed_processes: false` (the default), a process that has no active deployment in an allowed environment is skipped, and stateful ingestion soft-deletes it if it was ingested before.

#### Execution history

With `include_execution_history: true` (the default), the source plugin reads `ExecutionRecord` for the last `execution_history_days` days (7 by default, at most 30) in one query for the whole account. For each ingested process it keeps the `max_executions_per_process` most recent executions (50 by default) and emits each one as a DataProcessInstance run of the process DataJob, with the custom properties `status`, `atomName`, `executionType`, `parentExecutionId`, `topLevelExecutionId`, `inboundDocumentCount`, `outboundDocumentCount` and `inboundErrorDocumentCount`. Manual executions (`exec_manual`) are typed as ad hoc runs and all others as scheduled runs. A run started by a Process Call shape (`sub_process`) takes the type of its top-level run and is linked to the run that called it as its parent instance. Boomi leaves `parentExecutionId` empty on these runs in practice, so the top-level run stands in for the caller. The link is only made when the calling run is ingested in the same run of the source plugin.

Each run gets a start event at `executionTime`. Finished runs also get an end event at `executionTime` plus `executionDuration`, or at `executionTime` when Boomi reports no duration:

| Boomi status                                             | Run result       |
| -------------------------------------------------------- | ---------------- |
| `COMPLETE`, `COMPLETE_WARN`                              | Success          |
| `ERROR`, `ABORTED`                                       | Failure          |
| `DISCARDED`                                              | Skipped          |
| `INPROCESS`, `STARTED`, `ENQUEUED`, `ABORTED_RECOVERING` | Start event only |

Any other status gets a start event only and is counted in `executions_unknown_status`. Executions of processes that were not ingested (filtered or undeployed) are skipped and counted in `executions_process_not_emitted`. `environment_pattern` does not filter executions directly, because an execution record names the runtime (atom) it ran on, not the environment. Runs follow the ingested processes, so a process skipped because it is only deployed to denied environments gets no runs.

Boomi purges execution history after the runtime's purge setting, 30 days by default, so runs older than that cannot be ingested. Schedule ingestion more often than the purge interval to keep a continuous history. The window query pages through every execution in the account, so busy accounts with a long window need more API calls.

#### Folders

Every folder that matches `folder_pattern` is emitted as a container, together with its parent folders so the browse path stays complete. `folder_pattern` is matched against the full folder path, for example `Acme/Production/Orders`. Processes in folders that do not match are skipped. If a process's folder is not in the folder listing, the path is read from the process component instead. When no path is available, the process is kept if `folder_pattern` only has deny entries and skipped if it has an allow list.

#### Branches

If Branch and Merge is enabled on the account, set `branch_name` (usually `main`). A branch copy of a component keeps the original component ID, so the source plugin keeps only the most recently modified copy of each component.

#### Rate limiting

Boomi allows 10 Platform API requests per second per account and answers with HTTP 503 when the limit is exceeded. The source plugin caps itself at `max_requests_per_second` (8 by default) and retries 429, 500, 502, 503 and 504 responses with exponential backoff. Lower the cap if other API clients share the account.

The source plugin makes one request per process to read its component XML, so an account with 1,000 processes needs a little over two minutes at the default rate.

### Limitations

- Lineage between processes and the systems they read and write is not extracted yet.
- Execution history is limited to what Boomi still retains, 30 days by default.
- A process is one DataFlow across all environments. The version deployed to each environment is only shown in the `deployments` property.
- Owners are only emitted as user URNs. If your DataHub users are not keyed by email, the owners do not link to existing users.

### Troubleshooting

#### HTTP 401 on every request

Check that `username` is the plain user name (the source plugin adds the `BOOMI_TOKEN.` prefix), that the token has not been revoked, and that `account_id` matches the account the token belongs to.

#### Warning "Cannot read Boomi deployments"

The token's role lacks the privilege to query `DeployedPackage`. The run continues, ingests every process and leaves out the `deployments` property. Grant **Packaged Component Deployment** to restore deployment filtering.

#### Warning "Cannot read Boomi execution history"

The token's role lacks **View Results**, so `ExecutionRecord` returns HTTP 403. The run continues without run history. Grant **View Results**, or set `include_execution_history: false` to skip the query.

If the account has no runtimes (Atoms) the token can see, Boomi rejects the execution query with HTTP 400. The source plugin treats that as no execution history, sets `executions_no_runtimes` in the report and does not warn.

#### Fewer processes than expected

Check the `processes_filtered` and `processes_undeployed_skipped` lists in the ingestion report. Undeployed processes are skipped unless `include_undeployed_processes` is `true`, and folder restrictions on the role hide processes entirely.
