### Naming and Grouping

Data Flow ids are stable across runs:

- On-prem: `<domain>/<appspace>` (for example `MyDomain/MyAppSpace`).
- Cloud: the subscription id.

Each deployed application becomes a Data Job named after the application, nested
under its scope's Data Flow. Application version, run state, and application
type are attached as custom properties, and (on-prem) appnode names and states
are attached to the appspace.

### Known Limitations

- **No dataset-level lineage.** The bwagent and TIBCO Cloud APIs expose the
  deployment topology (domains, appspaces, subscriptions, applications and their
  run state) but not the internal process definitions or the source/target
  systems each application connects to. As a result, the connector emits no
  lineage between the applications and the datasets they read or write.
- **No process-level detail.** Individual BusinessWorks processes within an
  application are not enumerated by these APIs, so applications are the finest
  granularity captured.
