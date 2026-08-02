### Capabilities

#### Naming and Grouping

Data Flow ids are stable across runs:

- On-prem: `<domain>/<appspace>` (for example `MyDomain/MyAppSpace`).
- Cloud: the subscription id.

Each deployed application becomes a Data Job named after the application, nested
under its scope's Data Flow. Application version, run state, and application
type are attached as custom properties, and (on-prem) appnode names and states
are attached to the appspace.

Data Flows and Data Jobs cannot belong to a container, so the deployment
hierarchy is spelled out in each entity's browse path instead: on-prem an
appspace browses under its domain and an application under
`<domain>/<appspace>`; in the cloud a subscription is already the top level, so
an application browses directly under its subscription.

#### Lineage

The bwagent and TIBCO Cloud APIs expose deployment topology (domains, appspaces,
subscriptions, applications and their run state) but **not** the datasets each
application reads or writes. Lineage therefore cannot be discovered automatically
and is instead declared by the operator via `application_lineage`, which maps an
application to the dataset urns it consumes (`upstreams`) and produces
(`downstreams`).

Application names are only unique within their deployment scope, so key each
entry by `<scope>/<application>` — where scope is `<domain>/<appspace>` on-prem
or the subscription id in the cloud:

```yaml
application_lineage:
  MyDomain/MyAppSpace/order-sync:
    upstreams:
      - "urn:li:dataset:(urn:li:dataPlatform:kafka,orders_in,PROD)"
    downstreams:
      - "urn:li:dataset:(urn:li:dataPlatform:hana,sales.orders,PROD)"
```

A bare application name is also accepted and applies to every application with
that name, which is only safe when the name is unique across the estate. When a
bare key does match more than one scope the connector warns, because the same
lineage is then copied onto each of them.

The referenced datasets are linked as the application's inputs/outputs without
being materialized, so lineage is added to datasets that other connectors own.
Malformed urns are rejected at config validation time.

##### Column-level lineage (opt-in)

The runtime APIs do not describe an application's field-level transforms, but many
BusinessWorks/TCI applications pass fields through largely unchanged. When
`emit_column_lineage` is enabled, the connector reads the schemas of the declared
upstream and downstream datasets from DataHub (contributed by their own connectors)
and emits a field-level edge for each field name they share. Matching is
**case-insensitive** — the two platforms often case fields differently (e.g.
`OrderId` vs `orderid`) — while the emitted `schemaField` URNs preserve each side's
real field path. This is a best-effort name-match heuristic: datasets without a
schema in DataHub produce no column lineage, and fields that are renamed or derived
by the application are not captured.

### Limitations

- **Lineage is manual.** Because the runtime APIs do not expose an application's
  data flows, dataset-level lineage must be supplied through `application_lineage`
  rather than discovered. Column-level lineage (`emit_column_lineage`) is a
  best-effort name match between the declared datasets' schemas, not derived from
  the application's actual transforms.
- **No process-level detail.** Individual BusinessWorks processes within an
  application are not enumerated by these APIs, so applications are the finest
  granularity captured.

### Troubleshooting

#### No lineage appears

Lineage is not auto-discovered; populate `application_lineage` with the upstream
and downstream dataset urns for each application. For column-level lineage, also
set `emit_column_lineage` and ensure the referenced datasets already have schemas
in DataHub.
