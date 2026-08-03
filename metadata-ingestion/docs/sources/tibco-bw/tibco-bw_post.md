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

##### Declared message schemas from application archives (opt-in)

A BusinessWorks application's JMS activities declare the exact message each
process publishes or consumes, but that declaration lives inside the deployed
archive — the runtime APIs never return it. Point `application_archives.paths` at
the `.ear` files and the connector reads the JMS activities out of each process,
resolves their destination names against the archive's module properties, and
emits the declared XSD as the **schema of the TIBCO EMS destination**:

```yaml
application_archives:
  paths:
    - "/mnt/bw-releases/*.ear"
  ems_target:
    platform_instance: null # match your tibco-ems recipe
    env: PROD
    server_group: default
```

The `ems_target` block must address destinations the same way your `tibco-ems`
recipe does, since both connectors are describing the same queues and topics. A
mismatch produces two entities for one destination rather than an error.

What this adds:

- **Schemas** on the EMS destinations a process publishes to, flattened from the
  message XSD into dot-delimited field paths with the original XSD retained as the
  raw schema. Only a publisher's declaration is used — what a consumer reads is
  its own contract, not the destination's.
- **Lineage** from each application to the destinations its own processes read
  and write, merged with (not replacing) anything you declared in
  `application_lineage`. An archive can only see its JMS endpoints, so database
  and file endpoints still have to be configured by hand.
- **Provenance**, via `schema_source: tibco-bw-ear` and the process that declared
  it, so a declared schema is distinguishable from one the TIBCO EMS connector
  estimated from downstream consumers. The EMS connector will not overwrite a
  declared schema with a derived one.

Archives are matched to applications by filename, with any trailing version
stripped: `OrderPublisher_1.2.0.ear` and `OrderPublisher.ear` both map to the
application `OrderPublisher`. Everything here is emitted non-primary, because
the destinations belong to the `tibco-ems` source.

Set `emit_destination_schemas: false` or `emit_destination_lineage: false` to take
one half without the other.

### Limitations

- **Lineage is manual by default.** Because the runtime APIs do not expose an
  application's data flows, dataset-level lineage must be supplied through
  `application_lineage` — or, for JMS endpoints only, read from supplied archives.
  Column-level lineage (`emit_column_lineage`) is a best-effort name match between
  the declared datasets' schemas, not derived from the application's actual
  transforms.
- **Archives must be supplied out of band.** Neither bwagent nor the TCI API
  serves the deployed archive, so `application_archives.paths` has to point at a
  copy the ingestion host can read — typically the release artifact store.
- **JMS endpoints only.** An archive's JDBC, file and HTTP endpoints are not
  parsed; those remain the job of `application_lineage`.
- **No process-level detail.** Individual BusinessWorks processes within an
  application are not enumerated by these APIs, so applications are the finest
  granularity captured.

### Troubleshooting

#### No lineage appears

Lineage is not auto-discovered; populate `application_lineage` with the upstream
and downstream dataset urns for each application. For column-level lineage, also
set `emit_column_lineage` and ensure the referenced datasets already have schemas
in DataHub.

#### An archive was read but no schemas appeared

Check the report counters. `jms_activities_found: 0` means the processes use
endpoint types this parser does not read (only JMS activities are parsed).
`unresolved_destinations` means an activity's destination is a module property
that no `.substvar` in the archive defines — usually because the value is
supplied at deployment time rather than packaged, in which case declare that
destination through `application_lineage` instead. `unresolved_elements` means
the message's XSD was imported from a module that was not packaged in this
archive.

#### The destination has two entries in DataHub

`ems_target` does not match the `tibco-ems` recipe. Both connectors build the
destination urn from platform instance, environment and server group; align the
three and re-run both.
