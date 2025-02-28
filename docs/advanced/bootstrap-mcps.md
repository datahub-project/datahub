# Bootstrap MetadataChangeProposals (MCPs)

Bootstrap MCPs are templated MCPs which are loaded when the `system-update` job runs. This allows adding
entities and aspects to DataHub at install time with the ability to customize them via environment variable
overrides.

The built-in bootstrap MCP process can also be extended with custom MCPs. This can streamline deployment
scenarios where a set of standard ingestion recipes, data platforms, users groups, or other configuration
can be applied without the need for developing custom scripts.

## Process Overview

When DataHub is installed or upgraded, a job runs called `system-update`, this job is responsible for data
migration (particularly Elasticsearch indices) and ensuring the data is prepared for the next version of
DataHub. This is the job which will also apply the bootstrap MCPs.

The `system-update` job, depending on configuration, can be split into two sequences of steps. If they are
not split, then all steps are blocking.

1. An initial blocking sequence which is run prior to the new version of GMS and other components
2. Second sequence of steps where GMS and other components are allowed to run while additional data migration steps are
continued in the background

When applying bootstrap MCPs `system-update` will perform the following steps:

1. The `bootstrap_mcps.yaml` file is read, either from a default classpath location, `bootstrap_mcps.yaml`, or a filesystem location
   provided by an environment variable, `SYSTEM_UPDATE_BOOTSTRAP_MCP_CONFIG`.
2. Depending on the mode of blocking or non-blocking each entry in the configuration file will be executed in sequence.
3. The template MCP file is loaded either from the classpath, or a filesystem location, and the template values are applied.
4. The rendered template MCPs are executed with the options specified in the `bootstrap_mcps.yaml`.

## `bootstrap_mcps.yaml` Configuration

The `bootstrap_mcps.yaml` file has the following format.

```yaml
bootstrap:
  templates:
    - name: <name>
      version: <version>
      force: false
      blocking: false
      async: true
      optional: false
      mcps_location: <classpath or file location>
      values_env: <environment variable>
```

Each entry in the list of templates points to a single yaml file which can contain one or more MCP objects. The
execution of the template MCPs is tracked by name and version to prevent re-execution. The MCP objects are executed once
unless `force=true` for each `name`/`version` combination.

See the following table of options for descriptions of each field in the template configuration.

| Field         | Default  | Required  | Description                                                                                                |
|---------------|----------|-----------|------------------------------------------------------------------------------------------------------------|
| name          |          | `true`    | The name for the collection of template MCPs.                                                              |
| version       |          | `true`    | A string version for the collection of template MCPs.                                                      |
| force         | `false`  | `false`   | Ignores the previous run history, will not skip execution if run previously.                               |
| blocking      | `false`  | `false`   | Run before GMS and other components during upgrade/install if running in split blocking/non-blocking mode. |
| async         | `true`   | `false`   | Controls whether the MCPs are executed for sync or async ingestion.                                        |
| optional      | `false`  | `false`   | Whether to ignore a failure or fail the entire `system-update` job.                                        |
| mcps_location |          | `true`    | The location of the file which contains the template MCPs                                                  |
| values_env    |          | `false`   | The environment variable which contains override template values.                                          |

## Template MCPs

Template MCPs are stored in a yaml file which uses the mustache templating library to populate values from an optional environment
variable. Defaults can be provided inline making override only necessary when providing install/upgrade time configuration.

In general the file contains a list of MCPs which follow the schema definition for MCPs exactly. Any valid field for an MCP
is accepted, including optional fields such as `headers`.


### Example: Native Group

An example template MCP collection, configuration, and values environment variable is shown below which would create a native group.

```yaml
- entityUrn: urn:li:corpGroup:{{group.id}}
  entityType: corpGroup
  aspectName: corpGroupInfo
  changeType: UPSERT
  aspect:
   description: {{group.description}}{{^group.description}}Default description{{/group.description}}
   displayName: {{group.displayName}}
   created: {{&auditStamp}}
   members: [] # required as part of the aspect's schema definition
   groups: [] # required as part of the aspect's schema definition
   admins: [] # required as part of the aspect's schema definition
- entityUrn: urn:li:corpGroup:{{group.id}}
  entityType: corpGroup
  aspectName: origin
  changeType: UPSERT
  aspect:
     type: NATIVE
```

Creating an entry in the `bootstrap_mcps.yaml` to populate the values from the environment variable `DATAHUB_TEST_GROUP_VALUES`

```yaml
    - name: test-group
      version: v1
      mcps_location: "bootstrap_mcps/test-group.yaml"
      values_env: "DATAHUB_TEST_GROUP_VALUES"
```

An example json values are loaded from environment variable in `DATAHUB_TEST_GROUP_VALUES` might look like the following.

```json
{"group":{"id":"mygroup", "displayName":"My Group", "description":"Description of the group"}}
```

Using standard mustache template semantics the values in the environment would be inserted into the yaml structure
and ingested when the `system-update` runs.

#### Default values

In the example above, the group's `description` if not provided would default to `Default description` if not specified
in the values contain in the environment variable override following the standard mustache template semantics.

#### AuditStamp

A special template reference, `{{&auditStamp}}` can be used to inject an `auditStamp` into the aspect. This can be used to
populate required fields of type `auditStamp` calculated from when the MCP is applied. This will insert an inline json representation
of the `auditStamp` into the location and avoid escaping html characters per standard mustache template indicated by the `&` character.

### Ingestion Template MCPs

Ingestion template MCPs are slightly more complicated since the ingestion `recipe` is stored as a json string within the aspect.
For ingestion recipes, special handling was added so that they can be described naturally in yaml instead of the normally encoded json string.

This means that in the example below, the structure beneath the `aspect.config.recipe` path will be automatically converted
to the required json structure and stored as a string.

```yaml
- entityType: dataHubIngestionSource
  entityUrn: urn:li:dataHubIngestionSource:demo-data
  aspectName: dataHubIngestionSourceInfo
  changeType: UPSERT
  aspect:
    type: 'demo-data'
    name: 'demo-data'
    config:
      recipe:
        source:
          type: 'datahub-gc'
          config: {}
      executorId: default
```

## `bootstrap_mcps.yaml` Override

Additionally, the `bootstrap_mcps.yaml` can be overridden.
This might be useful for applying changes to the version when using helm defined template values.

```yaml
bootstrap:
  templates:
    - name: myMCPTemplate
      version: v1
      mcps_location: <classpath or file location>
      values_env: <value environment variable>
      revision_env: REVISION_ENV
```

In the above example, we've added a `revision_env` which allows overriding the MCP bootstrap definition itself (excluding `revision_env`).

In this example we could configure `REVISION_ENV` to contain a timestamp or hash: `{"version":"2024060600"}` 
This value can be changed/incremented each time the helm supplied template values change. This ensures the MCP is updated
with the latest values during deployment.


## Known Limitations

* Supported change types: 
  * UPSERT
  * CREATE
  * CREATE_ENTITY
