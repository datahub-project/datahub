# Custom Environment

A custom environment (entity type `customFabricType`) is an environment name that your organization registers in addition to the built-in ones. Assets that carry an environment, such as datasets, ML models, containers and pipelines, can then use it the same way they use `PROD` or `DEV`.

The built-in environments are `DEV`, `TEST`, `QA`, `UAT`, `EI`, `PRE`, `STG`, `NON_PROD`, `PROD`, `CORP`, `RVW`, `PRD`, `TST`, `SIT`, `SBX`, `SANDBOX` and `CERT`. Register a custom environment when none of these fits, for example `PERF` for a performance-test environment or `EU_PROD` for a regional production deployment.

Custom environments are turned off by default. See [Turning custom environments on](#turning-custom-environments-on).

## Identity

A custom environment is identified by its id, which is also the value assets carry as their environment:

```
urn:li:customFabricType:<id>
```

For example, `urn:li:customFabricType:PERF`. A dataset in that environment has the URN `urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PERF)`.

The id must:

- start with an uppercase letter and contain only uppercase letters, digits and underscores;
- be at most 64 characters long;
- not be a built-in environment name, in any letter case.

The id can't be changed after the environment is created.

## Important Capabilities

### Properties

The `customFabricTypeInfo` aspect holds:

- **Display Name**: the name shown in the UI, for example "Performance". The id is shown when it is not set.
- **Description**: what the environment is used for.

Custom environments also support `ownership`, `status` and `deprecation`.

### Where an environment can be used

Every place that accepts a built-in environment also accepts a registered custom environment:

| Entity                   | Where the environment is stored |
| ------------------------ | ------------------------------- |
| Dataset                  | URN (`origin` in the key)       |
| ML Model, ML Model Group | URN (`origin` in the key)       |
| ML Model Deployment      | URN (`origin` in the key)       |
| Container                | `env` in `containerProperties`  |
| Data Flow                | `env` in `dataFlowInfo`         |
| Data Job                 | `env` in `dataJobInfo`          |
| Data Platform Instance   | `env` in `icebergWarehouseInfo` |
| Data Process (legacy)    | URN (`origin` in the key)       |

Ingestion recipes take a custom environment in the same `env` setting, for example `env: PERF`. The value is upper-cased, as built-in values always have been.

### Which writes are accepted

DataHub checks the environment of every write that introduces it: creating an asset whose URN names the environment, setting `env` in one of the aspects above to a new value, or referring to a dataset in a new environment from lineage or other aspects. The write is rejected when the environment is:

- not built-in and not registered;
- a custom environment that has been deleted or soft-deleted;
- a deprecated custom environment;
- any custom environment while custom environments are turned off.

A write that keeps an asset's current environment is always accepted. Assets already in a deprecated environment, or created before custom environments were turned off, can still be read, updated and re-ingested.

### Deprecation and deletion

Deprecate an environment to stop new assets from using it while keeping the existing ones. Deprecation uses the standard `deprecation` aspect; its note, decommission time and replacement are informational only, and DataHub does not move assets to the replacement. Un-deprecating the environment allows new uses again.

A custom environment can't be deleted, hard or soft, while any asset still uses it. The error names up to five of those assets. Move or delete them first, or deprecate the environment instead.

DataHub finds the assets through the search index, in a single search over every environment field that is searchable. Two limits follow. An asset written moments before the delete may not be in the index yet, so it can be missed. And an environment stored in a field that isn't searchable, such as `env` in `icebergWarehouseInfo`, isn't checked. GMS logs a warning at startup for each such field.

### Managing custom environments

In the UI, go to **Settings > Environments** to list, create, edit, deprecate and delete custom environments. The page is shown when custom environments are turned on and you have the **Manage Environments** privilege.

The GraphQL API has the `customFabricType` and `listCustomFabricTypes` queries and the `createCustomFabricType`, `updateCustomFabricType` and `deleteCustomFabricType` mutations. Deprecation uses the existing `updateDeprecation` mutation. The environment of an asset is returned as a string in the `env` field of datasets, ML models and the other entities above. The existing `origin` field returns `OTHER` for a custom environment.

### Authorization

- The **Manage Environments** platform privilege (`MANAGE_CUSTOM_FABRIC_TYPES`) allows creating, editing, deprecating and deleting custom environments. The default Admin and Editor policies grant it.
- Using an environment needs no extra privilege: anyone who may write an asset may put it in any accepted environment.
- Custom environments are readable by everyone, so environment names can be shown wherever assets appear.
- Metadata policies can be limited to assets in specific environments with the **Environments** resource filter, built-in or custom. See [Policies](../../../authorization/policies.md).

## Turning custom environments on

Custom environments are controlled by the `CUSTOM_FABRIC_TYPES_ENABLED` setting on GMS, which is `false` by default. Turn it on only when:

- every GMS and consumer runs a release that supports custom environments; and
- you won't roll back to a release without them.

Once assets use a custom environment, an earlier release can't read or update them. Turning the setting off again stops new uses but leaves existing custom environments and the assets in them as they are. See the custom environments entry in [Breaking Changes by Version](../../../how/breaking-changes.md).

## Notable Exceptions

- Built-in environments can't be registered, deprecated or deleted.
- The Java `FabricType` enum and the Python `FabricTypeClass` list only the built-in environments. Code that reads an environment through them sees `$UNKNOWN` (Java) for a custom value; read the value as a string instead.
