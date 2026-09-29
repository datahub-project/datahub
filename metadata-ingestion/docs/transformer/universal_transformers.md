---
title: "Universal transformers"
---

# Universal Transformers

These transformers apply metadata across multiple entity types in a single configuration. They are the recommended way to add domains, ownership, or modify browse paths when your ingestion pipeline produces entities beyond just datasets.

## Supported Entity Types

The domain and ownership transformers on this page support the following entity types by default:

- Dataset
- Container
- Chart
- Dashboard
- Data Job
- Data Flow

You can restrict the set of entity types via the optional `entity_types` config field.

:::tip "Relationship to Dataset Transformers"

The domain and ownership transformers on this page are drop-in replacements for their `*_dataset_*` counterparts (e.g. `simple_add_domain` replaces `simple_add_dataset_domain`). The `*_dataset_*` variants continue to work but only process dataset entities. Use the names on this page when you need broader entity type coverage.
:::

| Aspect          | Transformer                                                                                            |
| --------------- | ------------------------------------------------------------------------------------------------------ |
| `domains`       | - [Simple Add Domain](#simple-add-domain)<br/> - [Pattern Add Domain](#pattern-add-domain)             |
| `ownership`     | - [Simple Add Ownership](#simple-add-ownership)<br/> - [Pattern Add Ownership](#pattern-add-ownership) |
| `browsePathsV2` | - [Set browsePaths](#set-browsepaths)                                                                  |

## Domain Transformers

### Simple Add Domain

Adds a static list of domains to all supported entity types flowing through the pipeline.

#### Config Details

| Field              | Required | Type                  | Default     | Description                                                      |
| ------------------ | -------- | --------------------- | ----------- | ---------------------------------------------------------------- |
| `domains`          | ✅       | list[union[urn, str]] |             | List of domain URNs or simple domain names.                      |
| `replace_existing` |          | boolean               | `false`     | Whether to remove domains from entity sent by ingestion source.  |
| `semantics`        |          | enum                  | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |
| `on_conflict`      |          | enum                  | `DO_UPDATE` | Whether to make changes if domains already exist on the target.  |
| `entity_types`     |          | list[string]          | all types   | Restrict which entity types this transformer processes.          |

#### Examples

Add a domain to all entities (datasets, dashboards, charts, etc.):

```yaml
transformers:
  - type: "simple_add_domain"
    config:
      domains:
        - urn:li:domain:engineering
```

Add a domain, merging with existing domains on the server:

```yaml
transformers:
  - type: "simple_add_domain"
    config:
      semantics: PATCH
      domains:
        - urn:li:domain:engineering
```

Restrict to only datasets and containers:

```yaml
transformers:
  - type: "simple_add_domain"
    config:
      entity_types:
        - dataset
        - container
      domains:
        - urn:li:domain:engineering
```

### Pattern Add Domain

Adds domains based on regex matching against entity URNs. Works across all supported entity types.

#### Config Details

| Field              | Required | Type                              | Default     | Description                                                              |
| ------------------ | -------- | --------------------------------- | ----------- | ------------------------------------------------------------------------ |
| `domain_pattern`   | ✅       | map[regex, list[union[urn, str]]] |             | Entity URN regex and list of domain URNs to apply to matching entities.  |
| `replace_existing` |          | boolean                           | `false`     | Whether to remove domains from entity sent by ingestion source.          |
| `semantics`        |          | enum                              | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.         |
| `is_container`     |          | bool                              | `false`     | Whether to also propagate domains to parent containers via browse paths. |
| `entity_types`     |          | list[string]                      | all types   | Restrict which entity types this transformer processes.                  |

#### Examples

Add domains to PowerBI dashboards and Snowflake datasets with different domain assignments:

```yaml
transformers:
  - type: "pattern_add_domain"
    config:
      domain_pattern:
        rules:
          ".*powerbi.*": ["urn:li:domain:analytics"]
          ".*snowflake.*": ["urn:li:domain:data-warehouse"]
```

Add domains to matching entities and propagate to their parent containers:

```yaml
transformers:
  - type: "pattern_add_domain"
    config:
      is_container: true
      semantics: PATCH
      domain_pattern:
        rules:
          ".*powerbi.*": ["analytics"]
```

## Ownership Transformers

### Simple Add Ownership

Adds a static list of owners to all supported entity types.

#### Config Details

| Field              | Required | Type         | Default     | Description                                                      |
| ------------------ | -------- | ------------ | ----------- | ---------------------------------------------------------------- |
| `owner_urns`       | ✅       | list[string] |             | List of owner URNs.                                              |
| `ownership_type`   |          | string       | `DATAOWNER` | Ownership type (enum value or ownership type URN).               |
| `replace_existing` |          | boolean      | `false`     | Whether to remove owners from entity sent by ingestion source.   |
| `semantics`        |          | enum         | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |
| `entity_types`     |          | list[string] | all types   | Restrict which entity types this transformer processes.          |

#### Examples

Add owners to all entities in the pipeline:

```yaml
transformers:
  - type: "simple_add_ownership"
    config:
      owner_urns:
        - "urn:li:corpuser:data-team"
        - "urn:li:corpGroup:platform-eng"
      ownership_type: "DATAOWNER"
```

### Pattern Add Ownership

Adds owners based on regex matching against entity URNs. Works across all supported entity types including containers.

#### Config Details

| Field              | Required | Type                  | Default     | Description                                                                |
| ------------------ | -------- | --------------------- | ----------- | -------------------------------------------------------------------------- |
| `owner_pattern`    | ✅       | map[regex, list[urn]] |             | Entity URN regex and list of owner URNs to apply to matching entities.     |
| `ownership_type`   |          | string                | `DATAOWNER` | Ownership type (enum value or ownership type URN).                         |
| `replace_existing` |          | boolean               | `false`     | Whether to remove owners from entity sent by ingestion source.             |
| `semantics`        |          | enum                  | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.           |
| `is_container`     |          | bool                  | `false`     | Whether to also propagate ownership to parent containers via browse paths. |
| `entity_types`     |          | list[string]          | all types   | Restrict which entity types this transformer processes.                    |

#### Examples

Assign different owners to different platforms:

```yaml
transformers:
  - type: "pattern_add_ownership"
    config:
      owner_pattern:
        rules:
          ".*powerbi.*": ["urn:li:corpuser:bi-team"]
          ".*snowflake.*": ["urn:li:corpuser:data-eng"]
      ownership_type: "TECHNICAL_OWNER"
```

Assign owners and propagate to parent containers:

```yaml
transformers:
  - type: "pattern_add_ownership"
    config:
      is_container: true
      semantics: PATCH
      owner_pattern:
        rules:
          ".*powerbi.*": ["urn:li:corpuser:bi-team"]
      ownership_type: "DATAOWNER"
```

## Container Propagation (`is_container`)

Both `pattern_add_domain` and `pattern_add_ownership` support an `is_container` flag. When enabled, the transformer will look up the browse path of each matched entity and apply the same metadata to all parent containers found in that path.

This is useful when you want containers to inherit domains or ownership from their children without configuring each container individually.

:::info
When multiple entities in the same container have different owners or domains, all values are merged additively onto the container. For example, if dataset A has owner `alice` and dataset B has owner `bob`, and both are in the same container, that container gets both `alice` and `bob` as owners.
:::

## Set browsePaths

This transformer operates on `browsePathsV2` aspect. If it is not emitted by the ingestion source, it will be created
by the transformer. By default it will prepend configured path to the original path (so it will add it as a prefix).

### Config Details

| Field              | Required | Type         | Default | Description                                                                                         |
| ------------------ | -------- | ------------ | ------- | --------------------------------------------------------------------------------------------------- |
| `path`             | ✅       | list[string] |         | List of nodes in the new path.                                                                      |
| `replace_existing` |          | boolean      | `false` | Whether to overwrite existing browse path, if set to `false`, the configured path will be prepended |

In the most basic case `path` contains list of static strings, for example, below config:

```yaml
transformers:
  - type: "set_browse_path"
    config:
      path:
        - abc
        - def
```

will be reflected as every entity having path prefixed by `abc` and `def` nodes (`def` will be contained by `abc`).

### Variable substitution

The transformer has a mechanism of variables substitution in the path, where list of variables are build based on
existing `browsePathsV2` aspect of the entity. Every _node_ in the existing path, as long as it contains reference to
another entity (e.g. a `container` or a `dataPlatformInstance`) is stored in the list of variables to use. Since
we can have multiple references to entities of the same type (e.g. `containers`) in the browse path, they are stored
in a list-like object, with original order being respected. Let's consider an example, real-world situation, of a table
ingested from Snowflake source, and having `platform_instance` set to some value. Such table will have `browsePathsV2`
aspect set to contain below references:

```yaml
- urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_platform_instance)
- urn:li:container:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
- urn:li:container:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

where `urn:li:container:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` identifies a `container` reflecting a Snowflake's _database_ and
`urn:li:container:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb` identifies a `container` reflecting a Snowflake's _schema_.
Such, existing, path will be mapped into variables as shown below:

```python
dataPlatformInstance[0] = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_platform_instance)"
container[0] = "urn:li:container:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
container[1] = "urn:li:container:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
```

Those variables can be refered to, from the config, by using `$` character, like below:

```yaml
transformers:
  - type: "set_browse_path"
    config:
      path:
        - $dataPlatformInstance[0]
        - $container[0]
        - $container[1]
```

Additionally, 2 more rules apply to the variables resolution:

- If a variable does not exist (or if the index reached outside of list's length) - it will be ignored and not used in the path, all the other nodes will be used and path will be modified
- `$variable[*]` will expand entire list of variables to multiple _nodes_ in the path (think about it as a "flat map"), for example, the equivalent of above config, would be:
  ```yaml
  transformers:
    - type: "set_browse_path"
      config:
        path:
          - $dataPlatformInstance[0]
          - $container[*]
  ```

### Examples

Add (prefix) a top-level node "datahub" to paths emitted by the source:

```yaml
transformers:
  - type: "set_browse_path"
    config:
      path:
        - datahub
```

Remove data platform instance from the path (if it was set), while retaining containers structure:

```yaml
transformers:
  - type: "set_browse_path"
    config:
      replace_existing: true
      path:
        - $container[*]
```
