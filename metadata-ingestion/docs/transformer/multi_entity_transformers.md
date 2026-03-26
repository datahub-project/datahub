---
title: "Multi-Entity Transformers"
---

# Multi-Entity Transformers

These transformers apply metadata (domains, ownership) across multiple entity types in a single configuration. They are the recommended way to add domains or ownership when your ingestion pipeline produces entities beyond just datasets.

## Supported Entity Types

All transformers on this page support the following entity types:

- Dataset
- Container
- Chart
- Dashboard
- Data Job
- Data Flow

> **Relationship to Dataset Transformers:** The transformers on this page are drop-in replacements for their `*_dataset_*` counterparts (e.g. `simple_add_domain` replaces `simple_add_dataset_domain`). The `*_dataset_*` variants continue to work but only process dataset entities. Use the names on this page when you need broader entity type coverage.

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

### Pattern Add Domain

Adds domains based on regex matching against entity URNs. Works across all supported entity types.

#### Config Details

| Field              | Required | Type                              | Default     | Description                                                              |
| ------------------ | -------- | --------------------------------- | ----------- | ------------------------------------------------------------------------ |
| `domain_pattern`   | ✅       | map[regex, list[union[urn, str]]] |             | Entity URN regex and list of domain URNs to apply to matching entities.  |
| `replace_existing` |          | boolean                           | `false`     | Whether to remove domains from entity sent by ingestion source.          |
| `semantics`        |          | enum                              | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.         |
| `is_container`     |          | bool                              | `false`     | Whether to also propagate domains to parent containers via browse paths. |

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

⚠️ **Note:** When multiple entities in the same container have different owners or domains, all values are merged additively onto the container. For example, if dataset A has owner `alice` and dataset B has owner `bob`, and both are in the same container, that container gets both `alice` and `bob` as owners.
