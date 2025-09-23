---
title: "Universal transformers"
---

# Transformers

The below table shows transformer which can transform aspects of any entity having them.

| Aspect          | Transformer                           |
| --------------- | ------------------------------------- |
| `browsePathsV2` | - [Set browsePaths](#set-browsepaths) |

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
