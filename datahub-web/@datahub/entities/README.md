@datahub/entities
==============================================================================

This package holds entity specific components and modules.

## Creating an entity module

Modules for entities should be namespaced according to the entity in the format:

```
<module-type>/<pluralized-entity-name>/<entity-module-path>
```

where `module-type` is the name of the module we are working with (for example, `component`
or `util`), `pluralized-entity-name` is the identifier for the entity in plural form (for example,
`datasets` or `metrics`) and `entity-module-path` is the specific organization we want for the
module.

With the above criteria, when we create an upstream datasets component for datasets with respect to
relationships, the command we might run might be:

```
// Creates the module components/datasets/relationships/upstream-datasets.ts
ember g component datasets/relationships/upstream-datasets
```
