# Glossary Term Propagation Action

The Glossary Term Propagation Action allows you to propagate glossary terms from your assets into downstream entities.

## Use Cases

Enable classification of datasets or field of datasets to propagate metadata to downstream datasets with minimum manual work.

## Functionality

Propagation can be controlled via a specified list of terms or a specified list of term groups.

### Target Terms

- Given a list of "target terms", the propagation action will detect application of the target term to any field or dataset and propagate it down (as a dataset-level term) on all downstream datasets. For example, given a target term of `Classification.Confidential` (the default), if you apply `Classification.Confidential` term to a dataset (at the dataset level or a field-level), this action will find all the downstream datasets and apply the `Classification.Confidential` term to them at the dataset level. Note that downstream application is only at the dataset level, regardless of whether the primary application was at the field level or the dataset level.
- This action also supports term linkage. If you apply a term that is linked to the target term via inheritance, then this action will detect that application and propagate it downstream as well. For example, if the term `PersonalInformation.Email` inherits `Classification.Confidential` (the target term), and if you apply the `PersonalInformation.Email` term to a dataset (or a field in the dataset), it will be picked up by the action, and the `PersonalInformation.Email` term will be applied at the dataset level to all the downstream entities.

### Term Groups

- Given a list of "term groups", the propagation action will only propagate terms that belong to these term groups.

### Additions and Removals

The action propagates both term additions and removals to downstream datasets.

## Configurability

You can control what the target term should be. Linkage to the target term is controlled through your business glossary which is completely under your control.

### Example Config

```yaml
name: "term_propagation"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
action:
  type: "term_propagation"
  config:
    target_terms:
      - Classification
    term_groups:
      - "Personal Information"

datahub:
  server: "http://localhost:8080"
```

## Caveats

- Term Propagation is currently only supported for downstream datasets. Terms will not propagate to downstream dashboards or charts. Let us know if this is an important feature for you.
- A term removal is only propagated once the source dataset no longer carries the term on any field or at the dataset level, so removing it from one field while another field still has it leaves downstream datasets untouched.
- Propagation does not track provenance on downstream datasets: when a removal is propagated it clears the term from every downstream dataset, and an addition adds it, regardless of whether the term was also applied to that dataset directly. A propagated removal can therefore still clear a term that a user set on a downstream dataset independently.
