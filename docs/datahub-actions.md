<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# DataHub Actions

**Event-driven automation for DataHub** — react to metadata changes in real time and trigger workflows, notifications, or custom logic.

## What you can do

- **Listen to metadata change events** as they happen (entity updates, tag additions, ownership changes, etc.)
- **Filter events** to only act on what matters to you
- **Chain transformers** to enrich or reshape events before they reach your action
- **Build custom actions** — call APIs, send Slack alerts, trigger pipelines, sync to external systems
- **Run multiple pipelines** in a single process

## Installation

```bash
pip install acryl-datahub acryl-datahub-actions
datahub actions version
```

## Quickstart

Define an action pipeline in YAML and run it:

```yaml
# my_action.yml
name: tag_watcher

source:
  type: kafka
  config:
    connection:
      bootstrap: localhost:9092
      schema_registry_url: http://localhost:8081

filter:
  event_type: EntityChangeEvent_v1
  event:
    category: TAG
    operation: ADD

action:
  type: hello_world
```

```bash
datahub actions -c my_action.yml
```

## Key concepts

| Concept         | Description                                        |
| --------------- | -------------------------------------------------- |
| **Source**      | Where events come from (currently: Kafka)          |
| **Filter**      | Narrow down which events trigger the action        |
| **Transformer** | Optionally reshape or enrich events                |
| **Action**      | What to do — call an API, send a message, run code |

## Supported events

- `EntityChangeEvent_v1` — fired when an entity's metadata changes (tags, owners, terms, etc.)
- `MetadataChangeLogEvent_v1` — low-level log of every aspect write

## Links

- [Documentation](/)
- [Actions concepts](/docs/actions/concepts)
- [Quickstart guide](/docs/actions/quickstart)
- [Developing a custom action](/docs/actions/guides/developing-an-action)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
