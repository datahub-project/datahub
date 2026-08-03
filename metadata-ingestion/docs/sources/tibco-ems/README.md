## Overview

TIBCO EMS is an enterprise messaging system. Learn more in the [official TIBCO EMS documentation](https://www.tibco.com/products/tibco-enterprise-message-service).

The DataHub integration for TIBCO EMS captures messaging entities (queues and topics) and the lineage between them derived from EMS bridges. It also captures stateful deletion detection.

## Concept Mapping

| Source Concept | DataHub Concept                                                    | Notes              |
| -------------- | ------------------------------------------------------------------ | ------------------ |
| `tibco-ems`    | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |                    |
| EMS Queue      | [Dataset](docs/generated/metamodel/entities/dataset.md)            | _subType_: `Queue` |
| EMS Topic      | [Dataset](docs/generated/metamodel/entities/dataset.md)            | _subType_: `Topic` |
| EMS Bridge     | [Upstream Lineage](docs/generated/metamodel/entities/dataset.md)   | Target ← source    |
