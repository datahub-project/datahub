
# Classification

The classification framework lets sources automatically predict info types for
columns and apply them as glossary terms during ingestion. It is an explicit
opt-in feature and is not enabled by default.

:::warning Built-in classifier removed

The built-in **`datahub`** classifier (`DataHubClassifier`) has been **removed**.
It relied on the unmaintained [`acryl-datahub-classify`](https://pypi.org/project/acryl-datahub-classify/)
library, which pinned `numpy<2` and an outdated spaCy stack and blocked dependency
upgrades across the ingestion framework.

The classification **framework** — the `Classifier` interface, the classifier
registry, and the per-source orchestration — is retained so you can register your
own classifier. There is no longer a classifier registered out of the box, so a
recipe that sets `classification.enabled: true` without registering a replacement
fails fast at startup with guidance.

**If you still need the built-in classifier**, pin the last release that ships it:

```shell
pip install 'acryl-datahub==1.6.0.5'
```

:::

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                     | Required | Type                                    | Description                                                                                                                                                                                                                                                                                                                               | Default                                                    |
| ------------------------- | -------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| enabled                   |          | boolean                                 | Whether classification should be used to auto-detect glossary terms                                                                                                                                                                                                                                                                       | False                                                      |
| sample_size               |          | int                                     | Number of sample values used for classification.                                                                                                                                                                                                                                                                                          | 100                                                        |
| max_workers               |          | int                                     | Number of worker processes to use for classification. Set to 1 to disable.                                                                                                                                                                                                                                                                | Number of CPU cores                                        |
| info_type_to_term         |          | Dict[str,string]                        | Optional mapping to provide glossary term identifier for info type.                                                                                                                                                                                                                                                                       | By default, info type is used as glossary term identifier. |
| classifiers               |          | Array of object                         | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.                                                                                                                                                                        | [{'type': 'datahub', 'config': None}]                      |
| table_pattern             |          | AllowDenyPattern (see below for fields) | Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.\*' | {'allow': ['.*'], 'deny': [], 'ignoreCase': True}          |
| table_pattern.allow       |          | Array of string                         | List of regex patterns to include in ingestion                                                                                                                                                                                                                                                                                            | ['.*']                                                     |
| table_pattern.deny        |          | Array of string                         | List of regex patterns to exclude from ingestion.                                                                                                                                                                                                                                                                                         | []                                                         |
| table_pattern.ignoreCase  |          | boolean                                 | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                                                                               | True                                                       |
| column_pattern            |          | AllowDenyPattern (see below for fields) | Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format.                                                                                                                                   | {'allow': ['.*'], 'deny': [], 'ignoreCase': True}          |
| column_pattern.allow      |          | Array of string                         | List of regex patterns to include in ingestion                                                                                                                                                                                                                                                                                            | ['.*']                                                     |
| column_pattern.deny       |          | Array of string                         | List of regex patterns to exclude from ingestion.                                                                                                                                                                                                                                                                                         | []                                                         |
| column_pattern.ignoreCase |          | boolean                                 | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                                                                               | True                                                       |

## Bring your own classifier

Implement the `Classifier` interface and register it before running ingestion. The
columns handed to `classify` are `ColumnInfo` instances (see
`datahub.ingestion.glossary.classification_types`); return the same list with
`infotype_proposals` populated.

```python
from typing import Any, Dict, List

from datahub.ingestion.glossary.classification_types import ColumnInfo
from datahub.ingestion.glossary.classifier import Classifier
from datahub.ingestion.glossary.classifier_registry import classifier_registry


class MyClassifier(Classifier):
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @classmethod
    def create(cls, config_dict: Dict[str, Any]) -> "MyClassifier":
        return cls(config_dict or {})

    def classify(self, columns: List[ColumnInfo]) -> List[ColumnInfo]:
        # Populate column.infotype_proposals here.
        return columns


classifier_registry.register("my-classifier", MyClassifier)
```

Then reference it from the recipe:

```yml
source:
  type: snowflake
  config:
    # ... source config ...
    classification:
      enabled: true
      classifiers:
        - type: my-classifier
```

## Supported sources

- All SQL sources
