---
description: "The column-level classification feature has been removed from acryl-datahub."
---

# Classification (Removed)

The column-level classification feature — which auto-predicted info types for
columns and applied them as glossary terms during ingestion — has been **removed**
from `acryl-datahub`.

It relied on the unmaintained [`acryl-datahub-classify`](https://pypi.org/project/acryl-datahub-classify/)
library, which pinned `numpy<2` and an outdated spaCy stack and blocked the rest of
the ingestion dependencies from moving forward.

## What this means

- `classification.enabled: true` is no longer supported. A source configured with
  classification enabled fails fast at startup with guidance pointing here.
- Recipes that leave classification disabled (the default) are unaffected.

## If you still need classification

Pin the last release that supports it:

```shell
pip install 'acryl-datahub==1.6.0.5'
```
