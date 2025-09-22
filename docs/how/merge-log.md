# Merge Log

This file tracks the progress of upstream merges into the SaaS branch using the AI-powered merge system.

## Overview

The AI merge system automatically processes upstream commits with:

- **Automated conflict resolution** using Claude AI
- **Build validation** after each commit
- **Rollback on failures** with retry logic
- **Progress tracking** with detailed status updates
- **Multiple merge strategies** (stacked, cherry-pick)

## Merge Sessions

Each merge session below represents a complete upstream merge operation with detailed progress tracking.

---

## Upstream Merge Session - 2025-09-21 19:10:05

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details

- **Source**: `upstream/master`
- **Target**: `oss-merge-09-19-2025`
- **Merge Base**: `6a3f31b0930e370e313e708a95a3ec56b410e202`
- **End Commit**: `6ec6f01`
- **Total Commits**: 5
- **Strategy**: stacked
- **Started**: Sun Sep 21 19:10:05 UTC 2025

### Commits Included in This Merge

- **aee04b5680** - feat(build): improve failure logging (#14696)
  _by david-leifker on 2025-09-08_

- **5ce93f54b4** - feat(ingest): add time taken by compute stats overall (#14713)
  _by Aseem Bansal on 2025-09-09_

- **3d464941d0** - fix(ingest/tableau): retry when getting 'unexpected error occurred' (#14672)
  _by Michael Maltese on 2025-09-09_

- **4ea758da19** - chore(ingest/sqlparser): Bump sqlglot to 27.12.0 (#14673)
  _by Tamas Nemeth on 2025-09-09_

- **6ec6f0150d** - refactor(metrics): Make MetricUtils.registry non-nullable
  _by Abe on 2025-09-09_

## Upstream Merge Session - 2025-09-22 03:45:16

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details

- **Source**: `upstream/master`
- **Target**: `oss-merge-09-20-2025-stack1`
- **Merge Base**: `6ec6f0150d3eb8dae9f80ad199c400eb0cf4b855`
- **End Commit**: `a82d4e0`
- **Total Commits**: 3
- **Strategy**: stacked
- **Started**: Mon Sep 22 03:45:16 UTC 2025

### Commits Included in This Merge

- **c731e725ff** - docs(metrics): Add a best practices guide for micrometer metrics library (#14711)
  _by Abe on 2025-09-09_

- **c7ad3f45ea** - feat(ui): Add option to remove asset from an Application (#14679)
  _by Saketh Varma on 2025-09-09_

- **a82d4e0647** - fix(ingest/athena): Fix Athena partition extraction and CONCAT function type issues (#14712)
  _by Tamas Nemeth on 2025-09-10_
