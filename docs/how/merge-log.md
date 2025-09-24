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

## Upstream Merge Session - 2025-09-23 15:36:41

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details

- **Source**: `upstream/master`
- **Target**: `oss-merge-backfill-09-11-2025`
- **Merge Base**: `d67681b08f0035e115c2867e4d10e7d58d04ebec`
- **End Commit**: `dede42d9336fe07f396a8086ddc1d5b0bd780faf`
- **Total Commits**: 15
- **Strategy**: stacked
- **Started**: Tue Sep 23 15:36:41 UTC 2025

### Commits Included in This Merge

- **3fbef4a632** - chore(setup): Limit mixpanel dependency <=4.10.1 (#14725)
  _by skrydal on 2025-09-10_

- **5f23652fd3** - fix(ingestion/iceberg): Improve iceberg source resiliency to server errors (#14731)
  _by skrydal on 2025-09-11_

- **137ffb7d48** - fix(ingest): only add to samples where platform match (#14722)
  _by Aseem Bansal on 2025-09-11_

- **d0519ddce3** - fix(docs): fixing spelling mistakes (#14730)
  _by Jonny Dixon on 2025-09-11_

- **01932d3f87** - fix(ingest/pipeline): Fix for slow ingestion and incomplete ingestion report metrics (#14735)
  _by Tamas Nemeth on 2025-09-11_

- **4244620e7a** - feat(cassandra): Add optional SSL configuration (#14726)
  _by Brock Griffey on 2025-09-11_

- **14130701b5** - fix(ingest): use sequence for sdk input types (#14695)
  _by Harshal Sheth on 2025-09-11_

- **3af386b626** - chore(ingestion/iceberg): Safe-guard pyiceberg version before pydantic 1->2 transition (#14736)
  _by skrydal on 2025-09-11_

- **ac80e8171b** - fix(kafka-setup): remove default to enable topicDefaults to be used (#14738)
  _by Chakru on 2025-09-11_

- **4ce1ae8dd1** - feat(docs): revise tuning instructions for smart assertions (#14740)
  _by Jay on 2025-09-11_

- **57250477bf** - feat(access-request): enable groups to be granted to role and grey button when granted (#14622)
  _by Jonny Dixon on 2025-09-11_

- **f34abede15** - fix(trivy): fix pattern (#14743)
  _by david-leifker on 2025-09-11_

- **030b4ace93** - chore(bump): bump msk-iam-auth (#14744)
  _by david-leifker on 2025-09-11_

- **c3283ef314** - docs(platform-instance): documentation refresh (#14739)
  _by david-leifker on 2025-09-11_

- **dede42d933** - docs(dev-setup): Update IntelliJ setup instructions (#14718)
  _by Abe on 2025-09-11_

## Upstream Merge Session - 2025-09-24 18:22:27

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details

- **Source**: `upstream/master`
- **Target**: `oss-merge-backfill-09-12-2025`
- **Merge Base**: `dede42d9336fe07f396a8086ddc1d5b0bd780faf`
- **End Commit**: `289c5c118457304fed79adc7f687723573690e2c`
- **Total Commits**: 11
- **Strategy**: stacked
- **Started**: Wed Sep 24 18:22:27 UTC 2025

### Commits Included in This Merge

- **e62719b207** - chore(deps): bump actions/download-artifact from 4 to 5 (#14700)
  _by dependabot[bot] on 2025-09-12_

- **87c2d14ea2** - fix(ui) Improve flakiness of modules and templates cypress tests (#14748)
  _by Chris Collins on 2025-09-12_

- **17ec667af1** - chore(deps): bump aws-actions/configure-aws-credentials from 4 to 5 (#14698)
  _by dependabot[bot] on 2025-09-12_

- **8fc449b45f** - chore(deps): bump actions/stale from 9 to 10 (#14697)
  _by dependabot[bot] on 2025-09-12_

- **ef5a5b4fa2** - fix(ci): cloudflare workflow cannot run without token (#14749)
  _by david-leifker on 2025-09-12_

- **46ac6c428a** - chore(deps): bump aquasecurity/trivy-action from 0.33.0 to 0.33.1 (#14699)
  _by dependabot[bot] on 2025-09-12_

- **d3e8139d53** - chore(deps-dev): bump vite from 6.3.5 to 6.3.6 in /datahub-web-react (#14720)
  _by dependabot[bot] on 2025-09-12_

- **becfe19fee** - chore(deps): bump actions/setup-python from 5 to 6 (#14701)
  _by dependabot[bot] on 2025-09-12_

- **10649f3f38** - feat(ingest/fivetran): map google_cloud_postgresql => postgres (#14742)
  _by Michael Maltese on 2025-09-12_

- **7e8049bfe6** - docs(ingestion/redshift): update documentation to cover svv and stv system tables (#14727)
  _by Jonny Dixon on 2025-09-12_

- **289c5c1184** - fix(structured-properties): fix structured properties manage role (#14751)
  _by david-leifker on 2025-09-12_
