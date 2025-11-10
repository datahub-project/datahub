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

## Upstream Merge Session - 2025-09-25 14:55:25

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-backfill-09-13-2025`
- **Merge Base**: `289c5c118457304fed79adc7f687723573690e2c`
- **End Commit**: `e479ca59b77e8e9698374bba64c8d4de25beeb7d`
- **Total Commits**: 15
- **Strategy**: stacked
- **Started**: Thu Sep 25 14:55:25 UTC 2025

### Commits Included in This Merge
- **45ba15f4ac** - fix(ci): revert workaround to enumerate image targets (#14755)
  *by Chakru on 2025-09-15*

- **f706465a05** - fix(ci): use correct path for cypress test result xmls (#14756)
  *by Chakru on 2025-09-15*

- **492e28a938** - feat(ingest/neo4j): migrate Neo4j source to DataHub Python SDK v2 (#14591)
  *by Sergio Gómez Villamor on 2025-09-15*

- **b568eac9bc** - chore(ingestion/iceberg): Include explicit extras in dependencies (#14766)
  *by skrydal on 2025-09-15*

- **92bcccd2bc** - docs(ingestion/redshift): add required permissions for table and view discovery in pre-requisites documentation (#14765)
  *by Jonny Dixon on 2025-09-15*

- **2788b867c5** - fix(ingest/pipeline):  Fix BatchPartitionExecutor Shutdown Race Condition (#14750)
  *by Tamas Nemeth on 2025-09-15*

- **acd7236290** - fixes to improve stability of the ci build (#14752)
  *by Alex on 2025-09-15*

- **f8a401ddd1** - fix(ui) Fix weird indents on schema table descriptions (#14652)
  *by Chris Collins on 2025-09-15*

- **229911a495** - fix(ui): Increasing the 'Try your test' modal width (#14612)
  *by Saketh Varma on 2025-09-15*

- **36cf767d2d** - Revert "chore(ingestion/iceberg): Safe-guard pyiceberg version before pydantic 1->2 transition (#14736)" (#14767)
  *by skrydal on 2025-09-15*

- **d82ae8014e** - feat(bigquery): add created and modified timestamps to dataset containers (#14716)
  *by Sergio Gómez Villamor on 2025-09-15*

- **f5f753343f** - fix(web): Search results Scroll Issue with filters sidebar (#14484)
  *by andrewsrajasekar on 2025-09-15*

- **f38c25dabb** - refactor(ui): Prevent console warnings in Tabs.tsx and SidebarAboutSection.tsx (#14144)
  *by Andrew Sikowitz on 2025-09-15*

- **29f717b7d1** - fix(ui/browse): Fix bug where browse would not paginate when leaving a nested container (#14483)
  *by Andrew Sikowitz on 2025-09-15*

- **e479ca59b7** - docs(Ask DataHub) Update naming conventions for Ask DataHub (#14746)
  *by Maggie Hays on 2025-09-15*


## Upstream Merge Session - 2025-10-01 16:22:32

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-backfill-09-17-2025`
- **Merge Base**: `e479ca59b77e8e9698374bba64c8d4de25beeb7d`
- **End Commit**: `d090c1da4a4540c814bbe4f448d440ddfcc63f2e`
- **Total Commits**: 1
- **Strategy**: stacked
- **Started**: Wed Oct  1 16:22:32 UTC 2025

### Commits Included in This Merge
- **4fd60c698c** - fix(ui): Render the values instead of urns in Policies Modal (#14613)
  *by Saketh Varma on 2025-09-16*


## Upstream Merge Session - 2025-10-03 05:30:49

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-backfill-09-17-2025--remerge-c075d1407cd01226ee835794dceb1a534e754e5f-attempt2`
- **Merge Base**: `4fd60c698c1db62cf5f2391ccf2bc48c8b65a396`
- **End Commit**: `c075d1407cd01226ee835794dceb1a534e754e5f`
- **Total Commits**: 10
- **Strategy**: stacked
- **Started**: Fri Oct  3 05:30:49 UTC 2025

### Commits Included in This Merge
- **e8e97beee6** - fix(ui) Add collection of minor fixes for summary pages and home page (#14771)
  *by Chris Collins on 2025-09-16*

- **8fafa22c68** - feat(ui) Update home page template editability (#14772)
  *by Chris Collins on 2025-09-16*

- **0c388dcfa1** - feat(docs) Add feature guide doc for the new Custom Asset Summaries (#14782)
  *by Chris Collins on 2025-09-16*

- **bd8f335110** - docs(release): Add release notes for version 0.3.14 (#14732)
  *by Gabe Lyons on 2025-09-16*

- **6d2501ba0a** - Revert "docs(release): Add release notes for version 0.3.14" (#14788)
  *by Gabe Lyons on 2025-09-16*

- **667b7cb12c** - fix(sdk_v2/lineage): Fix handling of null platform (#14784)
  *by skrydal on 2025-09-17*

- **002cc398d0** - fix(ingest): change redash sql parse error to warnining (#14785)
  *by Kevin Karch on 2025-09-17*

- **acffdce986** - feat(dbt): add filtering for materialized nodes based on their physical location (#14689)
  *by Abdullah on 2025-09-17*

- **e2c5767e39** - fix(ge_profiler): support nonnull_count for complex types (#14631)
  *by Michael Maltese on 2025-09-17*

- **c075d1407c** - fix(): Fix bundled venv (#14660)
  *by John Joyce on 2025-09-17*


## Upstream Merge Session - 2025-10-03 18:53:51

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-backfill-09-17-2025--remerge-da885c61963c9af37be85265ad103dd049f6b26d`
- **Merge Base**: `c075d1407cd01226ee835794dceb1a534e754e5f`
- **End Commit**: `da885c61963c9af37be85265ad103dd049f6b26d`
- **Total Commits**: 18
- **Strategy**: stacked
- **Started**: Fri Oct  3 18:53:51 UTC 2025

### Commits Included in This Merge
- **eaa472ecf1** - feat(): Adding authenticator for OIDC OAuth  (#14707)
  *by John Joyce on 2025-09-17*

- **6073e20f92** - docs(): Add documentation for Microsoft Teams Application (#14783)
  *by John Joyce on 2025-09-17*

- **6fe831387b** - docs(): Updating datahub cloud actions source docs to include support for MCL events (#14786)
  *by John Joyce on 2025-09-17*

- **29982adc61** - fix(exception): surface exceptions to API response (#14795)
  *by Aseem Bansal on 2025-09-18*

- **3294e721e1** - feat(summary-tab): use manage summary permission to allow editing documentation and links (#14688)
  *by purnimagarg1 on 2025-09-18*

- **7341597835** - fix(ui/summary-tab): fix view more button when switching tabs (#14796)
  *by purnimagarg1 on 2025-09-18*

- **c3768afe3f** - improvement(ui/summary-tab): use editor from component library in CompactMarkdownViewer for consistent styles (#14797)
  *by purnimagarg1 on 2025-09-18*

- **a1c3af3360** - feat(summary-page): add analytics events for asset summary page (#14798)
  *by purnimagarg1 on 2025-09-18*

- **9ef4c3945e** - fix(ui): handle edit documentation button on sidebar with new summary page and update permissions (#14799)
  *by purnimagarg1 on 2025-09-18*

- **df5148a610** - fix(ui): fetch data product info for entity preview (#14800)
  *by purnimagarg1 on 2025-09-18*

- **a50bb30898** - fix(ui/summary-tab): fix functionality on add assets button in assets module (#14801)
  *by purnimagarg1 on 2025-09-18*

- **5c07dc6e5a** - feat(superset/preset): propagate chart & dashboard tags to DataHub (#14538)
  *by Benjamin Maquet on 2025-09-17*

- **d090c1da4a** - fix(summaryTab):  bring fixes from saas (#14764)
  *by v-tarasevich-blitz-brain on 2025-09-18*

- **6fdeb84785** - fix(summaryTab): fix empty state when assets were deleted (#14777)
  *by v-tarasevich-blitz-brain on 2025-09-18*

- **524d7994eb** - docs(logical): Add logical models feature guide (#14774)
  *by Andrew Sikowitz on 2025-09-17*

- **2ccd764ce1** - fix(impact-lineage): separate viz and impact query path (#14773)
  *by david-leifker on 2025-09-17*

- **ad69b3fd7e** - docs(teams): Update Teams App setup instructions (#14803)
  *by Gabe Lyons on 2025-09-17*

- **da885c6196** - refactor(ingestion): lookml source migration to use SDKv2 entities (#14710)
  *by Anush Kumar on 2025-09-17*


## Upstream Merge Session - 2025-10-06 16:40:08

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-sep-30-2025`
- **Merge Base**: `da885c61963c9af37be85265ad103dd049f6b26d`
- **End Commit**: `b25748eda18cc4d82c76eb42ff773682447bcd21`
- **Total Commits**: 63
- **Strategy**: stacked
- **Started**: Mon Oct  6 16:40:08 UTC 2025

### Commits Included in This Merge
- **5efad7bdd2** - fix(summaryTab):  UI fixes (#14778)
  *by v-tarasevich-blitz-brain on 2025-09-18*

- **60dcd8ce21** - Revert "Revert "docs(release): Add release notes for version 0.3.14"" (#14789)
  *by Gabe Lyons on 2025-09-18*

- **5be17c6444** - feat(ingestion/tableau): parameter to have entity owners as email address of owner (#14724)
  *by Jonny Dixon on 2025-09-18*

- **478cd97db4** - improvement(ui/summary-tab): handle deleted structured properties in properties header of asset summary (#14805)
  *by purnimagarg1 on 2025-09-18*

- **8d3e2fa433** - docs(slack): Update Slack setup instructions for token generation (#14745)
  *by Gabe Lyons on 2025-09-18*

- **7768abc6eb** - Update menu on structured props table to new component (#14655)
  *by Anna Everhart on 2025-09-18*

- **d268d14ee2** - feat(ui): add summary page feature flag to local storage and make it generic (#14807)
  *by purnimagarg1 on 2025-09-19*

- **8560c19241** - chore(): bump spring (#14811)
  *by david-leifker on 2025-09-18*

- **7c1200c704** - refactor(ingestion): looker source migration to use SDKv2 entities (#14693)
  *by Anush Kumar on 2025-09-18*

- **50a231cd1a** - Update Entity Dropdown to Menu Component (#14656)
  *by Anna Everhart on 2025-09-18*

- **6bf511d8a8** - fix(ingestion/looker): handle potential None values in explore dataset entity (#14813)
  *by Anush Kumar on 2025-09-18*

- **109a419969** - feat(ui) Add ability to add links to asset header (#14770)
  *by Chris Collins on 2025-09-18*

- **5c79114cb2** - feat(ingestion/superset): add HTTP retry configuration to prevent infinite loops (#14818)
  *by Sergio Gómez Villamor on 2025-09-19*

- **23389d0344** - fix(summary-tab): show correct feedback when trying to add duplicate link (#14817)
  *by purnimagarg1 on 2025-09-19*

- **d26b335b59** - fix(summaryTab): follow ups from reloading modules PR (#14779)
  *by v-tarasevich-blitz-brain on 2025-09-19*

- **04b76b5664** - docs(): Update saas vs. oss docs for v0.3.14 (#14814)
  *by John Joyce on 2025-09-19*

- **01b3ef27d4** - feat(ui): Add context paths for Data Products (#14802)
  *by Saketh Varma on 2025-09-19*

- **beb93f1dc8** - ci: add username pr-labeler (#14828)
  *by Deepak Garg on 2025-09-22*

- **082719b197** - docs(bigquery): improve docs about strategies for lineage/usage extraction (#14806)
  *by Sergio Gómez Villamor on 2025-09-22*

- **3b18fd2369** - chore(deps): fix (org.postgresql:postgresql) (#14831)
  *by rahul MALAWADKAR on 2025-09-22*

- **9778c10b1b** - test: bring cypress tests for structured properties to OSS (#14832)
  *by purnimagarg1 on 2025-09-22*

- **b654d93962** - fix(ui): fix link in entity header flashing infinitely (#14820)
  *by purnimagarg1 on 2025-09-22*

- **d68b86c5d6** - docs(teams): removing stale teams docs (#14810)
  *by Gabe Lyons on 2025-09-22*

- **c130241c2a** - fix(tests): removing flakey v1 test (#14676)
  *by Gabe Lyons on 2025-09-22*

- **00bc0db68c** - feat(quickstart): bump min docker req (#14827)
  *by Deepak Garg on 2025-09-23*

- **a17fc4e0a8** - chore(python): drop pydantic v1 support (#14014)
  *by Harshal Sheth on 2025-09-23*

- **77fa969b76** - fix(ingestion): avoid pyarrow CVE-2023-47248 (#14819)
  *by Sergio Gómez Villamor on 2025-09-23*

- **a379f21089** - feat(ui/ingest): bring back exact start time in run history (#14837)
  *by Adrian Machado on 2025-09-23*

- **ddaebfbde1** - doc(datahub cloud): update recommended versions for cli, helm (#14841)
  *by Aseem Bansal on 2025-09-23*

- **6a97baeee4** - feat(ingestion/snaplogic): Add snaplogic as a source for metadata ingestion (#14231)
  *by sabdul on 2025-09-23*

- **9be65bd971** - feat(ingest/tableau): enable extract_lineage_from_unsupported_custom_sql_queries by default (#14717)
  *by Michael Maltese on 2025-09-23*

- **0f69e96078** - feat(sdk): Added support for Change Audit Stamps in Dashboard and Chart entities (#14815)
  *by Anush Kumar on 2025-09-23*

- **ec166abade** - tests(snaplogic): fix tests (#14848)
  *by Sergio Gómez Villamor on 2025-09-23*

- **b0c9662be7** - feature(transformers): Introduce Set browsePathsV2 transformer (#14825)
  *by skrydal on 2025-09-23*

- **30c16d2e6f** - Update react readme instructions (#14839)
  *by Adrian Machado on 2025-09-23*

- **b6eab24406** - refactor(ui): Fix typo in onboarding "Quality" pop-up message (#14816)
  *by Fabio Serragnoli on 2025-09-24*

- **7e868493fe** - fix(ingest/redshift): Fix for missing schema containers during ingestion (#14845)
  *by Tamas Nemeth on 2025-09-24*

- **6677f7d7ea** - fix(ui/ingest): system source save (#14847)
  *by Aseem Bansal on 2025-09-24*

- **60d888412f** - feat(ui/ingest): filter for status on run history (#14851)
  *by Aseem Bansal on 2025-09-24*

- **dac35550cc** - feat(alchemy): updating input with maxSize and Helper Text (#14856)
  *by Gabe Lyons on 2025-09-24*

- **bd0379b8cb** - fix(security): disable akka dns (#14858)
  *by david-leifker on 2025-09-24*

- **a1abac5250** - feat(ui/ingest): make filter params part of url for navigation (#14852)
  *by Aseem Bansal on 2025-09-25*

- **0fa4639857** - fix(sdk): fixes imports for some SaaS classes (#14843)
  *by Sergio Gómez Villamor on 2025-09-25*

- **941b3c725a** - feat(ingest): add lowercase urn config option to metabase source (#14850)
  *by Kevin Karch on 2025-09-25*

- **72ee770ae3** - feat(ui) Update summary page editability (#14822)
  *by Chris Collins on 2025-09-25*

- **55d714e0cd** - fix(ingest/mssql): don't split_statements on keywords inside bracketed identifiers (#14863)
  *by Michael Maltese on 2025-09-25*

- **a85f83d556** - fix(): fix config value (#14865)
  *by david-leifker on 2025-09-25*

- **eb066dcf1e** - fix(ingest/gcs): fix a number of issues and add integration tests (#14857)
  *by Michael Maltese on 2025-09-25*

- **4bf3f0e66d** - feat(Plugin Loader) Add config to control plugin loader when failure happens (#14769)
  *by Jesse Jia on 2025-09-26*

- **7e9c525448** - fix(ingestion): Fix for module level variable caching in sqllite check (#14861)
  *by Tamas Nemeth on 2025-09-26*

- **50c5841b50** - fix(schema-registry): fix v1.2.0.1 schema registry bug (#14846)
  *by david-leifker on 2025-09-26*

- **c18b125a05** - feat(ingestion): Enhanced column lineage extraction for Looker/LookML (#14826)
  *by Anush Kumar on 2025-09-26*

- **900d7fe244** - docs: hide pydantic_removed_field marked fields from documentation (#14829)
  *by Sergio Gómez Villamor on 2025-09-28*

- **0b75674ab3** - free up disk space in quickstart verification CI (#14879)
  *by Chakru on 2025-09-29*

- **8d13b03e85** - feat(sdk/search): add owner filter (#14649)
  *by Mayuri Nehate on 2025-09-29*

- **e698f0bf1d** - feat(sdk/search): add tags, glossary terms filter (#14873)
  *by Mayuri Nehate on 2025-09-29*

- **1e749d0c55** - docs(ingest): decode strings for easier getting started (#14830)
  *by Aseem Bansal on 2025-09-29*

- **795a6828e8** - feat(identity): only suggest users that are active or have displayName (#14867)
  *by Ben Blazke on 2025-09-29*

- **e9e18e4705** - feat(secret): FileSecretStore and EnvironmentSecretStore (#14882)
  *by Sergio Gómez Villamor on 2025-09-30*

- **af12c2be37** - docs(snaplogic): Add snaplogic to integration page (#14881)
  *by sabdul on 2025-09-30*

- **ba56bc9e29** - docs(cli): add details on parameters (#14886)
  *by Aseem Bansal on 2025-09-30*

- **ddffb85e14** - fix(auth): include dataProcessInstance in policies UI (#14880)
  *by Chakru on 2025-09-30*

- **b25748eda1** - feat(web): UI pagination for Assertion List page  (#14859)
  *by Adrian Machado on 2025-09-30*


## Upstream Merge Session - 2025-10-11 19:18:34

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-oct-10-2025`
- **Merge Base**: `b25748eda18cc4d82c76eb42ff773682447bcd21`
- **End Commit**: `ef0b4a71ce77ec1b7ba7ef35ec9854998be6524c`
- **Total Commits**: 67
- **Strategy**: stacked
- **Started**: Sat Oct 11 19:18:34 UTC 2025

### Commits Included in This Merge
- **8248999758** - chore(model): remove unused model (#14887)
  *by Aseem Bansal on 2025-10-01*

- **00caa38adf** - feat(ingestion/sqlglot): preserve CTEs when extracting SELECT from INSERT statements and add corresponding unit test (#14898)
  *by Anush Kumar on 2025-10-01*

- **04d0a50118** - feat(): Basepath support (#14866)
  *by david-leifker on 2025-10-01*

- **0f5d5fd358** - fix(protobuf): use DynamicMessage for MESSAGE-type extension defaults (#14900)
  *by Abe on 2025-10-01*

- **660a4efa5f** - MCL Generation via CDC (#14824)
  *by Chakru on 2025-10-02*

- **f7ea7f033d** - chore(devenv): upgrade of opensearch to 2.17 and stability improvements (#14895)
  *by Alex on 2025-10-01*

- **5da54bf14d** - feat(s3/ingest): performance improvements for get_dir_to_process and get_folder_info (#14709)
  *by Michael Maltese on 2025-10-02*

- **28b866a721** - feat: ConnectionModel and DataHubGraph:get_urns_by_filter and StructuredProperties from saas (#14912)
  *by Sergio Gómez Villamor on 2025-10-02*

- **5b4a082c03** - fix(ingest/snowflake): Fixed the Snowflake external URL generation issue for privatelink connections. (#14905)
  *by Tamas Nemeth on 2025-10-02*

- **9f4ec9b220** - ci(nightly): add more profiles to nightly tests (#14907)
  *by Chakru on 2025-10-03*

- **1d46fc7fb0** - ci(cloudflare): fix workflow check for secret (#14906)
  *by Chakru on 2025-10-03*

- **9b6ad2263f** - chore(): bump grpc-protobuf (#14915)
  *by david-leifker on 2025-10-02*

- **78d258383f** - fix(ingest/snowflake): Skip sql parsing if all the features disable in config where it is needed (#14908)
  *by Tamas Nemeth on 2025-10-03*

- **cecb91b615** - feat(ingest): add high level stage for ingestion (#14862)
  *by Aseem Bansal on 2025-10-03*

- **1d0e3778a5** - fix(ingest/grafana): add exception handling (#14921)
  *by Aseem Bansal on 2025-10-03*

- **c88a4a5a48** - config(gms): enable some features by default (#14889)
  *by Aseem Bansal on 2025-10-03*

- **c7ea813a6e** - ci(reviewers): add petemango to pr-labeller (#14922)
  *by Peter Wang on 2025-10-03*

- **e244fc56ee** - fix(ci): bump metadata-ingestion runner (#14924)
  *by david-leifker on 2025-10-03*

- **6cf03160ba** - test(searchBarAutocomplete): add cypress tests (#13333)
  *by v-tarasevich-blitz-brain on 2025-10-03*

- **e58061c81d** - feat(structuredProperties): add new property to hide properties with emty value (#14870)
  *by v-tarasevich-blitz-brain on 2025-10-03*

- **37c9ed8564** - feat(structuredProperties): add option to hide properties with empty values (#14872)
  *by v-tarasevich-blitz-brain on 2025-10-03*

- **d0e0e20cff** - fix(ui) Fix re-expanding entity name after sidebar opens/closes (#14925)
  *by Chris Collins on 2025-10-03*

- **b4d6877746** - tests(structuredProperties): add cypress tests (#14888)
  *by v-tarasevich-blitz-brain on 2025-10-03*

- **2d7c009b81** - docs(release-notes): disclaimers for 0.3.14 (#14812)
  *by Jay on 2025-10-03*

- **eea46700f6** - chore(doc): Fix json schema generation after pydantic v2 move (#14926)
  *by skrydal on 2025-10-04*

- **0613f8d471** - feat(quickstart): bump min docker req and add validation (#14927)
  *by Deepak Garg on 2025-10-06*

- **a043d15193** - docs: fix datajob docs inline code format (#14933)
  *by Hyejin Yoon on 2025-10-07*

- **40b51ac2da** - feat(ingestion): Added Databricks support to Fivetran source (#14897)
  *by Anush Kumar on 2025-10-06*

- **e847b58472** - feat(ingest): ensure payload size constraints for queryProperties, querySubjects and upstreamLineage aspects (#14919)
  *by Sergio Gómez Villamor on 2025-10-06*

- **3eac7fab5b** - feat(search): implement multi-client search engine shim for ES8 support (#14904)
  *by RyanHolstien on 2025-10-06*

- **5d007f04c4** - fix(build): fix "grep: invalid option -- P" error in quickstart (#14916)
  *by Peter Wang on 2025-10-06*

- **335290dfec** - feat: RelationshipChangeEvent model + attribution action graph + kafka msk iam (all from SaaS) (#14938)
  *by Sergio Gómez Villamor on 2025-10-07*

- **c092e91223** - feat(ui/ingest): add source errors, warnings (#14939)
  *by Aseem Bansal on 2025-10-07*

- **5d28c2fd14** - fix(smoke-tests): smoke test fixes for postgres profile (#14940)
  *by Chakru on 2025-10-07*

- **9c22a4ae62** - fix(web): embedded search list responsiveness (#14913)
  *by Jay on 2025-10-07*

- **d87f46d686** - fix(entity controller) Fix case sensitivity in entity controller (#14902)
  *by Jesse Jia on 2025-10-07*

- **f2d3380226** - improvement(summary-tab): hide current property in replace dropdown of property header (#14842)
  *by purnimagarg1 on 2025-10-07*

- **86e5c13f29** - test(customLinks): add cypress tests (#274) (#14834)
  *by v-tarasevich-blitz-brain on 2025-10-07*

- **61ab5a8a17** - fix(ui/LineChart): adjust scaling of the line chart for the data with a small difference (#14836)
  *by v-tarasevich-blitz-brain on 2025-10-07*

- **91b17a7f5b** - feat(customLinks): add upsert link endpoint (#291) (#14854)
  *by v-tarasevich-blitz-brain on 2025-10-07*

- **d14ccdf57c** - feat(analytics) Support google tag tracking only with ID supplied (#14946)
  *by Chris Collins on 2025-10-07*

- **b7b4f1c9a5** - feat(uplodaFiles): add feature flag (#14951)
  *by v-tarasevich-blitz-brain on 2025-10-08*

- **136e4dc0d4** - test(ingestion): add cypress tests for redesigned ingestion flow (#14844)
  *by purnimagarg1 on 2025-10-08*

- **9e69b3f881** - test(cypress/statsTabV2): add cypress tests (#13495)
  *by v-tarasevich-blitz-brain on 2025-10-08*

- **0032384342** - test(customLinks): add integration tests (#275) (#14835)
  *by v-tarasevich-blitz-brain on 2025-10-08*

- **9992722e63** - feat(structuredProperties): refresh structured properties on update (#14910)
  *by v-tarasevich-blitz-brain on 2025-10-08*

- **5cdb7e2594** - docs(ingestion): Updating breaking changes for LookML and Looker sources (#14947)
  *by Anush Kumar on 2025-10-08*

- **3caa781540** - chore: add 'askumar27' to PR labeler configuration (#14949)
  *by Sergio Gómez Villamor on 2025-10-08*

- **0a0fcef047** - fix(protobuf): skip MESSAGE-type options in PropertyVisitor (#14957)
  *by Abe on 2025-10-08*

- **b6ff38d1c3** - bugfix(setup): pin pydantic version due to incompatibility with pyiceberg (#14959)
  *by Anush Kumar on 2025-10-08*

- **2a4f57791b** - bugfix(fivetran/unity): move UnityCatalogConnectionConfig import to avoid circular deps with ge_profiler (#14956)
  *by Anush Kumar on 2025-10-08*

- **cfda03cf6f** - docs(snowflake): use_queries_v2 + some minor fixes (#14944)
  *by Sergio Gómez Villamor on 2025-10-09*

- **4803e9e9b6** - fix(fivetran/setup): updated fivetran databricks dependencies (#14962)
  *by Anush Kumar on 2025-10-09*

- **b23571c16f** - Update release notes for v0.3.14.1-acryl (#14958)
  *by Gabe Lyons on 2025-10-09*

- **d98abe877e** - fix(smoke-test):cleanup existing token and add wait (#14965)
  *by Deepak Garg on 2025-10-09*

- **26685cba6c** - chore(deps): bump tar-fs from 2.1.3 to 2.1.4 in /docs-website (#14875)
  *by dependabot[bot] on 2025-10-09*

- **82f1e1d035** - doc(smoke test): add some guideline for smoke test (#14967)
  *by Aseem Bansal on 2025-10-09*

- **3d210ae34c** - chore(): bump grpc-netty (#14969)
  *by david-leifker on 2025-10-09*

- **e72f76b536** - ci(): metadata-io instance size (#14876)
  *by david-leifker on 2025-10-09*

- **06aebfe86f** - fix(ui): Proper url encoding in Impact view filters  (#14963)
  *by Saketh Varma on 2025-10-10*

- **be7cc44670** - fix(ui): Fix overlapping Modals  (#14964)
  *by Saketh Varma on 2025-10-10*

- **602f40d01d** - fix: change deprecated filename key to path in example recipe (#14950)
  *by Túlio Lima on 2025-10-10*

- **7b5680efbc** - fix(ingest): serialisation of structured report (#14973)
  *by Aseem Bansal on 2025-10-10*

- **c94ea28140** - refactor(policy): Update policy check for get invite token and create invite token (#14941)
  *by DucNgoQuang on 2025-10-10*

- **a3fae8d1e2** - fix(log): do not consider error if not strict (#14977)
  *by Aseem Bansal on 2025-10-10*

- **c5d70914cd** - feat(loadIndices): loadIndices upgrade (#14928)
  *by david-leifker on 2025-10-10*

- **ef0b4a71ce** - fix(impact): add missing executor pool (#14976)
  *by david-leifker on 2025-10-10*


## Upstream Merge Session - 2025-10-14 05:11:27

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-oct-13-2025`
- **Merge Base**: `ef0b4a71ce77ec1b7ba7ef35ec9854998be6524c`
- **End Commit**: `79eac47ec5aae2ca4120f4a64a3e364e9bfa9ca8`
- **Total Commits**: 13
- **Strategy**: stacked
- **Started**: Tue Oct 14 05:11:27 UTC 2025

### Commits Included in This Merge
- **b45d606062** - fix(authentication): fix PlayCacheSessionStore (#14754)
  *by trialiya on 2025-10-11*

- **e8c144362e** - fix(ingest/dremio): Dremio sql parsing fix (#14974)
  *by Tamas Nemeth on 2025-10-13*

- **c20d2eb49e** - fix(cli): remove pydantic warning (#14987)
  *by Aseem Bansal on 2025-10-13*

- **6a167da408** - fix(build): Add --copies flag to Python venv creation for better compatibility (#14120)
  *by Pedro Silva on 2025-10-13*

- **82338628b6** - fix(smoke-test):cleanup existing token and add wait (#14988)
  *by Deepak Garg on 2025-10-13*

- **9fb82a73ad** - fix(ingest/deltalake) Deltalake ingestor doesn't delete metadata if the table is deleted.  (#14763)
  *by alplatonov on 2025-10-13*

- **db4a801f9d** - fix(ci): only applies to master (#14981)
  *by david-leifker on 2025-10-13*

- **c5cdce2c3d** - fix(upsertLink): remove log (#14984)
  *by v-tarasevich-blitz-brain on 2025-10-13*

- **d7b73b615d** - refactor(ui) : New great expectation logo (#14983)
  *by DucNgoQuang on 2025-10-13*

- **e4184fe251** - fix(api): add validation entity type for policy creation (#14955)
  *by Aseem Bansal on 2025-10-13*

- **ed4d3680a9** - build(actions): optimize docker layer caching for bundled-venvs (#14945)
  *by Chakru on 2025-10-13*

- **b0581dfe5a** - quickstart Reload improvements (#14982)
  *by Chakru on 2025-10-13*

- **79eac47ec5** - fix(upsertLink): reorder fields to be consistent with Saas (#14986)
  *by v-tarasevich-blitz-brain on 2025-10-13*


## Upstream Merge Session - 2025-10-14 18:58:50

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-14-185848`
- **Merge Base**: `79eac47ec5aae2ca4120f4a64a3e364e9bfa9ca8`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 13
- **Strategy**: stacked
- **Started**: Tue Oct 14 18:58:50 UTC 2025

### Commits Included in This Merge
- **7e9b1353ac** - fix(ui): Entity text overflow in 'owner of' section (#14934)
  *by Saketh Varma on 2025-10-14*

- **5f03c94484** - refactor(test): refactor smoke test (#14993)
  *by Aseem Bansal on 2025-10-14*

- **7ee4085316** - fix(ingest/grafana): filter out invalid input field workunits (#14972)
  *by Aseem Bansal on 2025-10-14*

- **d50a06329b** - refactor(test): refactor smoke test (#14996)
  *by Aseem Bansal on 2025-10-14*

- **a85c4ffca8** - refactor(test): refactor smoke test (#14998)
  *by Aseem Bansal on 2025-10-14*

- **2b9d250c29** - fix(build): add ability to specificy constraints for bundled-venvs (#14999)
  *by Chakru on 2025-10-14*

- **6e5b3580d8** - fix(python): skip pydantic_core 2.41.3 (#14997)
  *by Sergio Gómez Villamor on 2025-10-14*

- **4ce4718729** - fix(ingest/mssql): ODBC connection always needs a database name (#14994)
  *by Michael Maltese on 2025-10-14*

- **c4e104eba2** - feat(dataproduct): output ports (#15000)
  *by Sergio Gómez Villamor on 2025-10-14*

- **0f71cf0ebf** - chore(iceberg): bump dependency (#15001)
  *by Sergio Gómez Villamor on 2025-10-14*

- **da0d4dd499** - refactor(test): refactor smoke test (#15003)
  *by Aseem Bansal on 2025-10-14*

- **559a08c9af** - update tag editing to include name and updated menu (#14884)
  *by Anna Everhart on 2025-10-14*

- **e25a82abe3** - fix(ui) Move template editable changes to their own files (#14923)
  *by Chris Collins on 2025-10-14*


## Upstream Merge Session - 2025-10-16 06:16:49

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-16-061648`
- **Merge Base**: `e25a82abe3f12747951e0b12ae38dec0690e9c2e`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 8
- **Strategy**: stacked
- **Started**: Thu Oct 16 06:16:49 UTC 2025

### Commits Included in This Merge
- **f9913cd944** - feat(cli): graphql command (#14781)
  *by Shirshanka Das on 2025-10-14*

- **60f0abb72a** - choir(ingestion/sqlparser): Bump sqlglot to v27.27.0 (#15008)
  *by Tamas Nemeth on 2025-10-15*

- **18d3eec8d7** - feat(ingest/unity): Use sql to extract query history for usage (#14953)
  *by Tamas Nemeth on 2025-10-15*

- **ffd2e761b1** - fix(doc): Update permissions in redshift quickstart doc (#14909)
  *by Tamas Nemeth on 2025-10-15*

- **f02ef04e6d** - fix(ingest/teradata):  Fix Teradata lineage URN generation to prevent extra database prefixes (#14715)
  *by Tamas Nemeth on 2025-10-15*

- **673bed6595** - test(summaryTab): cypress tests (#14808)
  *by v-tarasevich-blitz-brain on 2025-10-15*

- **f2321d5df9** - fix(externalEvents): fixes timeout issues for external events api (#14979)
  *by RyanHolstien on 2025-10-15*

- **3b0c34f84d** - refactor(test): refactor smoke test (#15010)
  *by Aseem Bansal on 2025-10-16*


## Upstream Merge Session - 2025-10-17 00:00:08

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-17-000007`
- **Merge Base**: `3b0c34f84d9e75d649824a624c02f84fe114c8c3`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 10
- **Strategy**: stacked
- **Started**: Fri Oct 17 00:00:08 UTC 2025

### Commits Included in This Merge
- **10c28be20c** - fix(test): remove un-necessary test (#15015)
  *by Aseem Bansal on 2025-10-16*

- **1934ed45e8** - feat(ingestion): add ability to exclude archived mode reports. (#14978)
  *by Neha Gslab on 2025-10-16*

- **afc6b138b7** - fix(mongo-source): Add failing test for complex topic name parsing (#15002)
  *by Chaiwon Hyun on 2025-10-16*

- **6610883fe1** - refactor(sdk): extract env variables into a single file (#15021)
  *by Aseem Bansal on 2025-10-16*

- **fb895c91e4** - doc(dev): update claude md for git worktrees (#15020)
  *by Aseem Bansal on 2025-10-16*

- **b3d354bf89** - chore: migrate Unity catalog from deprecated SqlParsingBuilder to SqlParsingAggregator and remove SqlParsingBuilder (#15005)
  *by Sergio Gómez Villamor on 2025-10-16*

- **45dda5e33a** - feat(cli): add user `add` command (#15011)
  *by Aseem Bansal on 2025-10-16*

- **caad6d5bdc** - chore(): bump netty (#15027)
  *by david-leifker on 2025-10-16*

- **878a55e723** - refactor(test): refactor smoke test (#15019)
  *by Aseem Bansal on 2025-10-17*

- **b301b13474** - docs(elasticsearch): add note for shim config (#15030)
  *by RyanHolstien on 2025-10-16*


## Upstream Merge Session - 2025-10-18 00:00:07

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-18-000006`
- **Merge Base**: `b301b134740b1275477f49a55db140abfc02a801`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 13
- **Strategy**: stacked
- **Started**: Sat Oct 18 00:00:07 UTC 2025

### Commits Included in This Merge
- **317d1292db** - fix(auth):allow colon in secret password (#15023)
  *by Deepak Garg on 2025-10-17*

- **1703f424d2** - chore(deps): Upgrade to latest (com.mysql:mysql-connector-j) (#15018)
  *by rahul MALAWADKAR on 2025-10-17*

- **e48008fa38** - chore(deps): bump actions/labeler from 5 to 6 (#14760)
  *by dependabot[bot] on 2025-10-17*

- **6af05ce765** - chore(deps): bump actions/github-script from 7 to 8 (#14761)
  *by dependabot[bot] on 2025-10-17*

- **7b3cb7805e** - chore(deps): bump actions/setup-java from 4 to 5 (#14759)
  *by dependabot[bot] on 2025-10-17*

- **74d620fcc3** - chore(deps): bump github/codeql-action from 3 to 4 (#14985)
  *by dependabot[bot] on 2025-10-17*

- **65d613a99e** - refactor(test): make python version ==3.11.x (#15035)
  *by Aseem Bansal on 2025-10-17*

- **abad667d88** - fix(cypress): fix summaryTab.js (#15017)
  *by v-tarasevich-blitz-brain on 2025-10-17*

- **2578fa2b0a** - improvement(ui): consolidate all rich text editors to use editor in component library (#15009)
  *by purnimagarg1 on 2025-10-17*

- **7b454c7559** - fix(mce-consumer): database max connections (#15042)
  *by david-leifker on 2025-10-17*

- **46b5ce0d14** - fix(ingestion/mongodb): Fix handling of arrays containing complex structures (#15026)
  *by skrydal on 2025-10-17*

- **4650fb6bba** - fix(collectionModule): fix long entity name (#15033)
  *by v-tarasevich-blitz-brain on 2025-10-17*

- **da4849895a** - feat(uploadFiles): add endpoint to get presigned upload url (#14943)
  *by v-tarasevich-blitz-brain on 2025-10-18*


## Upstream Merge Session - 2025-10-22 00:13:29

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-18-000006`
- **Merge Base**: `da4849895a4e7b1283019504384a87006b89b849`
- **End Commit**: `fd00f04b0f8e95dac55cd5864f40bdb3a4101ee5`
- **Total Commits**: 19
- **Strategy**: stacked
- **Started**: Wed Oct 22 00:13:29 UTC 2025

### Commits Included in This Merge
- **6fc68b6c6b** - feat(gms) Add new REST endpoint for retrieving files from S3 (#15048)
  *by Chris Collins on 2025-10-17*

- **d54dd9642d** - fix(security): Update dependencies to address multiple CVEs (#15045)
  *by Esteban Gutierrez on 2025-10-18*

- **ceafe2083b** - fix(security): Update dependencies to address multiple CVEs (addendum) (#15049)
  *by Esteban Gutierrez on 2025-10-18*

- **604e5df446** - chore(docker): upgrade OpenSearch from 2.17.0 to 2.19.3 (#15047)
  *by Abe on 2025-10-19*

- **33089b4eaa** - chore(docs): Clarifying listDomains query documentation (#15050)
  *by skrydal on 2025-10-20*

- **b4b96ab8f9** - feat(ui/ingest): unity-catalog => databricks (#14636)
  *by Michael Maltese on 2025-10-20*

- **22c602dcaf** - feat(ingest/postgres,mysql): Add iam auth support for MySql and Postgresql source (#14899)
  *by Tamas Nemeth on 2025-10-21*

- **0f63e6d367** - chore(redshift): log time taken queries (#15054)
  *by Sergio Gómez Villamor on 2025-10-21*

- **27fca1c0bf** - feat(ui/platform): Adds env variable to control default skipHighlighting search flag (#15038)
  *by Saketh Varma on 2025-10-21*

- **c436e97e84** - fix(ui): Removing unused props and trimming down the graphql objects (#15043)
  *by Saketh Varma on 2025-10-21*

- **bca63189fe** - ci(label): add username to pr-labeler (#15064)
  *by Hyejin Yoon on 2025-10-21*

- **0b5616208f** - docs(ingestion/bigquery): update docs to cover bigquery.tables.getData for use_tables_list_query_v2 parameter (#14728)
  *by Jonny Dixon on 2025-10-21*

- **de38fd048e** - feat(ui) Add new file upload to S3 extension in the UI (#15061)
  *by Chris Collins on 2025-10-21*

- **7628ab3fa4** - fix(redsfhit): fix closed cursor (#15065)
  *by Sergio Gómez Villamor on 2025-10-21*

- **d0bd2d50e4** - feat(ui): Show all views in settings (#14971)
  *by Saketh Varma on 2025-10-21*

- **8f21a6c6ce** - feat(homepage): add default platforms module on the homepage (#14942)
  *by purnimagarg1 on 2025-10-22*

- **2973da553f** - feat(sql-setup): system-update replacement for mysql/postgres setup (#15044)
  *by david-leifker on 2025-10-21*

- **edcc8bca76** - feat(elasticsearch-setup): system-update replacement for elasticsearch-setup (#15058)
  *by david-leifker on 2025-10-21*

- **fd00f04b0f** - SE-123: added product update resolver (#14980)
  *by Ben Blazke on 2025-10-21*


## Upstream Merge Session - 2025-10-22 21:34:21

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-22-213420`
- **Merge Base**: `da4849895a4e7b1283019504384a87006b89b849`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 28
- **Strategy**: stacked
- **Started**: Wed Oct 22 21:34:21 UTC 2025

### Commits Included in This Merge
- **6fc68b6c6b** - feat(gms) Add new REST endpoint for retrieving files from S3 (#15048)
  *by Chris Collins on 2025-10-17*

- **d54dd9642d** - fix(security): Update dependencies to address multiple CVEs (#15045)
  *by Esteban Gutierrez on 2025-10-18*

- **ceafe2083b** - fix(security): Update dependencies to address multiple CVEs (addendum) (#15049)
  *by Esteban Gutierrez on 2025-10-18*

- **604e5df446** - chore(docker): upgrade OpenSearch from 2.17.0 to 2.19.3 (#15047)
  *by Abe on 2025-10-19*

- **33089b4eaa** - chore(docs): Clarifying listDomains query documentation (#15050)
  *by skrydal on 2025-10-20*

- **b4b96ab8f9** - feat(ui/ingest): unity-catalog => databricks (#14636)
  *by Michael Maltese on 2025-10-20*

- **22c602dcaf** - feat(ingest/postgres,mysql): Add iam auth support for MySql and Postgresql source (#14899)
  *by Tamas Nemeth on 2025-10-21*

- **0f63e6d367** - chore(redshift): log time taken queries (#15054)
  *by Sergio Gómez Villamor on 2025-10-21*

- **27fca1c0bf** - feat(ui/platform): Adds env variable to control default skipHighlighting search flag (#15038)
  *by Saketh Varma on 2025-10-21*

- **c436e97e84** - fix(ui): Removing unused props and trimming down the graphql objects (#15043)
  *by Saketh Varma on 2025-10-21*

- **bca63189fe** - ci(label): add username to pr-labeler (#15064)
  *by Hyejin Yoon on 2025-10-21*

- **0b5616208f** - docs(ingestion/bigquery): update docs to cover bigquery.tables.getData for use_tables_list_query_v2 parameter (#14728)
  *by Jonny Dixon on 2025-10-21*

- **de38fd048e** - feat(ui) Add new file upload to S3 extension in the UI (#15061)
  *by Chris Collins on 2025-10-21*

- **7628ab3fa4** - fix(redsfhit): fix closed cursor (#15065)
  *by Sergio Gómez Villamor on 2025-10-21*

- **d0bd2d50e4** - feat(ui): Show all views in settings (#14971)
  *by Saketh Varma on 2025-10-21*

- **8f21a6c6ce** - feat(homepage): add default platforms module on the homepage (#14942)
  *by purnimagarg1 on 2025-10-22*

- **2973da553f** - feat(sql-setup): system-update replacement for mysql/postgres setup (#15044)
  *by david-leifker on 2025-10-21*

- **edcc8bca76** - feat(elasticsearch-setup): system-update replacement for elasticsearch-setup (#15058)
  *by david-leifker on 2025-10-21*

- **fd00f04b0f** - SE-123: added product update resolver (#14980)
  *by Ben Blazke on 2025-10-21*

- **e9becdd11c** - feat(ingest/snowfle,bigquery): Stateful time window ingestion for queries v2 with bucket alignment (#15040)
  *by Tamas Nemeth on 2025-10-22*

- **afdf18e370** - feat(uploadFiles): disable feature flag when s3 isn't configured (#14990)
  *by v-tarasevich-blitz-brain on 2025-10-22*

- **efae5559fb** - feat(file-upload): implement selecting and uploading file via button on the editor toolbar (#15036)
  *by purnimagarg1 on 2025-10-22*

- **c3ae2133ea** - feat(uploadFiles): handle errors (#15024)
  *by v-tarasevich-blitz-brain on 2025-10-22*

- **396868391d** - feat(gms) Add new dataHubFile entity and relevant graphql mappings (#15028)
  *by Chris Collins on 2025-10-22*

- **eecc2e922c** - fix(ingest): Handle empty column names in SQL parsing column lineage (#15013)
  *by kyungsoo-datahub on 2025-10-22*

- **e9fd5fe475** - feat(gms) Add permission checks to download file REST endpoint (#15059)
  *by Chris Collins on 2025-10-22*

- **605ce5e5ce** - fix(mongodb): fixes missing deps for unit testing (#15077)
  *by Sergio Gómez Villamor on 2025-10-22*

- **0641aa434d** - feat(uloadFiles): add support to schema field and asset documentation tab (#15055)
  *by v-tarasevich-blitz-brain on 2025-10-22*


## Upstream Merge Session - 2025-10-24 16:40:01

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-24-164000`
- **Merge Base**: `0641aa434db4c1e36d80fb402e7b452a54343798`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 23
- **Strategy**: stacked
- **Started**: Fri Oct 24 16:40:01 UTC 2025

### Commits Included in This Merge
- **9b212562dd** - feat(uploadFiles): add anaytics events (#15080)
  *by v-tarasevich-blitz-brain on 2025-10-23*

- **b9c263b0f6** - fix(datahub-web-react): Update Titles and subtitles of Settings pages (#15072)
  *by ani-malgari on 2025-10-22*

- **a7f1c6b32f** - fix(sort): fix out of bounds exception with count 0 (#15085)
  *by david-leifker on 2025-10-22*

- **81310f7e4f** - fix(ui): Minor UX improvements for Query Tab (#15074)
  *by Saketh Varma on 2025-10-23*

- **12cf28186c** - fix(druid): fixes docker setup for postgres in druid integration tests (#15090)
  *by Sergio Gómez Villamor on 2025-10-23*

- **695fb4904e** - fix(quickstart): handle potential wrapping of df cmd output in disk space check (#15083)
  *by Chakru on 2025-10-23*

- **8079e1c91b** - fix(system-update): fix index prefix in new setup for es/os (#15087)
  *by david-leifker on 2025-10-23*

- **0d91067feb** - chore: udpate ingestion support status (#15053)
  *by Sergio Gómez Villamor on 2025-10-23*

- **4d0f2b76e3** - fix(search): Remove duplicate filter values that can't be handled by elasticsearch (#15078)
  *by Andrew Sikowitz on 2025-10-23*

- **a5f823a07b** - fix(files) Remove bucket name prefix to presigned urls for files (#15082)
  *by Chris Collins on 2025-10-23*

- **232bf3551d** - fix(smoke-tests): correct server config access in tracking test (#15092)
  *by Shirshanka Das on 2025-10-23*

- **b1b68b42fb** - feat(docs): enhance metadata model entity documentation with field tables and SDK examples (#15095)
  *by Shirshanka Das on 2025-10-23*

- **114186b01b** - feat(fivetran/google_sheets): add Google Sheets support and API client integration (#15007)
  *by Anush Kumar on 2025-10-23*

- **96c20cb81f** - fix(datahub-web-react): Fixed routing issue to prevent non-admin users from accessing analytics page (#15093)
  *by ani-malgari on 2025-10-23*

- **eed191c72e** - fix(datahub-web-react): Fix icons not rendering for domains on autocomplete search results (#15096)
  *by ani-malgari on 2025-10-23*

- **f5d0efcc73** - feat(oss): add clientId to ctaLink (#15071)
  *by Ben Blazke on 2025-10-23*

- **7372b9120d** - SE-123: Fetch product updates from backend  (#15079)
  *by Ben Blazke on 2025-10-23*

- **5a1569743a** - feat(docs): add structured property for search field names in metadata model (#15097)
  *by Shirshanka Das on 2025-10-23*

- **a865b6ba63** - fix(ingest/dremio): Fix platform_instance URN generation (#15076)
  *by Tamas Nemeth on 2025-10-24*

- **9bfb90e188** - docs(testing): add guidelines to avoid AI test anti-patterns (#15073)
  *by Abe on 2025-10-24*

- **61f9dd92ab** - refactor(smoke test): centralise env variables (#15100)
  *by Aseem Bansal on 2025-10-24*

- **a18e716bdd** - fix(restore-indices): log error instead of silent failure (#15070)
  *by david-leifker on 2025-10-24*

- **55c4692e19** - fix(ui): Fix routing issue in Manage Views (#15101)
  *by Saketh Varma on 2025-10-24*


## Upstream Merge Session - 2025-10-29 04:03:24

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-29-040323`
- **Merge Base**: `55c4692e19c4c4cfc3ab697142c19cb1656ecee9`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 19
- **Strategy**: stacked
- **Started**: Wed Oct 29 04:03:24 UTC 2025

### Commits Included in This Merge
- **080233054b** - fix(ui/column-stats): fix unopenable side panel for nested column stats (#14874)
  *by Jonny Dixon on 2025-10-24*

- **8038dc9c45** - fix(elasticsearch): fix regression system-update elasticsearch-setup (#15107)
  *by david-leifker on 2025-10-24*

- **bc25b82da5** - fix(web): tsc null check (#15109)
  *by Jay on 2025-10-25*

- **ee20dbbcb3** - fix(prefix): improve/fix es prefix calculation (#15110)
  *by david-leifker on 2025-10-25*

- **f844ea7b2d** - chore(metabase): use non deprecated fields in ChartInfo and DashboardInfo aspects (#15102)
  *by Sergio Gómez Villamor on 2025-10-27*

- **9016a4446f** - feat(docs): generate DataHub-optimized entity documentation variant (#15111)
  *by Shirshanka Das on 2025-10-27*

- **dc0b9b2f76** - fix(docs): Fix upgrade restore docs (#15103)
  *by Harsh Verma on 2025-10-27*

- **6af5182e9b** - fix(mssql): replace permission-based RDS detection with server name analysis and fix SQLAlchemy 1.4+ compatibility (#14975)
  *by Sergio Gómez Villamor on 2025-10-27*

- **f05f3e40f2** - fix(ingestion/dremio): handle dremio oom errors when ingesting large amount of metadata (#14883)
  *by Jonny Dixon on 2025-10-27*

- **680d16fa84** - feat(ingestion/sql-queries): add performance optimizations, S3 support, temp table patterns (#14757)
  *by Brock Griffey on 2025-10-27*

- **5c2ee31fde** - docs(snowflake): lineage limitations (#15115)
  *by Sergio Gómez Villamor on 2025-10-28*

- **0baabeb639** - fix(ci): reworked smoke test batching (#15039)
  *by Chakru on 2025-10-28*

- **0f79f62187** - fix(cypress): fix summaryTab.js (#15136)
  *by v-tarasevich-blitz-brain on 2025-10-28*

- **bc814fd328** - feat(ci): monitor test weights and update automatically (#15126)
  *by Chakru on 2025-10-28*

- **653e5a5b7e** - feat(ui/theme): Support styled-components theming; clean up ant theming (#14787)
  *by Andrew Sikowitz on 2025-10-28*

- **a6222ca5cc** - Fixed some UX and React reconciliation errors on Tags page (#15098)
  *by ani-malgari on 2025-10-28*

- **eeb2c88469** - feat(search): unified entity index (#14966)
  *by david-leifker on 2025-10-28*

- **b02f182e70** - fix(cypress) Skip summaryTab.js while we fix it (#15135)
  *by Chris Collins on 2025-10-28*

- **a3275c37c1** - fix(docs): move DataHub variant entity docs to separate directory (#15144)
  *by Shirshanka Das on 2025-10-28*


## Upstream Merge Session - 2025-10-31 00:00:07

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-10-31-000006`
- **Merge Base**: `a3275c37c1692acacdb1fc4966c2fd388d72b086`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 10
- **Strategy**: stacked
- **Started**: Fri Oct 31 00:00:07 UTC 2025

### Commits Included in This Merge
- **33d36dcc56** - feat(elasticsearch): support for composable index templates (#15089)
  *by Sergio Gómez Villamor on 2025-10-29*

- **6d4b41be32** - tests(cypress): optimize symmaryTab tests (#15138)
  *by v-tarasevich-blitz-brain on 2025-10-29*

- **b390dd4d7e** - feat(iam-setup): additional support for iam (#15143)
  *by david-leifker on 2025-10-29*

- **67753a24c3** - feat(graphql): Add comprehensive entity patching functionality (#14823)
  *by Brock Griffey on 2025-10-29*

- **eec2f305da** - fix(docs): fixed misprint in api comparison table, fixed table layout and columns (#14481)
  *by Taimuraz Tibilov on 2025-10-30*

- **b637df4f69** - fix(web): nested select component should not auto-clear selections (#15122)
  *by Jay on 2025-10-30*

- **07373259a4** - fix(quickstart): bump default version to v1.3.0 (#15155)
  *by Chakru on 2025-10-30*

- **ee6672f594** - fix(cypress): fix manage_tagsV2 (#15151)
  *by v-tarasevich-blitz-brain on 2025-10-30*

- **46d2063091** - fix(cypress): fix cypress test for dataset_health (#15153)
  *by Purnima Garg on 2025-10-30*

- **2bcbd76ede** - fix(cypress): fix ingestion_source cypress test (#15154)
  *by Purnima Garg on 2025-10-30*


## Upstream Merge Session - 2025-11-04 00:00:12

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-11-04-000011`
- **Merge Base**: `2bcbd76edef79525973b4b3af1ff15a20bf8830a`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 16
- **Strategy**: stacked
- **Started**: Tue Nov  4 00:00:12 UTC 2025

### Commits Included in This Merge
- **660a26bda1** - chore(): bump kubectl (#15157)
  *by david-leifker on 2025-10-30*

- **df6e6a94c2** - docs(search): improve docs for upstream and downstream advanced query (#15167)
  *by Aseem Bansal on 2025-10-31*

- **4e0b8b76d4** - feat(OpenAPI v3): Improve generic scroll API to have advanced pagination and facets (#14877)
  *by Jesse Jia on 2025-10-31*

- **8ee03b40b2** - fix(upgrade reindex): Ensure count uses all the restore job args to accurately estimate the… (#15094)
  *by Harsh Verma on 2025-10-31*

- **e7d71568a4** - fix(lineage): Use lineage viz methods when ignoreAsHops is specified (#15171)
  *by Andrew Sikowitz on 2025-10-31*

- **0203ab6fa7** - fix(build): regression in nuke task when running non-debug profiles (#15016)
  *by Chakru on 2025-11-03*

- **5ab063f68f** - feat(files) Add combined commits together for file upload/download (#15170)
  *by Chris Collins on 2025-11-03*

- **6eb27d625c** - fix(ci): cache gradle plugins (#15175)
  *by Chakru on 2025-11-03*

- **aacaa76e38** - feat(ui): Modernize Analytics Page with v2/Alchemy Components (#6872) (#15068)
  *by Chris Collins on 2025-11-03*

- **e1d7f118f8** - fix(tests): fix v2_edit_documentation (bring from SaaS) (#15165)
  *by v-tarasevich-blitz-brain on 2025-11-03*

- **07b8508b3d** - fix(tests): bring fixes for modules.js cypress tests from saas (#15081)
  *by v-tarasevich-blitz-brain on 2025-11-03*

- **8472531be9** - fix(ingest): Handle empty column names from Snowflake access history (#15106)
  *by kyungsoo-datahub on 2025-11-03*

- **523555167a** - feat(models): AssertionInfo with note (#15031)
  *by Jay on 2025-11-03*

- **e06d37e86e** - feat(pdl): Add Microsoft Teams as a new notification sink type (#15186)
  *by Anush Kumar on 2025-11-03*

- **41cb01c101** - docs(observe): add documentation for smart sql assertions (#15187)
  *by Peter Wang on 2025-11-03*

- **12dec48ce1** - fix(modules) Support unknown module types gracefully in the app (#15185)
  *by Chris Collins on 2025-11-03*


## Upstream Merge Session - 2025-11-05 00:00:14

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-11-05-000013`
- **Merge Base**: `12dec48ce17a339f682929e9152bdf0c4a9c6fd3`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 8
- **Strategy**: stacked
- **Started**: Wed Nov  5 00:00:14 UTC 2025

### Commits Included in This Merge
- **135786c1c3** - fix(ingestion): Add aws_common dependency to cockroachdb and mariadb plugins (#15189)
  *by Anush Kumar on 2025-11-03*

- **34abd003dd** - fix(ui/theme): Catch errors in loading custom theme (#15184)
  *by Andrew Sikowitz on 2025-11-03*

- **3e5e552cd9** - docs(observe): add documentation for custom sql in column value assertions (#15192)
  *by Peter Wang on 2025-11-04*

- **76ecfa6e8e** - feat(uploadFiles): bring changes from SaaS (#15166)
  *by v-tarasevich-blitz-brain on 2025-11-04*

- **3d7d7a6eed** - improvement(upload-files): bring back changes from SaaS (#15181)
  *by Purnima Garg on 2025-11-04*

- **b0673a67d1** - feat(ui/file-upload): show inline previews of text, pdf and video files (#15182)
  *by Purnima Garg on 2025-11-04*

- **f429c2a491** - test(uploadFile): add cypress tests (#15195)
  *by v-tarasevich-blitz-brain on 2025-11-04*

- **bf21758549** - improve(uploadFiles): move upload files related props to its own oject (#15179)
  *by v-tarasevich-blitz-brain on 2025-11-04*


## Upstream Merge Session - 2025-11-07 00:00:10

This section tracks the progress of merging upstream changes from `upstream/master` using stacked strategy.

### Merge Details
- **Source**: `upstream/master`
- **Target**: `oss-merge-2025-11-07-000009`
- **Merge Base**: `12dec48ce17a339f682929e9152bdf0c4a9c6fd3`
- **End Commit**: `upstream/master (latest)`
- **Total Commits**: 40
- **Strategy**: stacked
- **Started**: Fri Nov  7 00:00:10 UTC 2025

### Commits Included in This Merge
- **135786c1c3** - fix(ingestion): Add aws_common dependency to cockroachdb and mariadb plugins (#15189)
  *by Anush Kumar on 2025-11-03*

- **34abd003dd** - fix(ui/theme): Catch errors in loading custom theme (#15184)
  *by Andrew Sikowitz on 2025-11-03*

- **3e5e552cd9** - docs(observe): add documentation for custom sql in column value assertions (#15192)
  *by Peter Wang on 2025-11-04*

- **76ecfa6e8e** - feat(uploadFiles): bring changes from SaaS (#15166)
  *by v-tarasevich-blitz-brain on 2025-11-04*

- **3d7d7a6eed** - improvement(upload-files): bring back changes from SaaS (#15181)
  *by Purnima Garg on 2025-11-04*

- **b0673a67d1** - feat(ui/file-upload): show inline previews of text, pdf and video files (#15182)
  *by Purnima Garg on 2025-11-04*

- **f429c2a491** - test(uploadFile): add cypress tests (#15195)
  *by v-tarasevich-blitz-brain on 2025-11-04*

- **bf21758549** - improve(uploadFiles): move upload files related props to its own oject (#15179)
  *by v-tarasevich-blitz-brain on 2025-11-04*

- **1749d845ff** - feat(docs): Click to expand image (#15201)
  *by Adrian Machado on 2025-11-04*

- **4a210cd6d9** - feat(docs): Add docs for injecting custom instructions to Ask DataHub, Docs Gen, + Classification (#15197)
  *by John Joyce on 2025-11-04*

- **6048a8255e** - feat(unity/mlmodel): add model signature and run details support in Unity Catalog Source (#15177)
  *by Anush Kumar on 2025-11-04*

- **b66a2b3270** - fix(smoke-tests): fix some of the top failing cypress tests (#15199)
  *by ani-malgari on 2025-11-05*

- **680a1eeabf** - add readWrite documentation to be added to mongodb page (#15200)
  *by dgluong-datahub on 2025-11-05*

- **d90fce58f7** - fix(quickstart): limit old cli to 1.2 (#15208)
  *by Chakru on 2025-11-05*

- **f1c95c79d1** - chore: Remove Pydantic V1 deprecation warnings (#15057)
  *by Sergio Gómez Villamor on 2025-11-05*

- **7df902af09** - fix(cypress/summaryTab): split test into separated files (#15194)
  *by v-tarasevich-blitz-brain on 2025-11-05*

- **725ffecaac** - feat(datasetSummaryTab): add feature flag (#15206)
  *by v-tarasevich-blitz-brain on 2025-11-05*

- **842512af34** - docs(subscriptions): Update docs for subscriptions management (#15203)
  *by Adrian Machado on 2025-11-05*

- **ac1d9d47e1** - chore(docs): Clarify definition of "View Entity" and "View Entity Pag… (#15213)
  *by Abe on 2025-11-05*

- **4ae64377a4** - docs(release): Add release notes for version 0.3.15 (#15139)
  *by Jay on 2025-11-05*

- **d8166f2388** - docs(logical): Add docs on creating logical datasets and bulk relationship removal (#15029)
  *by Andrew Sikowitz on 2025-11-05*

- **8b51cee2ef** - Add feature guide for upload/download to s3 in documentation (#15168)
  *by Chris Collins on 2025-11-05*

- **0262d9717d** - hotfix(): Add new create domains flow  (#15202)
  *by John Joyce on 2025-11-05*

- **da2043b0fb** - fix(docs): change incorrect link to CF template (#15217)
  *by Kevin Karch on 2025-11-05*

- **67aed02165** - fix(ingest/airflow): Remove Python 2.9 airflow tests (#15216)
  *by Tamas Nemeth on 2025-11-06*

- **1c11ec033d** - bug(unity-catalog): update parameter key in UnityCatalogApiProxy for MLFlow Model files (#15220)
  *by Anush Kumar on 2025-11-05*

- **6f81e720aa** - fix(ui): Handle structured properties overflow and indentation (#15131)
  *by Saketh Varma on 2025-11-06*

- **7f25b6d250** - chore(ci): update gradle config, test infrastructure, and testng suit… (#15207)
  *by david-leifker on 2025-11-05*

- **71daaa9ac0** - Update announcement bar content to Context ON-DEMAND (#15221)
  *by Gray Ayer on 2025-11-06*

- **bdb46d9909** - feat(databricks): adds Azure oauth to Databricks (#15117)
  *by P Anshul Jain on 2025-11-06*

- **fe4e204a26** - feat(sdk): add Tag entity to SDK v2 (#14791)
  *by Sergio Gómez Villamor on 2025-11-06*

- **860303f24b** - feat(impact): allow partial results for backwards compat (#15198)
  *by david-leifker on 2025-11-06*

- **002de4bcd8** - feat(docker) Add quickstartDebugAws gradle command for testing s3 locally (#15218)
  *by Chris Collins on 2025-11-06*

- **7f6e7aa519** - improvement(ui/file-upload): handle file previews when there's no permission to view or error occurs (#15210)
  *by Purnima Garg on 2025-11-06*

- **064a70bbb9** - feat: bring back changes from proposals cypress PR to OSS (#15224)
  *by Purnima Garg on 2025-11-06*

- **dc4c2452c2** - fix(elasticsearch): fix index template (#15227)
  *by david-leifker on 2025-11-06*

- **65616dbceb** - fix(search): improve queries index write perf (#15226)
  *by david-leifker on 2025-11-06*

- **88eb03b57e** - feat(structured-properties): implement infinite scroll with backend search for structured properties table (#14991)
  *by Purnima Garg on 2025-11-07*

- **6cc0b08f3d** - chore(build): upgrade git-properties gradle plugin (#15229)
  *by Chakru on 2025-11-07*

- **8dd6b6b8f9** - Move Ask DataHub doc into "Features"  (#15228)
  *by John Joyce on 2025-11-06*

