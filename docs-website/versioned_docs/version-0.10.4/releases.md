---
title: DataHub Releases
sidebar_label: Releases
slug: /releases
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs-website/generateDocsDir.ts
---

# DataHub Releases

## Summary

| Version      | Release Date | Links                                                                                                        |
| ------------ | ------------ | ------------------------------------------------------------------------------------------------------------ |
| **v0.10.4**  | 2023-06-09   | [Release Notes](#v0-10-4), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.10.4) |
| **v0.10.3**  | 2023-05-25   | [Release Notes](#v0-10-3), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.10.3) |
| **v0.10.2**  | 2023-04-13   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.10.2)                            |
| **v0.10.1**  | 2023-03-23   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.10.1)                            |
| **v0.10.0**  | 2023-02-07   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.10.0)                            |
| **v0.9.6.1** | 2023-01-31   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.6.1)                           |
| **v0.9.6**   | 2023-01-13   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.6)                             |
| **v0.9.5**   | 2022-12-23   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.5)                             |
| **v0.9.4**   | 2022-12-20   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.4)                             |
| **v0.9.3**   | 2022-11-30   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.3)                             |
| **v0.9.2**   | 2022-11-04   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.2)                             |
| **v0.9.1**   | 2022-10-31   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.1)                             |
| **v0.9.0**   | 2022-10-11   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.9.0)                             |
| **v0.8.45**  | 2022-09-23   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.45)                            |
| **v0.8.44**  | 2022-09-01   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.44)                            |
| **v0.8.43**  | 2022-08-09   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.43)                            |
| **v0.8.42**  | 2022-08-03   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.42)                            |
| **v0.8.41**  | 2022-07-15   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.41)                            |
| **v0.8.40**  | 2022-06-30   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.40)                            |
| **v0.8.39**  | 2022-06-24   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.39)                            |
| **v0.8.38**  | 2022-06-09   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.38)                            |
| **v0.8.37**  | 2022-06-09   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.37)                            |
| **v0.8.36**  | 2022-06-02   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.36)                            |
| **v0.8.35**  | 2022-05-18   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.35)                            |
| **v0.8.34**  | 2022-05-04   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.34)                            |
| **v0.8.33**  | 2022-04-15   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.33)                            |
| **v0.8.32**  | 2022-04-04   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.32)                            |
| **v0.8.31**  | 2022-03-17   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.31)                            |
| **v0.8.30**  | 2022-03-17   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.30)                            |
| **v0.8.29**  | 2022-03-10   | [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.8.29)                            |

## [v0.10.4](https://github.com/datahub-project/datahub/releases/tag/v0.10.4) {#v0-10-4}

Released on 2023-06-09 by [@pedro93](https://github.com/pedro93).

### Release Highlights

##### User Experience

- You can now create and assign Custom Ownership types within DataHub; plus, we now display the owner type on an Entity Page
  &lt;img width="336" alt="ownershiptype-displayed" src="https://github.com/datahub-project/datahub/assets/15873986/3bd84ef5-0860-4dfb-8670-b23857c6d6e0">

- Various bug fixes to Column Level Lineage visualization

##### Metadata ingestion

- You can now define column-level lineage (aka fine-grained lineage) via our file-based lineage source
- Looker: Ingest Looks that are not part of a Dashboard
- Glue: Error reporting now includes lineage failures
- BigQuery: Now support deduplicating LogEntries based on insertId, timestamp, and logName

##### Docs

- CSV Enricher: improvements to sample CSV and recipe
- Guide for changing default DataHub credentials
- Updated guide to apply time-based filters on Lineage

#### What's Changed

- ci(ingest/kafka): improve kafka integration test reliability by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8085
- fix(ingest/bigquery): Deduplicate LogEntries based on insertId, timestamp, logName by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8132
- feat(ingest/glue): report glue job lineage failures, update doc by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8126
- feat(lineage source): add fine grained lineage support by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/7904
- docs(glue): fix broken link by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8135
- feat(custom ownership): Adds Custom ownership types as a top level entity by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/8045
- Update updating-datahub.md for v0.10.3 release by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8139
- feat: add dbt-athena adapter support for column types mapping by [@svdimchenko](https://github.com/svdimchenko) in https://github.com/datahub-project/datahub/pull/8116
- docs(csv-enricher): add example csv file & recipe by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/8141
- chore(ci): update base requirements file by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8144
- fix(ingest/s3): Path spec aware folder traversal by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8095
- fix(ui) Fix selecting columns in Lineage tab for CLL by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/8129
- feat(search): adding support for `_entityType` filter in the application layer + frontend by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/8102
- docs(ingest/nifi): fix broken links by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8143
- fix(scroll): fix scroll cache key for hazelcast by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/8149
- chore(json): fix json vulnerability by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/8150
- fix(ingest/json-schema): handle property inheritance in unions by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8121
- chore(log): fix log as error instead of info by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8146
- fix(lineagecounts) Include entities that are filtered out due to sibling logic in the filtered count of lineage counts by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8152
- fix(stats): display consistent query count on stats tab by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/8151
- fix(ingest): remove `original_table_name` logic in sql source by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8130
- feat(ingest): add more fail-safes to stateful ingestion by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8111
- feat(ingest/snowflake): support for more operation types by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8158
- fix(ui) Show Entities first on Domain pages again by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/8159
- fix(ingest/nifi): allow nifi site url with context path by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8156
- feat(ingest): Create Browse Paths V2 under flag by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8120
- fix(ingestion/looker): set project-name for imported_projects views by [@mohdsiddique](https://github.com/mohdsiddique) in https://github.com/datahub-project/datahub/pull/8086
- fix(docs): Fix ownership type typos by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/8155
- docs(townhall) feb and march town hall agenda and recording by [@maggiehays](https://github.com/maggiehays) in https://github.com/datahub-project/datahub/pull/7676
- feat(ingest/unity): Add qualified name to dataset properties by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8164
- feat(ingest/bigquery_v2): enable platform instance using project id by [@Khurzak](https://github.com/Khurzak) in https://github.com/datahub-project/datahub/pull/8142
- feat(ingest/snowflake): Deprecate legacy lineage and optimize query history joins by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8176
- fix(ingest/kafka): Fixing error printing in Kafka properties get call by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8145
- fix(ingest/snowflake): set use_quoted_name to profile lowercase tables by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8168
- feat(classification): support for regex based custom infotypes by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8177
- fix(restli): update base client retry logic by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8172
- fix(ingest): Fix modeldocgen; bump feast to relax pyarrow constraint by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8178
- refactor(ci): move from sleep to kafka lag based testing by [@shirshanka](https://github.com/shirshanka) in https://github.com/datahub-project/datahub/pull/8094
- docs(lineage): document timestamp filtering in lineage feature by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8174
- build(ingest/feast): Pin feast to minor version by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8180
- feat(ingest/snowflake): Okta OAuth support; update docs by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8157
- feat(ingest/presto-on-hive): add support for extra properties and merge property capabilities by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8147
- docs(managed datahub): release notes for v0.2.8 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8185
- fix(nocode): fix DeleteLegacyGraphRelationshipsStep for Elasticsearch by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8181
- feat(docker):Add the jattach tool to the docker container([#7538](https://github.com/datahub-project/datahub/pull/7538)) by [@yangjiandan](https://github.com/yangjiandan) in https://github.com/datahub-project/datahub/pull/8040
- refactor: Return original exception as caused by by [@Jorricks](https://github.com/Jorricks) in https://github.com/datahub-project/datahub/pull/7722
- docs(ingest) Add MetadataChangeProposalWrapper import to example code by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8175
- fix(ingest/kafka): Better error handling around topic and topic description extraction by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8183
- fix(vulnerabilities)/vulnerabilities_fixes_datahub ([#8075](https://github.com/datahub-project/datahub/pull/8075)) by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8189
- fix: add dedicated guide on changing default credentials by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/8153
- feat(classification): configurable minimum values threshold by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8186
- fix(ingestion/looker): ingest looks not part of dashboard by [@mohdsiddique](https://github.com/mohdsiddique) in https://github.com/datahub-project/datahub/pull/8140
- fix(ingest/profiling): only apply monkeypatches once when profiling by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8160
- docs(tableau): site config is required for tableau cloud / tableau online by [@mohdsiddique](https://github.com/mohdsiddique) in https://github.com/datahub-project/datahub/pull/8041
- fix(ingest/bigquery): Swap log order to avoid confusion by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8197
- fix(ingest/redshift): Adding env parameter where it was missing for urn generation by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8199
- revert(ingest/bigquery): Do not emit DataPlatformInstance; remove references to platform_instance by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8196
- docs(managed datahub): add docs link to v0.2.8 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8202
- Add combined health check endpoint which can check multiple components by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8191
- chore(cp-schema-registry): bump minor version by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8192
- feat(ingest): Produce browse paths v2 on demand and with platform instance by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8173

#### New Contributors

- [@svdimchenko](https://github.com/svdimchenko) made their first contribution in https://github.com/datahub-project/datahub/pull/8116
- [@Khurzak](https://github.com/Khurzak) made their first contribution in https://github.com/datahub-project/datahub/pull/8142
- [@Jorricks](https://github.com/Jorricks) made their first contribution in https://github.com/datahub-project/datahub/pull/7722

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v0.10.3...v0.10.4

## [v0.10.3](https://github.com/datahub-project/datahub/releases/tag/v0.10.3) {#v0-10-3}

Released on 2023-05-25 by [@iprentic](https://github.com/iprentic).

### Release Highlights

##### User Experience

- Define Data Products via YAML and manage associated entities within a Domain
- Search experience: quickly apply a filter at time of search
- Form-based PowerBI ingestion

##### Developer Experience

- Progress toward Removing Confluent Schema Registry requirement -- Helm & Quickstart simplifications to follow
  - NOTE: this will only work for **new** deployments of DataHub; If you have already deployed DataHub with Confluent Schema Registry, you will not be able to disable it
- Delete CLI - correctly handles deleting timeseries aspects
- Ongoing improvements to Quickstart stability
- Support entity types filter in `get_urns_by_filter`
- Search customization
  - regex based query matching
  - full control over scoring functions (useable on any document field, i.e. tags, deprecated flags, etc)
  - enable/disable fuzzy, prefix, exact match queries

##### Ingestion

- BigQuery - Improve ingestion disk usage & speed; extract dataset usage from Views
- Unity Catalog - Capture create/last modified timestamps; extract usage; data profiling support
- PowerBI - Update workspace concept mapping; support `modified_since`, `extract_dataset_schema`, and more
- Superset – support stateful ingestion
- Business Glossary – Simplify ingestion source
- Kafka – Add description in dataset properties
- S3 – Support stateful ingestion & `last_updated`
- CSV Enricher – Support updating more types
- PII Classification - Configurable sample size
- Nifi - Support Kerberos authentication

#### What's Changed

- fix(ingest/bigquery): Add to lineage, not overwrite, when using sql parser by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7814
- fix(ingest/bigquery): Enable lineage and usage ingestion without tables by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7820
- fix(ingest/bigquery): Do not query columns when not ingesting tables or views by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7823
- fix(ingest/bigquery): update usage query, remove erroneous init by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7811
- fix(ingest/bigquery): Handle null values from usage aggregation by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7827
- perf(ingest/bigquery): Improve bigquery usage disk usage and speed by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7825
- fix(cli): use correct ingestion image in script by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7826
- fix(release): prevent republish of images on release edits by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7828
- feat(): finish populating the entity registry by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7818
- fix(ui) Fix 404 page routing bug by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7824
- feat(ui): Support PowerBI Ingestion via UI form by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/7817
- fix(ingest/snowflake): fix column name in snowflake optimised lineage by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7834
- feat(ingest/unity): capture create/lastModified timestamps by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7819
- fix(test): fix spark lineage test by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7829
- docs(): add markprompt help chat by [@jeffmerrick](https://github.com/jeffmerrick) in https://github.com/datahub-project/datahub/pull/7837
- Update DataJobInputOutput.pdl to express that CLL fields are not shown in the UI right now by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/7830
- feat(cli): improve quickstart stability by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7839
- chore(ci): regular upgrade base requirements.txt by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/7821
- feat(timeseries): Support sorting timeseries aspects by non-timestampMillis field + fix operations resolver by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/7840
- doc(ingestion/tableau): Fix rendering ingestion quickstart guide by [@mohdsiddique](https://github.com/mohdsiddique) in https://github.com/datahub-project/datahub/pull/7808
- fix(ingest): pin sqlparse version by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7847
- feat(urn): Add a validator when creating an URN that it is no longer than the li… by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/7836
- chore(ingest): bug fix in sqlparse pin by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7848
- feat: enriching guide on creating dataset by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/7777
- feat(docs): consolidate api guides by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/7857
- fix(ingest/salesforce): use report timestamp for operations by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7838
- chore(ci): fix CI failing due to lint by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/7863
- fix(mcl): fix improper pass by reference by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7860
- feat(urn) Add validator to reject URNs which contain the character we plan to u… by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/7859
- feat(elasticsearch): Add servlet which provides an endpoint for a healthcheck on the ES cl… by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/7799
- fix(ui) Add UI fixes and design tweaks to AutoComplete by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7845
- fix(ui) Get all entity assertions in chrome extension by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7849
- refactor(platform): Refactoring ES Utils, adding EXISTS condition support to Filter Criterion by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/7832
- chore(ui): change background color to transparent for avatar with photoUrl by [@hieunt-itfoss](https://github.com/hieunt-itfoss) in https://github.com/datahub-project/datahub/pull/7527
- refactor(ingest): Add helper DataHubGraph methods by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7851
- fix(ui) Disable cache on Domain and Glossary Related Entities pages by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7867
- fix(cache): Fix cache key serialization in search service by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/7858
- docs(ingest): update dbt and aws docs by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7870
- docs(ingest): fix CorpGroup example by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7816
- docs(ingest/powerbi): update workspace concept mapping by [@eeepmb](https://github.com/eeepmb) in https://github.com/datahub-project/datahub/pull/7835
- feat(ingest/powerbi): support modified_since, extract_dataset_schema and many more by [@aezomz](https://github.com/aezomz) in https://github.com/datahub-project/datahub/pull/7519
- Remove usages of commons-text library lower than 1.10.0 by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/7850
- feat(glue): allow resource links to be ignored by [@YusufMahtab](https://github.com/YusufMahtab) in https://github.com/datahub-project/datahub/pull/7639
- feat(ingestion): lookml refinement support by [@mohdsiddique](https://github.com/mohdsiddique) in https://github.com/datahub-project/datahub/pull/7781
- feat(ingest/unity): Ingest ownership for containers; lookup service principal display names by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7869
- Logging and test models fixes by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7884
- feat(model) Add ContainerPath aspect model by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7774
- bug(7882): run kafka-configs.sh on DataHubUpgradeHistory_v1 to make sure the retention.ms is set to infinite by [@jinlintt](https://github.com/jinlintt) in https://github.com/datahub-project/datahub/pull/7883
- fix: refactor toc by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/7862
- feat(cli): Modifies ingest-sample-data command to use DataHub url & token based on config by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/7896
- feat(ingest/snowflake): optionally emit all upstreams irrespective of recipe pattern by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7842
- fix(ingestion/tableau): backward compatibility with version 2021.1 an… by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7864
- fix(ingest/dbt): ensure dbt shows view properties by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7872
- docs(airflow): add debug guide on url generation by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7885
- feat(sdk): support entity types filter in `get_urns_by_filter` by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7902
- fix(ingest/snowflake): fix optimised lineage query, filter temporary … by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7894
- fix(ingest/bigquery): fix handling of time decorator offset queries by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7843
- fix(ingest): fix minor bug + protective dep requirements by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7861
- fix(cli): remove duplicate labels from quickstart files by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7886
- Revert "feat(cli): Modifies ingest-sample-data command to use DataHub… by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/7899
- feat(sdk): add `DataHubGraph.get_entity_semityped` method by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7905
- test(ingest/biz-glossary): add test for enable_auto_id by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7911
- feat(ingest): add GCS ingestion source by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7903
- [bugfix] Fix remote file ingestion for Windows by [@xiphl](https://github.com/xiphl) in https://github.com/datahub-project/datahub/pull/7888
- refactor(ingest): report soft deleted stale entities with LossyList by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7907
- fix(platforms): fix json parse exception for data platforms by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7918
- docs(release): managed DataHub 0.2.6 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/7922
- fix(deploy): add missing plugin files for mysql-client library in mysql-setup by [@AndrewZures](https://github.com/AndrewZures) in https://github.com/datahub-project/datahub/pull/7909
- docs(deploy): document some of the environment variables by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7906
- fix(system-update): fix no wait flag by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7927
- fix(consumer): fix datahub usage event topic consumer by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7866
- logging(auth): adding optional logging to authentication exceptions by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7929
- feat(search): enable search initial customization by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7901
- feat(schema-registry): replace confluent schema registry by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7930
- feat(ingest/unity): Add usage extraction; add TableReference by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7910
- fix(ingest/unity-catalog): Add usage_common dependency to unity catalog plugin by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7935
- feat(search): add filter for specific entities by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/7919
- fix(ingest/unity): Add sqllineage dependency by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7938
- fix(ingest/hive): fix containers generation for hive by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7926
- docs(ingest): add note about path_specs configuration in data lake sources by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7941
- feat: add missing python sdk guides based on DatahubGraph by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/7875
- fix(ingest/unity): use fully qualified catalog/schema patterns by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7900
- feat(airflow): respect port parameter if provided by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7945
- fix(ingest): improve error message when graph connection fails by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7946
- fix(docs): Adding relationship types section to Business Glossary docs by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/7949
- docs(ingest): update max_threads default value by [@felipeac](https://github.com/felipeac) in https://github.com/datahub-project/datahub/pull/7947
- fix(ui) Fix Tag Details button to use url encoding by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7948
- docs: amend italic formatting by [@HansBambel](https://github.com/HansBambel) in https://github.com/datahub-project/datahub/pull/7893
- fix(ldap): properly handle escaped characters in LDAP DNs by [@Reilman79](https://github.com/Reilman79) in https://github.com/datahub-project/datahub/pull/7928
- docs(ingest/postgres): add example with ssl configuration by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7916
- refactor(ingest/biz-glossary): simplify business glossary source by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7912
- fix: Fix broken links on PowerBI by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/7959
- feat(model) Update aspect containerPath -> browsePathsV2 by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7942
- fix(ui) Fix displaying column level lineage for sibling nodes by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7955
- fix(ingest/bigquery): Filter projects for lineage and usage by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7954
- feat(tracking) Add tracking events to our chrome extension page by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7967
- fix(search): Handle .keyword properly in the entity type query to ind… by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/7957
- feat(es) Store and map containerPath to elastic search properly by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7898
- fix: build vercel python from source by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7972
- feat(models): Make assets searchable by their external URLs by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/7953
- fix(ingest/salesforce): support JSON web token auth by [@matthew-piatkus-cko](https://github.com/matthew-piatkus-cko) in https://github.com/datahub-project/datahub/pull/7963
- fix(SearchBar): Restore explore all link by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/7973
- fix(ingest/tableau): Add a try catch to LineageRunner parser by [@maaaikoool](https://github.com/maaaikoool) in https://github.com/datahub-project/datahub/pull/7965
- fix(ingest/salesforce): fix lint by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7980
- fix(ingest): use certs correctly in rest emitter by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7978
- fix(ingestion/redshift) - Fixing schema query by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/7975
- chore(log): change sout to log by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/7931
- fix(ingest/redshift): Enabling autocommit for Redshift connection by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/7983
- fix(ingest): use with for opened connections by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7908
- fix(ingest/unity): improve error message if no scheme in workspace_url by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7951
- fix(download as csv): Support download to csv for impact analysis tab by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/7956
- docs(development): update per feedback from community by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7958
- fix(ingest/bigquery): remove incorrectly used table_pattern filter by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7810
- feat(snowflake): add config option to specify deny patterns for upstreams by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7962
- fix(docker-compose): make startup more robust with deterministic services' dependencies by [@gcernier-semarchy](https://github.com/gcernier-semarchy) in https://github.com/datahub-project/datahub/pull/7880
- fix(cache): update search cache when skipped, but enabled by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7936
- feat(telemetry): add server version by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7979
- docs: add tips on language switchable tap on docs by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/7984
- fix(privileges) Use glossary term manage children privileges for edit docs and links by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7985
- fix(ingest/postgres): Allow specification of initial engine database; set default database to postgres by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7915
- refactor(ingest/unity): Use databricks-sdk over databricks-cli for usage query by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7981
- chore: cleanup some devtool console warnings by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/7988
- feat(search): support only searching by quick filter by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/7997
- feat(docs): Add cli documentation on how to add custom platforms by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/7993
- fix(search): fix custom search config parsing by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8010
- fix(auth): guards against creating a user for the system actor by [@aditya-radhakrishnan](https://github.com/aditya-radhakrishnan) in https://github.com/datahub-project/datahub/pull/7996
- chore(security): update org json json dependency - cve-2022-45688 by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7991
- feat(metrics): add metrics for upgrade steps by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/7992
- feat(models): Adding searchable for chart and dashboard url by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/8002
- feat(ingest/s3): Inferring schema from the alphabetically last folder by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8005
- feat(ingest/classification): add classification report by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7925
- docs(managed datahub): release notes for v0.2.7 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8020
- fix(ui ingest): Fix mapping for token_name, token_value form fields for Tableau by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/8018
- fix(ui): add loading indicator for download as CSV action by [@aditya-radhakrishnan](https://github.com/aditya-radhakrishnan) in https://github.com/datahub-project/datahub/pull/8003
- fix(ingest/snowflake): fix lineage query aggregation for optimised li… by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8011
- feat(ingest/unity): Add profiling support by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/7976
- feat(docs): Add example documentation for scrollAcrossEntities by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/8014
- fix(ingest/unity): Update databricks-cli pin by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8024
- fix(ingest/s3) Adding missing more-itertools dependency by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8023
- feat(cli): move registry delete to separate subcommand by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7968
- fix(sdk): throw errors on empty gms server urls by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8017
- feat(ingest/superset): add stateful ingestion by [@cccs-Dustin](https://github.com/cccs-Dustin) in https://github.com/datahub-project/datahub/pull/8013
- Gitignor'ing generated binary files in OSS by [@meyerkev](https://github.com/meyerkev) in https://github.com/datahub-project/datahub/pull/8031
- fix(PFP-260): Upgrading sqlite to fix SQLITE-449762 by [@meyerkev](https://github.com/meyerkev) in https://github.com/datahub-project/datahub/pull/8032
- feat(ingest): support importing local modules by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8026
- fix(timeline-events): fix NPE in timeline events by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8038
- fix(posts): fix formatting for posts where the title can get cut off by [@aditya-radhakrishnan](https://github.com/aditya-radhakrishnan) in https://github.com/datahub-project/datahub/pull/8001
- fix(ingestion/metabase): metabase connector bigquery lineage fix by [@shubhamjagtap639](https://github.com/shubhamjagtap639) in https://github.com/datahub-project/datahub/pull/8042
- fix(es) Fix browseV2 index mappings by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/8034
- fix(search): enter key with no query should search all by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/8036
- feat(ingest): Allow csv-enricher to update more types by [@xiphl](https://github.com/xiphl) in https://github.com/datahub-project/datahub/pull/7932
- fix(search): only show explore all btn on search and home by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/8047
- fix(ingest/dbt): fix dbt subtypes for sources by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8048
- fix(ingest/bigquery): update usage audit log query to include create/… by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/7995
- feat(docs): add guide on integration ML system via SDKs by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/8029
- refactor(ingest): Make get_workunits() return MetadataWorkUnits by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8051
- refractor(classification): simplify classification handler by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8056
- feat: Add support for Data Products by [@shirshanka](https://github.com/shirshanka) in https://github.com/datahub-project/datahub/pull/8039
- fix(build): fix lint issue by [@shirshanka](https://github.com/shirshanka) in https://github.com/datahub-project/datahub/pull/8066
- feat(system-update): remove datahub-update requirement on schema reg by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/7999
- fix(gitignore): update gitignore for generated files by [@minjin0121](https://github.com/minjin0121) in https://github.com/datahub-project/datahub/pull/7940
- feat(ingestion/kafka): add description in dataset properties by [@shubhamjagtap639](https://github.com/shubhamjagtap639) in https://github.com/datahub-project/datahub/pull/7974
- fix(ingestion/tableau): ingest parent project name in container properties by [@mohdsiddique](https://github.com/mohdsiddique) in https://github.com/datahub-project/datahub/pull/8030
- refactor(ingest): Move source_helpers.py from datahub/utilities -> datahub/api by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8052
- fix(ingest/snowflake): lowercase user urn when using email by [@matwalk](https://github.com/matwalk) in https://github.com/datahub-project/datahub/pull/7767
- fix(ingest/tableau): don't use unsupported sql condition field by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8065
- fix(ingest/looker): don't prematurely show connectivity success by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8070
- feat(web): update AWS logos by [@rinzool](https://github.com/rinzool) in https://github.com/datahub-project/datahub/pull/8057
- fix(metadata-io): remove assert in favor of exceptions by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/8035
- feat: add docs on column-level linage by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/8062
- ci: prevent qodana from using all of our cache by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8054
- ci(ingest/clickhouse): don't use kernel ephemeral ports by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8060
- test(sdk): better error messages in registry codegen test by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8081
- doc(managed datahub): update release notes for 0.2.7 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8088
- feat(ingest/s3) - Stateful ingestion and last-updated support by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8022
- docs(ingest/snowflake): fix authentication type docs by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8059
- fix(ingest/s3_data_lake)\_ingestor_skips_directories_with_similar_prefix by [@alplatonov](https://github.com/alplatonov) in https://github.com/datahub-project/datahub/pull/8078
- fix(ui) Fix entity name styling to show deprecation and others properly by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/8084
- test(sdk): move cli tests into the unit dir by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8028
- feat(sdk): better auth error messages in the rest emitter by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8025
- feat(caching): skip cache on ownership tabs by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/8082
- feat(embed): embed lookup route by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/8033
- fix(ingest/delta-lake): Walk through directory structure with full path; reduce resource creation by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8072
- feat(search): Add AggregateAcrossEntities endpoint by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8000
- chore(vulnerability): add exclusions for json to prevent leaking dependency by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/8090
- fix(ingestion/powerbi): skip erroneous pages of a report by [@shubhamjagtap639](https://github.com/shubhamjagtap639) in https://github.com/datahub-project/datahub/pull/8021
- feat(docs): Update markprompt by [@jeffmerrick](https://github.com/jeffmerrick) in https://github.com/datahub-project/datahub/pull/8079
- feat(images): Add build processes for arm64v8 image variants by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/7990
- feat(ingest): add `env` to container properties by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8027
- fix(checkstyle): Fix checkstyle violations to turn master green by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8099
- doc(auth): fixes doc in DataHubSystemAuthenticator.java by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/8071
- refactor(ingest): Auto report workunits by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8061
- feat(cli): support `datahub ingest mcps` by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/7871
- feat: datahub-upgrade.sh to support old versions by [@ollisala](https://github.com/ollisala) in https://github.com/datahub-project/datahub/pull/7891
- feat(ingest/s3): type aware directory sorting by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8089
- fix(ci): add missing updates to restli-spec by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/8106
- fix(ingest/build): setting typing extension &lt;4.6.0 because it breaks tests by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8108
- fix(upgrade): removes sleep from bootstrap process by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/8016
- fix(jackson): increase max serialized string length default by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/8053
- fix(ui): SchemaDescriptionField 'read-more' doesn't affect table height by [@jfrancos-mai](https://github.com/jfrancos-mai) in https://github.com/datahub-project/datahub/pull/7970
- fix(ingest): emitter bug fixes by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8093
- fix(sample data): Update timestamps in bootstrap_mce.json to more recent by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8103
- feat(ui) Add readOnly flag that disables profile URL editing by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/8067
- feat(cli): delete cli v2 by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8068
- refactor(ingest): simplify stateful ingestion provider interface by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/8104
- Update updating-datahub.md with breaking changes by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/7964
- feat(ui) Show documentation on Domain pages first by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/8110
- docs(readme): adds PITS Global Data Recovery Services to the adopters list by [@pheianox](https://github.com/pheianox) in https://github.com/datahub-project/datahub/pull/8080
- fix(ingest/redshift): Making Redshift source more verbose by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8109
- feat(ingest): Browse Path v2 helper by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8012
- feat(classification): configurable sample size by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8096
- fix logic for multiple entities found and clean up messy code by [@joshuaeilers](https://github.com/joshuaeilers) in https://github.com/datahub-project/datahub/pull/8113
- fix(search): Update _entityType transform logic to work for entities containing _ by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8112
- feat(ingest/bigquery): usage for views by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8046
- fix(ui): Open mailto link in new tab by [@jfrancos-mai](https://github.com/jfrancos-mai) in https://github.com/datahub-project/datahub/pull/7982
- fix(search): Transform \_entityType/index output for scroll across entities as well by [@iprentic](https://github.com/iprentic) in https://github.com/datahub-project/datahub/pull/8117
- feat(ingest): Add GenericAspectTransformer by [@amanda-her](https://github.com/amanda-her) in https://github.com/datahub-project/datahub/pull/7994
- refactor(ingest): Call source_helpers via new WorkUnitProcessors in base Source by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8101
- feat(ingest/nifi): kerberos authentication by [@mayurinehate](https://github.com/mayurinehate) in https://github.com/datahub-project/datahub/pull/8097
- fix(ingest/redshift):fixing schema filter by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8119
- feat(ingest/unity): Allow ingestion without metastore admin role by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8091
- feat(ingest/bigquery): Add BigQuery Views lineage extraction from Google Data Catalog API by [@viniciusdsmello](https://github.com/viniciusdsmello) in https://github.com/datahub-project/datahub/pull/8100
- fix(ingest/redshift): Fixing Redshift subtypes by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/8125
- fix(ingest): Fix breaking smoke test on stateful ingestion by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/8128

#### New Contributors

- [@eeepmb](https://github.com/eeepmb) made their first contribution in https://github.com/datahub-project/datahub/pull/7835
- [@YusufMahtab](https://github.com/YusufMahtab) made their first contribution in https://github.com/datahub-project/datahub/pull/7639
- [@AndrewZures](https://github.com/AndrewZures) made their first contribution in https://github.com/datahub-project/datahub/pull/7909
- [@HansBambel](https://github.com/HansBambel) made their first contribution in https://github.com/datahub-project/datahub/pull/7893
- [@matthew-piatkus-cko](https://github.com/matthew-piatkus-cko) made their first contribution in https://github.com/datahub-project/datahub/pull/7963
- [@joshuaeilers](https://github.com/joshuaeilers) made their first contribution in https://github.com/datahub-project/datahub/pull/7973
- [@gcernier-semarchy](https://github.com/gcernier-semarchy) made their first contribution in https://github.com/datahub-project/datahub/pull/7880
- [@shubhamjagtap639](https://github.com/shubhamjagtap639) made their first contribution in https://github.com/datahub-project/datahub/pull/8042
- [@minjin0121](https://github.com/minjin0121) made their first contribution in https://github.com/datahub-project/datahub/pull/7940
- [@matwalk](https://github.com/matwalk) made their first contribution in https://github.com/datahub-project/datahub/pull/7767
- [@rinzool](https://github.com/rinzool) made their first contribution in https://github.com/datahub-project/datahub/pull/8057
- [@alplatonov](https://github.com/alplatonov) made their first contribution in https://github.com/datahub-project/datahub/pull/8078
- [@ollisala](https://github.com/ollisala) made their first contribution in https://github.com/datahub-project/datahub/pull/7891
- [@jfrancos-mai](https://github.com/jfrancos-mai) made their first contribution in https://github.com/datahub-project/datahub/pull/7970
- [@pheianox](https://github.com/pheianox) made their first contribution in https://github.com/datahub-project/datahub/pull/8080

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v0.10.2...v0.10.3

## [DataHub v0.10.2](https://github.com/datahub-project/datahub/releases/tag/v0.10.2) {#v0-10-2}

Released on 2023-04-13 by [@iprentic](https://github.com/iprentic).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.10.2) for DataHub v0.10.2 on GitHub.

## [DataHub v0.10.1](https://github.com/datahub-project/datahub/releases/tag/v0.10.1) {#v0-10-1}

Released on 2023-03-23 by [@aditya-radhakrishnan](https://github.com/aditya-radhakrishnan).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.10.1) for DataHub v0.10.1 on GitHub.

## [DataHub v0.10.0](https://github.com/datahub-project/datahub/releases/tag/v0.10.0) {#v0-10-0}

Released on 2023-02-07 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.10.0) for DataHub v0.10.0 on GitHub.

## [DataHub v0.9.6.1](https://github.com/datahub-project/datahub/releases/tag/v0.9.6.1) {#v0-9-6-1}

Released on 2023-01-31 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.6.1) for DataHub v0.9.6.1 on GitHub.

## [DataHub v0.9.6](https://github.com/datahub-project/datahub/releases/tag/v0.9.6) {#v0-9-6}

Released on 2023-01-13 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.6) for DataHub v0.9.6 on GitHub.

## [DataHub v0.9.5](https://github.com/datahub-project/datahub/releases/tag/v0.9.5) {#v0-9-5}

Released on 2022-12-23 by [@jjoyce0510](https://github.com/jjoyce0510).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.5) for DataHub v0.9.5 on GitHub.

## [[Known Issues] DataHub v0.9.4](https://github.com/datahub-project/datahub/releases/tag/v0.9.4) {#v0-9-4}

Released on 2022-12-20 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.4) for [Known Issues] DataHub v0.9.4 on GitHub.

## [DataHub v0.9.3](https://github.com/datahub-project/datahub/releases/tag/v0.9.3) {#v0-9-3}

Released on 2022-11-30 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.3) for DataHub v0.9.3 on GitHub.

## [DataHub v0.9.2](https://github.com/datahub-project/datahub/releases/tag/v0.9.2) {#v0-9-2}

Released on 2022-11-04 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.2) for DataHub v0.9.2 on GitHub.

## [DataHub v0.9.1](https://github.com/datahub-project/datahub/releases/tag/v0.9.1) {#v0-9-1}

Released on 2022-10-31 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.1) for DataHub v0.9.1 on GitHub.

## [DataHub v0.9.0](https://github.com/datahub-project/datahub/releases/tag/v0.9.0) {#v0-9-0}

Released on 2022-10-11 by [@szalai1](https://github.com/szalai1).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.9.0) for DataHub v0.9.0 on GitHub.

## [DataHub v0.8.45](https://github.com/datahub-project/datahub/releases/tag/v0.8.45) {#v0-8-45}

Released on 2022-09-23 by [@gabe-lyons](https://github.com/gabe-lyons).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.45) for DataHub v0.8.45 on GitHub.

## [DataHub v0.8.44](https://github.com/datahub-project/datahub/releases/tag/v0.8.44) {#v0-8-44}

Released on 2022-09-01 by [@jjoyce0510](https://github.com/jjoyce0510).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.44) for DataHub v0.8.44 on GitHub.

## [DataHub v0.8.43](https://github.com/datahub-project/datahub/releases/tag/v0.8.43) {#v0-8-43}

Released on 2022-08-09 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.43) for DataHub v0.8.43 on GitHub.

## [v0.8.42](https://github.com/datahub-project/datahub/releases/tag/v0.8.42) {#v0-8-42}

Released on 2022-08-03 by [@gabe-lyons](https://github.com/gabe-lyons).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.42) for v0.8.42 on GitHub.

## [v0.8.41](https://github.com/datahub-project/datahub/releases/tag/v0.8.41) {#v0-8-41}

Released on 2022-07-15 by [@anshbansal](https://github.com/anshbansal).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.41) for v0.8.41 on GitHub.

## [v0.8.40](https://github.com/datahub-project/datahub/releases/tag/v0.8.40) {#v0-8-40}

Released on 2022-06-30 by [@gabe-lyons](https://github.com/gabe-lyons).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.40) for v0.8.40 on GitHub.

## [v0.8.39](https://github.com/datahub-project/datahub/releases/tag/v0.8.39) {#v0-8-39}

Released on 2022-06-24 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.39) for v0.8.39 on GitHub.

## [[!] DataHub v0.8.38](https://github.com/datahub-project/datahub/releases/tag/v0.8.38) {#v0-8-38}

Released on 2022-06-09 by [@jjoyce0510](https://github.com/jjoyce0510).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.38) for [!] DataHub v0.8.38 on GitHub.

## [[!] DataHub v0.8.37](https://github.com/datahub-project/datahub/releases/tag/v0.8.37) {#v0-8-37}

Released on 2022-06-09 by [@jjoyce0510](https://github.com/jjoyce0510).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.37) for [!] DataHub v0.8.37 on GitHub.

## [DataHub V0.8.36](https://github.com/datahub-project/datahub/releases/tag/v0.8.36) {#v0-8-36}

Released on 2022-06-02 by [@treff7es](https://github.com/treff7es).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.36) for DataHub V0.8.36 on GitHub.

## [[!] DataHub v0.8.35](https://github.com/datahub-project/datahub/releases/tag/v0.8.35) {#v0-8-35}

Released on 2022-05-18 by [@dexter-mh-lee](https://github.com/dexter-mh-lee).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.35) for [!] DataHub v0.8.35 on GitHub.

## [v0.8.34](https://github.com/datahub-project/datahub/releases/tag/v0.8.34) {#v0-8-34}

Released on 2022-05-04 by [@maggiehays](https://github.com/maggiehays).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.34) for v0.8.34 on GitHub.

## [DataHub v0.8.33](https://github.com/datahub-project/datahub/releases/tag/v0.8.33) {#v0-8-33}

Released on 2022-04-15 by [@dexter-mh-lee](https://github.com/dexter-mh-lee).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.33) for DataHub v0.8.33 on GitHub.

## [DataHub v0.8.32](https://github.com/datahub-project/datahub/releases/tag/v0.8.32) {#v0-8-32}

Released on 2022-04-04 by [@dexter-mh-lee](https://github.com/dexter-mh-lee).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.32) for DataHub v0.8.32 on GitHub.

## [DataHub v0.8.31](https://github.com/datahub-project/datahub/releases/tag/v0.8.31) {#v0-8-31}

Released on 2022-03-17 by [@dexter-mh-lee](https://github.com/dexter-mh-lee).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.31) for DataHub v0.8.31 on GitHub.

## [Datahub v0.8.30](https://github.com/datahub-project/datahub/releases/tag/v0.8.30) {#v0-8-30}

Released on 2022-03-17 by [@rslanka](https://github.com/rslanka).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.30) for Datahub v0.8.30 on GitHub.

## [DataHub v0.8.29](https://github.com/datahub-project/datahub/releases/tag/v0.8.29) {#v0-8-29}

Released on 2022-03-10 by [@shirshanka](https://github.com/shirshanka).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.8.29) for DataHub v0.8.29 on GitHub.
