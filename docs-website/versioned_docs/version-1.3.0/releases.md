---
title: DataHub Releases
sidebar_label: Releases
slug: /releases
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs-website/generateDocsDir.ts
---

# DataHub Releases

## Summary

| Version | Release Date | Links |
| ------- | ------------ | ----- |
| **v1.2.0** | 2025-07-19 |[Release Notes](#v1-2-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.2.0) |
| **v1.1.0** | 2025-05-28 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.1.0) |
| **v1.0.0** | 2025-03-14 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.0.0) |
| **v0.15.0.1** | 2025-01-21 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.15.0.1) |
| **v0.15.0** | 2025-01-15 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.15.0) |
| **v0.14.1** | 2024-09-17 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.14.1) |
| **v0.14.0.2** | 2024-08-21 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.14.0.2) |
| **v0.14.0** | 2024-08-13 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.14.0) |
| **v0.13.3** | 2024-05-23 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.13.3) |
| **v0.13.2** | 2024-04-16 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.13.2) |
| **v0.13.1** | 2024-04-02 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.13.1) |
| **v0.13.0** | 2024-02-29 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.13.0) |
| **v0.12.1** | 2023-12-08 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.12.1) |
| **v0.12.0** | 2023-10-25 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.12.0) |
| **v0.11.0** | 2023-09-08 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v0.11.0) |


## [v1.2.0](https://github.com/datahub-project/datahub/releases/tag/v1.2.0) {#v1-2-0}

Released on 2025-07-19 by [@chakru-r](https://github.com/chakru-r).

### What's Changed
* ci: don't rerun docker workflows on labels by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13405
* test(audit-events): updates for audit event tests by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13419
* feat(ingest): associate queries with operations by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13404
* Update search results page by [@annadoesdesign](https://github.com/annadoesdesign) in https://github.com/datahub-project/datahub/pull/13303
* chore(airflow): update dev mypy to 1.14.1 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13374
* feat(changeSyncAction): support RESTATE type syncs by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13406
* feat(ui/lineage): Make show ghost entities toggle local storage sticky by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13424
* ci: Add yaml format check by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13407
* fix(graphql): remove false deprecation note by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13402
* feat(ingestion): Make jsonProps of schemaMetadata less verbose by [@skrydal](https://github.com/skrydal) in https://github.com/datahub-project/datahub/pull/13416
* feat(sdk): scaffold assertion client by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13362
* fix(): DUE Producer Configuration & tracking message validation by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13427
* fix(build): fix version in jars by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13432
* fix(smoke-test): fix flakiness of audit smoke test by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13429
* fix(ingest/snowflake): fix previously broken tests by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13428
* fix(searchBarAutocomplete): ui tweaks by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13430
* fix(docker): Fix for metadata ingestion docker build by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13435
* fix(ui) Add ellipses and tooltip to long names on home page header by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13425
* updated search menu items after search update by [@annadoesdesign](https://github.com/annadoesdesign) in https://github.com/datahub-project/datahub/pull/13422
* docs(release): Adding notes for v0.3.10.3 release by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/13437
* fix(ingest/tableau): Fix infinite loop in Tableau retry by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13442
* fix(cli): ignore extra configs by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13444
* fix(ingest/snowflake): parsing issues with empty queries by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13446
* docs: remove old pages & assets  by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13367
* fix(build): fix local quickstart builds by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13445
* docs: Adding color to 3.10 release notes by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13448
* fix(ingest/mode): Not failing if queries endpoint returns 404 by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13447
* chore(avro): bump parquet-avro version by [@esteban](https://github.com/esteban) in https://github.com/datahub-project/datahub/pull/13452
* ci(): run smoke tests on release by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13454
* feat(UI): funnel subtype for dataflows and datajobs all the way to the UI by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13455
* improvement(ui): add wrapper component for stop propagation by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13434
* chore(ingest): bump bounds on cooperative timeout test by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13449
* feat(docs): Add 0.3.10.4 hotfix release notes by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13458
* fix(authentication) redirection for native login and sso to function within iframes by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13453
* fix(docs): Add feature availability to audit API by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13459
* docs: remove markprompt by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13463
* docs: add runllm chatbot by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13464
* feat(sdk): add datajob lineage & dataset sql parsing lineage  by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13365
* fix(versioning): Properly set versioning scheme on unlink; always run side effects by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13440
* feat(sdk): update lineage sample script to use client.lineage by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13467
* feat(docs): Add doc links for 1.0.0 by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13462
* docs: remove 0.15.0 from archived list by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13468
* fix(docs): Add requirement on yarn for documentation for local development by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13461
* feat(ingestion/kafka): Add optional externalURL base for link to external platform by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/12675
* feat(ingest): filter by database in superset and preset by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13409
* fix(web) domain search result item visual cleanup by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13474
* fix(test): prevent audit test flakiness by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13475
* fix(ingest/hive): Fix hive properties with double colon by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13478
* fix(sdk): use pluralized assertions by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13481
* docs: Add show manage tags environment var by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13482
* docs(): v0.3.11 DataHub Cloud Docs by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13439
* tests(ingestion): fixes hex and hive docker flakiness by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13476
* fix(sdk): always url-encode entity links by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13483
* tests(smoke): removes non existing `mix_stderr` param in `CliRunner` by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13485
* docs: Adding notes on remote executors handling smart assertions by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13479
* fix(cli): warn more strongly about hard deletion by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13471
* fix(ui): enable to edit tag when properties aspect was not present by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13470
* docs(cloud): fix indent, remove note not relevant for cloud users by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13493
* Adding KafkaClients dependency to the datahub-upgrade module by [@RafaelFranciscoLuqueCerezo](https://github.com/RafaelFranciscoLuqueCerezo) in https://github.com/datahub-project/datahub/pull/13488
* feat(cassandra): Support ssl auth with cassandra by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13465
* fix(ingest/presto): Presto/Trino property extraction fix by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13487
* chore(): graphiql latest versions by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13484
* feat(ingest/sql): column logic + join extraction by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13426
* chore(hex): debug logs by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13473
* fix(mssql): improve stored proc lineage + add `temporary_tables_pattern` config by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13415
* fix(ingest/mode): Additional pagination and timing metrics by [@mminichino](https://github.com/mminichino) in https://github.com/datahub-project/datahub/pull/13497
* feat(config): add configurable search filter min length by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13499
* ci(workflow): postgres consolidation & release unit tests by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13500
* fix(config): fix mcp consumer batch property by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13504
* fix(ci): enable publish on release by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13506
* feat(ingestion/looker): extract group_labels from looker and add as tags in datahub by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/13503
* Refactor elasticsearch search indexed by [@jmacryl](https://github.com/jmacryl) in https://github.com/datahub-project/datahub/pull/13451
* fix(ingest/mode): Additional 404 handling and caching update by [@mminichino](https://github.com/mminichino) in https://github.com/datahub-project/datahub/pull/13508
* feat(platform): up limit of corpuser char length from 64 to 128 by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/13510
* feat(ingest): improve join extraction by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13502
* feat(ingest): support pydantic v2 in mysql source by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13501
* fix(ci): metadata-io test by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13514
* build(deps): bump gradle/gradle-build-action from 2 to 3 by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/12951
* build(deps): bump actions/cache from 3 to 4 by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/13346
* Support different container runtimes aliased as docker by [@uk555-git](https://github.com/uk555-git) in https://github.com/datahub-project/datahub/pull/13207
* fix(mcp-processor): prevent exception in mcp processor by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13513
* update(ci): specify branch by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13515
* fix(docs): broken link in spark docs by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13516
* fix(graphql,lineage): Fix CLL through queries by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13519
* fix(ci): fix typo by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13520
* fix(ui): fix useGetUserGroupUrns when user urn is empty by [@Masterchen09](https://github.com/Masterchen09) in https://github.com/datahub-project/datahub/pull/13359
* fix(cli): avoid click 8.2.0 due to bugs by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13518
* fix(sdk): change deprecated value use by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13511
* fix(cli): move to stderr instead of stdout by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13512
* build(quickstart): skip composeForceDownOnFailure for debug variants by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13521
* build(ci): fix a gradle implicit dependency error by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13522
* fix(docs): update support limits for liquid-python by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13524
* chore(): bump kafka-setup base image by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13527
* fix(ui/glossary): Display custom properties on glossary nodes by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13526
* fix(ui/table): Fix column / ml feature description show more button by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13525
* fix(): remove unused recursive code by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13528
* fix(docs): Improve backup and restore doc by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/13466
* fea(ui): Add menu action for copying the full name of the asset by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/13224
* fix(docs): add known issue for server_config by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13530
* fix(graphql): Add default parameters to access token resolver by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13535
* feat(search): PFP-1275/look-into-0-doc-indices by [@jmacryl](https://github.com/jmacryl) in https://github.com/datahub-project/datahub/pull/13296
* feat(ingestion): create feature flag for ingestion page redesign by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13532
* feat(ingest/snowflake): generate lineage through temp views by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13517
* feat(ingestion/s3): Add externalUrls for datasets in s3 and gcs by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/12763
* tests(ingestion): moving some tests so they are available for sdk users by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13540
* fix(ingestion/datahubapply): fix typos in config descriptions by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/13546
* fix(ingest(gc): remove default cli version by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13552
* fix(ingest/hive): Fix hive storage path formats by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13536
* fix(smoke-test): use full quickstart image instead of slim for spark tests by [@esteban](https://github.com/esteban) in https://github.com/datahub-project/datahub/pull/13543
* docs: fix inline code format by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13549
* feat(lineage): Add feature flag to hide expand more action by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13557
* ci(docs): Fix md prettier lint by ignoring inline blocks by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13558
* fix(ui/filters): Improve platform instance filter by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13559
* fix(hook): collect write mutation hook to ensure side effects by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13554
* ci(publish): restore image publish on push to master by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13562
* feat(ui/ingestion): create ingestionV2 folder and copy files by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13565
* feat(ingest/dbt): fallback to schema from graph by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13438
* fix(ui): Hide 404 page while loading permissions by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13570
* fix(ui): Add Admin Onboarding Steps + Change display name for iceberg policies DES-369 by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/13456
* docs(): Update v_0_3_11 Release notes by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13555
* fix(web): glossary term create buttons inlined in content area by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13571
* fix(ingest/dbt): Fix urn validation in ownership type check. by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13563
* feat(ingestionSource/ownership): add ownership aspect to ingestionSource by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13567
* fix(cli): strictly validate structured property values by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13576
* fix(ui): Fix pagination overflow on embedded list search results by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13580
* fix(tags): Support null tagProperties aspect when updating tag color by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/13572
* feat(web): display external assertion result links in run event view by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13587
* feat(docs-website): announcement banner features series b by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13588
* docs: update markdown_process_inline_directive to work with indentations by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13590
* fix(ui): Changes to components by [@sakethvarma397](https://github.com/sakethvarma397) in https://github.com/datahub-project/datahub/pull/13589
* docs(datahub cloud): fix remote executor version by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13597
* fix(build): fix quickstartslim to use datahub-actions by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13592
* refactor(web) move access management tab to the front by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13092
* fix(forms) Remove schema field entities from form assignment entity types by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13599
* fix(test): adjust exclusion rule by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13603
* fix(config): fix mcp batch configuration by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13598
* fix(ingest): restrict duckdb dep on old python versions by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13605
* feat(hex): consider additional context when parsing hex query metadata by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13596
* fix(web) set domain dropdown ui to match domains page by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13294
* doc(assertion): change to millis in example by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13610
* docs: add note on column transformation logic by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13615
* fix(config-servlet): fix config endpoint to be thread-safe by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13616
* feat(docs): Add docs for 0.3.11.1 Cloud Release by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13604
* feat(docs): Add section on updating DataHub for 1.1.0 by [@esteban](https://github.com/esteban) in https://github.com/datahub-project/datahub/pull/13561
* config(gradle): pin jackson via bom by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13617
* fix(ingest/datahub): Create Structured property templates in advance and batch processing by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13355
* fix(generic-patch): fix mixed attributed/non-attributed patches by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13618
* ci(smoke-tests): run tests on push to release branches by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13629
* fix(test): url encode urn by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13626
* fix(iceberg): update MinIO client commands for compatibility by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13631
* ci(smoke-tests): run tests on push to release branches ([#13629](https://github.com/datahub-project/datahub/pull/13629)) (addendum) by [@esteban](https://github.com/esteban) in https://github.com/datahub-project/datahub/pull/13635
* feat(ingestion): add snowflake ingestion config options by [@AndrewSmith593](https://github.com/AndrewSmith593) in https://github.com/datahub-project/datahub/pull/12841
* build: support tag computation from branch with slash in name by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13636
* build: add codecov bundle analysis by [@joseph-sentry](https://github.com/joseph-sentry) in https://github.com/datahub-project/datahub/pull/13087
* feat(tracing): trace error log with timestamp & update system-metadata by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13628
* feat(openapi): add verify_ssl configuration option by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13634
* fix(snowflake): pass correct BaseTimeWindowConfig instead of SnowflakeV2Config by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13643
* config(): disable service name in header by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13638
* feat(docker-compose): add localstack to compose profiles by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13650
* fix(policies): more assertions, add missing policy for editor role by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13644
* feat(docs): Publish docs for 1.1.0 release by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13647
* improvement(ui): bring compact markdown viewer changes to OSS by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13593
* feat(sdk): add dataflow and datajob entity by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13551
* fix(docs): Correct Access Policy references to GraphQL endpoints by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13645
* feat(ingestions): redesign ingestion table and page by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13585
* ci(coverage): run coverage without ci-optimization on release and schedule by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13579
* change ES default refresh_interval:3s by [@jmacryl](https://github.com/jmacryl) in https://github.com/datahub-project/datahub/pull/13155
* fix(log): improve log for list-source-runs without privileges by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13653
* build: add depot.json by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13595
* chore(): bump beanutils version by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13656
* config(docker-compose): localstack healthcheck by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13652
* fix(quickstart): Enable V2 UI by default by [@pedro93](https://github.com/pedro93) in https://github.com/datahub-project/datahub/pull/13664
* fix(web) fallback to search to get count by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13556
* feat(ingestions): support backend sort and combine type and name sorts in ingestion table by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13625
* feat(ingestion): update the layout of secrets tab by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13627
* feat(components): show the remaining number in stacked avatar component by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13633
* feat(ingestion): add source to executionRequest by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13614
* feat(ui/ingestion): add empty and loading states for sources and secrets tables by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13646
* bump(): upgrade Kafka 7.9.1 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13667
* fix(openapi_parser): add ability to parse openapi 3.0+ schemas by [@chatterjee-atanu](https://github.com/chatterjee-atanu) in https://github.com/datahub-project/datahub/pull/13624
* feat(iceberg): add namespace permissions by [@ksrinath](https://github.com/ksrinath) in https://github.com/datahub-project/datahub/pull/13414
* feat(ingestion): add execution log tab by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13611
* refactor(web): Embed url mapping into Tabs component for general use by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13648
* feat(kafka): add interface design for listeners by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13637
* fix(cli): safely initialize schema_fields by [@cpd85](https://github.com/cpd85) in https://github.com/datahub-project/datahub/pull/13586
* chore(): bump jetty version by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13679
* fix(web): Only display assertion tab buttons if there is more than one button by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13678
* docs: skip release note details for 1.1.0 on docs site to fix build by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13683
* docs(cloud): update remote executor version by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13675
* feat(ingestion/sql-common): add column level lineage for external tables by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/11997
* feat(ingestion): add owners in create/edit ingestion source and show owners in ingestion table by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13663
* feat(ingestion): add rollback button by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13677
* fix(ingestion): fix size of the sources filter by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13686
* improvement(ui/ingestion): improvements in redesigned ingestion table by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13674
* Pfp 1277/indexing perf by [@jmacryl](https://github.com/jmacryl) in https://github.com/datahub-project/datahub/pull/13480
* feat(search): lineage search performance by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13545
* feat(ui/lineage): Support changing home node via double click by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13403
* fix(docs): make docs inclusion opt-in by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13680
* feat(sdk): add structured properties aspect by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13689
* feat(sdk): add add_lineage to lineage subclient by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13622
* feat(sdk): add EntityClient.delete documentation and tests by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13688
* fix(build): restore deleted code that broke build by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13695
* build(deps): bump tar-fs from 2.1.2 to 2.1.3 in /docs-website by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/13673
* fix(ui): null deref by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13696
* feat(ui/ingestion): use routed tabs and add links between sources and execution logs tab by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13694
* feat(sdk): add get_lineage by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13654
* feat(sdk) add dashboard & chart entity by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13669
* feat(ingest/unity-catalog): Tag extraction by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13642
* feat(sdk): Add support for Assertion and Monitor entities by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13699
* fix(build/storybook): Surface errors by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13711
* fix(docs): policies footnotes out of order by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13691
* fix(sdk): Ignore mypy error for conditional ResolverClient import by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13712
* chore(): global tomcat exclude by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13698
* fix(ci): handle missing github ref by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13714
* fix(ui/ingestion): filter current user to prevent the owner from getting added twice by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13708
* fix(ui/ ingestion): fix double onboarding modals on ingestion page by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13709
* fix(ingestion): rename name column and filter to sources on the executions tab by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13704
* fix(ingestion): show dash for null values in sources and execution tables by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13705
* feat(application): Adding application entity models, apis, search and entity page. by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13660
* Updated drawer to match entity drawer styling by [@annadoesdesign](https://github.com/annadoesdesign) in https://github.com/datahub-project/datahub/pull/13665
* feat(sdk): Add subscriptions client to main client OSS by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13713
* fix(usageEvent): fix hook interface by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13724
* feat(smoke-test): support fixtures with timestamp updates by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13727
* feat(ui/ingestion): implement hover state for stacked avatars in owners column by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13703
* feat(ingestion): add hidden sources message to table's footer by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13685
* feat(rest-emitter): set 60s ttl on gms config cache by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13729
* feat(graphql) enriching entity health results with more context by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13728
* Updated the Did You Mean to not be cut off by [@annadoesdesign](https://github.com/annadoesdesign) in https://github.com/datahub-project/datahub/pull/13725
* fix(ui): Move buttons outside the form by [@sakethvarma397](https://github.com/sakethvarma397) in https://github.com/datahub-project/datahub/pull/13715
* feat(ingestion/mssql): detection of rds or managed sql server for jobs history by [@acrylJonny](https://github.com/acrylJonny) in https://github.com/datahub-project/datahub/pull/13731
* refactor(limits): refactor configuration of query limits by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13726
* chore(ingest): bump sqlglot dep by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13730
* refactor(sdk): Use sdk instead of _sdk_extras by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13736
* feat(ui/ingestions): maintain the system sources filter across tabs by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13734
* feat(config): update configuration caching by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13740
* fix(application): adjusting applicatino to new search signature by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13742
* fix(ui) Fix location of data-testid for schema field drawer by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13741
* feat(ui/ingestion): show the newly added ingestion source at the top of the list by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13723
* fix(ui) Fix cursor jumping around in search bar by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13743
* feat(ingest/glue): Lake formation tags ingestion by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13693
* feat(application): editing application assignment via UI by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13739
* fix(ui) Take users to page 1 after page size change by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13744
* fix(graphql) Allow updating names of groups without corpGroupInfo by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13745
* feat(ingest,graphql): Support displaying actor of CLI ingestion by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13754
* fix(ingest): restrict snowflake-sqlalchemy dep by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13753
* fix(ingest/bigquery): Set qualified name for bigquery containers by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13747
* fix(theme) Fix env var for customizing order of home page sidebar by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13749
* docs: update the example scripts with the new sdk by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13717
* docs: add search sdk guide by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13682
* fix(ingestion/nifi): Incorrect SSL context usage for client certificate auth by [@readl1](https://github.com/readl1) in https://github.com/datahub-project/datahub/pull/13531
* feat(ingest): add debug source by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13759
* build(deps): bump brace-expansion from 1.1.11 to 1.1.12 in /smoke-test/tests/cypress by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/13756
* docs: linage client SDK guide by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13700
* chore(): bump postgresql lib by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13764
* fix(ingestion): UI fixes by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13765
* fix(ingestion): replace nonexistent icon by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13750
* docs: use sphinx-markdown-builder for sdk doc generation by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13721
* feat(ui): add copy urn tag and structured properties by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13766
* fix url for view in source from edit view to explore view by [@llance](https://github.com/llance) in https://github.com/datahub-project/datahub/pull/13666
* fix(irc): map conflict exception per iceberg spec ([#5960](https://github.com/datahub-project/datahub/pull/5960)) by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13751
* build(deps): bump aquasecurity/trivy-action from 0.30.0 to 0.31.0 by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/13719
* chore(deps): fix (com.google.code.gson:gson) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/13672
* Update README.md by [@leaderofrogue](https://github.com/leaderofrogue) in https://github.com/datahub-project/datahub/pull/13668
* Handle Cyclic References in AVRO Schema Conversion by [@mihai103](https://github.com/mihai103) in https://github.com/datahub-project/datahub/pull/13608
* chore(deps): fix (org.reflections:reflections) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/13298
* fix(doc): Update proposals documentation by [@sakethvarma397](https://github.com/sakethvarma397) in https://github.com/datahub-project/datahub/pull/13769
* fix(dataschema): fix for reflections upgrade by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13774
* feat(ci): restrict workflow runs for publish jars by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13776
* fix(ownershipOwnerTypes): fix missing features in hook by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13775
* doc(unity-catalog): Add doc for Databricks metadata sync by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13760
* fix(embedded search): making embedded search list use smart defaults for types by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13778
* feat(kafka): bump confluent kafka by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13767
* fix(sdk): fix typos + improve links in sdk docs by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13777
* fix(sdk): Move resolver client import into resolve method by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13780
* docs: add MCP server guide by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13779
* fix(sql-parsing): handle pyo3_runtime.PanicException from SQLGlot Rust tokenizer by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13758
* refactor(neo4j): parameterized neo4j Queries execution to escape special characters in urn by [@deepgarg-visa](https://github.com/deepgarg-visa) in https://github.com/datahub-project/datahub/pull/13793
* feat(docs): smart assertions and dataset health dashboard docs by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13782
* fix(metadata_change_sync): fix unicode handling by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13804
* fix(snowflake summary): fixing snowflake summary source by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13785
* docs(actions): schema registry configuration tips by [@shirshanka](https://github.com/shirshanka) in https://github.com/datahub-project/datahub/pull/13789
* fix(docs): add missing privileges to docs by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13805
* fix(reindex): fix cast exception during reindex by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13815
* fix(docs): remove ref to github_info by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13816
* fix(docs): Update proposals docs by [@sakethvarma397](https://github.com/sakethvarma397) in https://github.com/datahub-project/datahub/pull/13800
* docs(Forms) Update Form Creation guide with notification details by [@maggiehays](https://github.com/maggiehays) in https://github.com/datahub-project/datahub/pull/13786
* fix(sql-parsing): catch pyo3_runtime.PanicException instead of BaseException by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13806
* feat(docs): add showing specific fields to docs of specific connectors by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13810
* feat(cli): add restore-indices CLI command by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13820
* doc(mysql,kafka): fix support status by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13812
* feat(cli): --streaming-batch option delete large hierarchy by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13824
* feat(doc): hide repeating allow/deny things in config tables by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13826
* fix(ingest/bigquery): use email as user urn by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13831
* ingest(snowflake): remove email_as_user_identifier support by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13827
* docs: add screenshots for AI documentation by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13821
* feat(docs) Update managed ingestion docs with new UI by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13799
* feat(openapi-31): properly update openapi spec to 3.1.0 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13828
* docs: add cloud v0.3.12 release notes by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13784
* telemetry: add support for unified mixpanel + kafka tracking in GMS by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13795
* docs(iceberg): added notes on session expiry config, some minor updates by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13832
* doc(ingest): mark test connection capability by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13837
* fix(ui/ingest): mark more sources as supporting test connection by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13836
* feat(policies): support policy privilege constraints and ingestion aspect validators by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13819
* docs: add docs on DataHub slack bot by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13834
* docs: add note about MCP transport types by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13822
* log(nullpointer): when elastic doc has missing urn by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13839
* feat(fabricType): add new supported fabric types by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13842
* fix(ingest): add fineGrainedLineages as a special case for aspects by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13844
* fix(ui/ingest): ingestion run report by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13838
* docs(release): add MCP server note to release notes by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13853
* docs(ingest): docs for lineage by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13847
* docs(ingest): update integrations page by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13846
* doc(ingest): mark for usage by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13850
* fix(ingest/dremio): fix report, mark usage stats capability by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13851
* deprecate(ingest): match_fully_qualified_names for redshift,bigquery by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13858
* fix(quickstart): limit aws_endpoint_url override only for localstack by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13861
* feat(operations): add es raw operations endpoints by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13855
* feat(ingestion): use approx_distinct when profiling Athena and Trino by [@ligfx](https://github.com/ligfx) in https://github.com/datahub-project/datahub/pull/13671
* ci: add ligfx to team in pr-labeler.yml by [@ligfx](https://github.com/ligfx) in https://github.com/datahub-project/datahub/pull/13864
* feat(): lineage registry via openapi by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13865
* fix(docs-site): updated weekly demo link by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13868
* fix(quickstart): use latest zookeeper version by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13871
* feat(dremio): add configurable time range for query lineage extraction, sql aggregator report and fix schema_pattern filtering by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13613
* fix(quickstart): restore kafka version by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13873
* fix(emitter): fix emitter handling of unicode characters by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13867
* feat(docs): add subscription client docs to assertions tutorial by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13872
* docs(ingestion): preset - update source certification status by [@shirshanka](https://github.com/shirshanka) in https://github.com/datahub-project/datahub/pull/13641
* feat(fabrictype): add sbx fabrictype by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13875
* fix(docs): update patch support documentation by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13093
* docs: add sdk entity guides by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13870
* fix(quickstart): use kafka version that supports zookeeper by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13879
* fix(ingest/rest): out-of-date structured report being sent by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13866
* doc(cli): update for restore-indices by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13880
* fix(docs): revise slack bot troubleshoot instructions by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13884
* doc(ingest/azuread): remove outdated information by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13885
* build(deps): bump brace-expansion from 1.1.11 to 1.1.12 in /datahub-web-react by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/13770
* fix(docs): add recommended versions for v0.3.12 by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13863
* doc(cloud): make datahub cloud docs prominent in side bar by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13860
* fix(ingest/sql_server): switch to engine inspector instead of connection by [@sleeperdeep](https://github.com/sleeperdeep) in https://github.com/datahub-project/datahub/pull/13104
* chore(deps): fix (org.glassfish:jakarta.json) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/13269
* feat(openapi): entity registry api by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13878
* feat(ingest/snowflake): add extra_info for snowflake by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13539
* fix(search): additional search size defaults by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13888
* refactor(ingestion): make ingestion schedular properties configurable by [@deepgarg-visa](https://github.com/deepgarg-visa) in https://github.com/datahub-project/datahub/pull/13887
* feat(mock-data-source): add first seen urn in report by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13889
* feat(ingest): generate capability summary by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13881
* fix(cli): explicit default field value for optional field (pydantic v2) by [@skrydal](https://github.com/skrydal) in https://github.com/datahub-project/datahub/pull/13901
* docs: add missing preset logo by [@bmaquet](https://github.com/bmaquet) in https://github.com/datahub-project/datahub/pull/13897
* tests(doc): add tests for doc gen by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13903
* docs(v.0.3.12.1): adding maintenance release docs by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13892
* feat(homepage): create new feature flag for homepage redesign by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13882
* fix(ingestion): fix group avatar (changes from SaaS) by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13772
* feat(ingestion): backend changes from saas by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13796
* fix(searchBarV2): do not clean filters on the search hit with empty filters by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13706
* feat(ui/homepage): render skeleton of the new homepage by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13886
* feat(homePageRedesign): add header by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13904
* docs(airflow): document background operation by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13907
* docs: fix api-gateway version in release notes by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/13909
* fix(openapi): fix example by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13899
* feat(preset): add preset to the list of platforms by [@bmaquet](https://github.com/bmaquet) in https://github.com/datahub-project/datahub/pull/13896
* chore: aggregator_generate_timer for snowflake by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13913
* feat(ingest): add source aspect number in telemetry by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13914
* chore: Fix typo by [@Deseao](https://github.com/Deseao) in https://github.com/datahub-project/datahub/pull/13910
* docs: archive 1.0.0 versioned docs by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13912
* feat(ingest/lineage): generate static json lineage file by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13906
* docs(mcp): add troubleshooting section by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13893
* docs(slack): add docs on slackbot permissions by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13908
* fix[AddDatasetOwnership]: update logic to manage case with same owner… by [@sleeperdeep](https://github.com/sleeperdeep) in https://github.com/datahub-project/datahub/pull/13081
* feat(docs): clearer slack troubleshoot guide by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13926
* feat(auditEvents): add in top level delete policy event by default by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13928
* feat(models) Add data models for Custom Home page project - Templates and Modules by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13911
* fix(login): show login errors provided in response by [@ngamanda](https://github.com/ngamanda) in https://github.com/datahub-project/datahub/pull/10871
* feat(ingestion): add patch structured properties to data product by [@gabriel-morais-rokos](https://github.com/gabriel-morais-rokos) in https://github.com/datahub-project/datahub/pull/13813
* fix(ingest/unity): don't crash when processing Platform Resources hits an error by [@ligfx](https://github.com/ligfx) in https://github.com/datahub-project/datahub/pull/13877
* fix(ingest/bigquery): Emit dataset profile when table does not have rows by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13919
* feat: update fivetran connector with new sdk by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/13859
* refactor(ingest): centralise subtype strings by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13935
* update hover color in selects by [@annadoesdesign](https://github.com/annadoesdesign) in https://github.com/datahub-project/datahub/pull/13931
* chore(actions): bump acryl-executor by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13930
* feat(customHomePage): add generic module component by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13922
* fix(airflow): set minimum supported version to 2.7.0 ([#13357](https://github.com/datahub-project/datahub/pull/13357)) by [@harishkesavarao](https://github.com/harishkesavarao) in https://github.com/datahub-project/datahub/pull/13619
* fix(smoke): prevent audit events smoke flake by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13944
* fix(ingest): mypy lint python 3.8 by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13939
* fix(docs): hide unsupported assertion API feature docs by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13943
* fix(ui): Fix non-functional page size in tags page by [@sakethvarma397](https://github.com/sakethvarma397) in https://github.com/datahub-project/datahub/pull/13891
* feat(secret): increase secret encryption strength by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13942
* feat(docs): Instructions for bulk creation using new assertions SDK by [@anthonyburdi](https://github.com/anthonyburdi) in https://github.com/datahub-project/datahub/pull/13924
* feat(graphql) Add GraphQL Objects, Type classes, and Mappers for Templates and Modules by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13923
* fix(token-service): extend validation for actor by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13947
* feat(customHomePage): add asset item component by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13936
* feat(templates/modules) Ingest default page templates and modules by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13925
* feat(ingest/kafka-connect): Add more connectors the regexp transformation support by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13748
* feat(hex): add retry logic with exponential backoff for 429 rate limiting by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13905
* feat(ingest): add aspects by subtype in report, telemetry by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13921
* feat(docs): v0.3.12.2 release notes by [@jayacryl](https://github.com/jayacryl) in https://github.com/datahub-project/datahub/pull/13932
* feat(cli): add kafka helper, improve restore indices helper by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13951
* fix(graph/client): use fixed GMS URL consistently by [@ligfx](https://github.com/ligfx) in https://github.com/datahub-project/datahub/pull/13945
* fix(ui): reset page in embedded components by [@sakethvarma397](https://github.com/sakethvarma397) in https://github.com/datahub-project/datahub/pull/13949
* fix(ui/navBar): Fix logic to display manage tags link by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13564
* docs(dbt): incremental-lineage by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13959
* docs(airflow): follow-ups for dropping airflow &lt;2.7 ([#13619](https://github.com/datahub-project/datahub/pull/13619)) by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13946
* feat(actions): require pydantic v2 by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13958
* feat(ingest): add modifiers on capability on sources by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13954
* fix(ingest/s3): Fix ingestion when path_spec had a wildcard character in the path by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13940
* fix(ingest/bigquery): Fix accidentally removed size in bytes property by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13972
* feat(ingest/athena): Iceberg partition columns extraction by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13607
* fix(customHomePage): fix position of the views popover by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13916
* feat(ingest/snowflake): support TLL for stored procs by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13890
* chore: add claude configs by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13983
* chore(deps): fix (com.datastax.oss:java-driver-shaded-guava) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/13969
* fix: snowflake filtering generated queries by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13937
* fix(kafka-connect): escape dots in topic.prefix for regex patterns by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13955
* build(docker): use root as build context by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13968
* fix(lineage): support overriding SQL dialect in infer_lineage_from_sql by [@ofirNakdai](https://github.com/ofirNakdai) in https://github.com/datahub-project/datahub/pull/13966
* fix(kafka-connect): handle SQL Server 3-level topic patterns and add Debezium integration tests by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13970
* feat(applications): Manage applications screen by [@gabe-lyons](https://github.com/gabe-lyons) in https://github.com/datahub-project/datahub/pull/13814
* fix(mysql): modifies hashing and equals behavior for primary keys to match mysql behavior by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13984
* fix(ui) Update spacing depending on height to fix sidebar scroll ([#5979](https://github.com/datahub-project/datahub/pull/5979)) by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13768
* feat(monitoring): modernize datahub monitoring by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13898
* feat(ui/ingest): make list of test connection dynamic by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13917
* feat(homepage): build announcements functionality on the new homepage by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13938
* fix(ci): cassandra test stabilization by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13997
* feat(template) Add upsertPageTemplate graphql endpoint + service by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13948
* feat(home) Get correct home page template and render rows/modules by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13978
* feat(ui) Use new Tabs component on entity profile pages by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13640
* feat(ui) Finalize Your Assets module on the frontend by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13980
* fix(gradle): prevent metadata-utils from downgrading zookeeper by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/13998
* feat(homepage): ingest your subscriptions and domains module in the default template by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13993
* chore(python): drop support for Python 3.8 by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13999
* feat(ingest): add better urn samples in report by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/13977
* chore(python): drop python 3.8 follow ups by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14004
* fix(airflow/extractors): capture destinationTable lineage in BigQueryInsertJobOperator by [@bmaquet](https://github.com/bmaquet) in https://github.com/datahub-project/datahub/pull/13963
* chore(deps):fix CVE-2021-37136 (com.typesafe.play : shaded-asynchttpclient) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/13976
* fix(ingest): add required json as part of CLI by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14008
* feat(graphql) Add upsertPageModule graphql endpoint for home page by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13981
* fix(ingest/unity): don't get schema or catalog tags if include_tags=false by [@ligfx](https://github.com/ligfx) in https://github.com/datahub-project/datahub/pull/14013
* fix(ingest/gcs): Fix GCS URI mismatch causing file filtering during ingestion by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/14006
* fix(systemUpdate): add configuration for opensearch clusters with zone awareness enabled by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/13996
* misc(metrics): fix legacy metrics cache & tests by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/14011
* chore(build): upgrade to gradle 8.14.3 by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14017
* fix(avro): expand record fields consistently in arrays, maps, and direct references by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13961
* feat(superset/ingest): add metrics to dataset columns by [@bmaquet](https://github.com/bmaquet) in https://github.com/datahub-project/datahub/pull/13894
* chore(deps): fix (com.typesafe.akka:akka-protobuf)  by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/14026
* feat(customHomePage): add skeleton for adding new modules to a template by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/13994
* chore(capability): remove capability summary task from pre-commit by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14023
* chore(ingest): simplify MetadataChangeProposalWrapper usages by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14019
* fix(snowflake): resolve table name collision between FileBackedList and StoredProcLineageTracker by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/14028
* fix(python): remove deprecated license classifiers in favor of SPDX identifiers by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14016
* refactor(sql-parsing): rename default_dialect to override_dialect parameter by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14015
* improvement(ingestion): update sort functionality and other ingestion improvements by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13762
* fix(docs): clarify snowflake back-fill capabilities by [@kevinkarchacryl](https://github.com/kevinkarchacryl) in https://github.com/datahub-project/datahub/pull/14031
* feat(ci): upgrade to Cypress 14.5.1 ([#6204](https://github.com/datahub-project/datahub/pull/6204)) by [@benjiaming](https://github.com/benjiaming) in https://github.com/datahub-project/datahub/pull/14030
* fix(metrics): cleanup warnings around duplicate metrics by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/14032
* feat(ingestion): bring back ingestionV2 changes to OSS by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/13974
* refactor(ingest/tableau): cleanup duplicate lineage calls by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14018
* docs(ui): Add CLAUDE.md by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/14035
* feat(ingest): add subtype capability modifier by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14039
* ci: add username to pr-labeler by [@yoonhyejin](https://github.com/yoonhyejin) in https://github.com/datahub-project/datahub/pull/14043
* fix(ingestion/snowflake): Address diamond lineage problem + performance improvements by [@skrydal](https://github.com/skrydal) in https://github.com/datahub-project/datahub/pull/13918
* feat(ingestion): Introduce query dedup strategy by [@skrydal](https://github.com/skrydal) in https://github.com/datahub-project/datahub/pull/13915
* feat(quickstart): migrate to compose profile and other improv. by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/13566
* feat(ingest): warn of older cli versions automatically by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14007
* feat(tableau): add config flags to emit all published and embedded datasources by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/13952
* fix(alchemy): fix typo by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14047
* fix(ingest/tableau): optimize tableau test performance by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14036
* fix(ingest/teradata): Teradata perf improvements by [@treff7es](https://github.com/treff7es) in https://github.com/datahub-project/datahub/pull/13967
* feat Add more granular USERS and GROUPS privileges by [@DiogoVala](https://github.com/DiogoVala) in https://github.com/datahub-project/datahub/pull/12637
* build(deps): bump aquasecurity/trivy-action from 0.31.0 to 0.32.0 by [@dependabot](https://github.com/dependabot)[bot] in https://github.com/datahub-project/datahub/pull/13971
* feat(openapi): add generic entity api for v3 CRUD operations by [@kevin1chun](https://github.com/kevin1chun) in https://github.com/datahub-project/datahub/pull/13987
* fix(sdk): deduplicate entity types in search sdk by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14041
* fix(ingest/dremio): add debug info on failed requests by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14040
* chore(ingest/teradata): fix lint errors by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14055
* feat(rest_emitter): support delete emit mcp by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/14033
* chore(airflow): remove v1 airflow plugin by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14054
* fix(ci): avoid concurrent action cache  updates by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/14058
* feat(ui/lineage): Lineage V3 (redesign + data flow lineage) by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/14056
* fix(metrics): fix metrics warning by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/14061
* fix(test): fix test conflicts in openapi by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/14064
* feat(homepage): support rendering top domains module in the new home page by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/14010
* fix(homepage): remove default subscription module and its type references by [@purnimagarg1](https://github.com/purnimagarg1) in https://github.com/datahub-project/datahub/pull/14027
* feat(customHomePage): adjust rows component and add wrapping by [@v-tarasevich-blitz-brain](https://github.com/v-tarasevich-blitz-brain) in https://github.com/datahub-project/datahub/pull/14025
* Enable UI theme V2 by default by [@chriscollins3456](https://github.com/chriscollins3456) in https://github.com/datahub-project/datahub/pull/13995
* docs: typos fixed by [@benjiaming](https://github.com/benjiaming) in https://github.com/datahub-project/datahub/pull/14068
* fix(ingest/mlflow): add missing container modifier by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14046
* fix(cli): add back upgrade manually on cases communicating with server by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14052
* fix(ingest/tableau): add missing container capability by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14069
* fix(ingest/report): fix bug w.r.t. aspect count by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14070
* fix(ingest): better experimental contract by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14071
* feat(ingest): improve readability by using tables by [@anshbansal](https://github.com/anshbansal) in https://github.com/datahub-project/datahub/pull/14074
* chore(deps): fix (org.mozilla:rhino) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/13270
* chore(deps): fix (org.pac4j:pac4j-oidc) by [@relaxedboi](https://github.com/relaxedboi) in https://github.com/datahub-project/datahub/pull/14065
* feat(search): custom search configuration for highlighting by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/14057
* fix(ci): remove use of wildcards in hashFiles by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/14072
* chore(gx-plugin): require pydantic v2 by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14059
* feat(sdk): add client_mode and component to DataHubClient.from_env() by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14060
* feat(ingest): add global sentry/telemetry properties by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/14062
* feat(snowflake): add database pattern filtering to access history query for improved performance by [@sgomezvillamor](https://github.com/sgomezvillamor) in https://github.com/datahub-project/datahub/pull/14021
* fix(openapi): nullable openapi 3.1.0 syntax for aspect ref fields by [@kevin1chun](https://github.com/kevin1chun) in https://github.com/datahub-project/datahub/pull/14063
* docs(ui/lineageV3): Update lineage docs with new copy and screenshots; add data flow lineage doc by [@asikowitz](https://github.com/asikowitz) in https://github.com/datahub-project/datahub/pull/13833
* feat(frontend): upgrade to vite 6, use vite plugin-react-swc by [@hsheth2](https://github.com/hsheth2) in https://github.com/datahub-project/datahub/pull/13716
* feat(ui): Adding release pop up in disabled state for v0.1.2 by [@jjoyce0510](https://github.com/jjoyce0510) in https://github.com/datahub-project/datahub/pull/14066
* fix(openapi): openapi 3.1.0 nullable refs to use oneOf(ref, null) by [@kevin1chun](https://github.com/kevin1chun) in https://github.com/datahub-project/datahub/pull/14079
* fix(ci): adjust runner size by [@chakru-r](https://github.com/chakru-r) in https://github.com/datahub-project/datahub/pull/14135

### New Contributors
* [@RafaelFranciscoLuqueCerezo](https://github.com/RafaelFranciscoLuqueCerezo) made their first contribution in https://github.com/datahub-project/datahub/pull/13488
* [@uk555-git](https://github.com/uk555-git) made their first contribution in https://github.com/datahub-project/datahub/pull/13207
* [@AndrewSmith593](https://github.com/AndrewSmith593) made their first contribution in https://github.com/datahub-project/datahub/pull/12841
* [@joseph-sentry](https://github.com/joseph-sentry) made their first contribution in https://github.com/datahub-project/datahub/pull/13087
* [@chatterjee-atanu](https://github.com/chatterjee-atanu) made their first contribution in https://github.com/datahub-project/datahub/pull/13624
* [@cpd85](https://github.com/cpd85) made their first contribution in https://github.com/datahub-project/datahub/pull/13586
* [@readl1](https://github.com/readl1) made their first contribution in https://github.com/datahub-project/datahub/pull/13531
* [@leaderofrogue](https://github.com/leaderofrogue) made their first contribution in https://github.com/datahub-project/datahub/pull/13668
* [@ligfx](https://github.com/ligfx) made their first contribution in https://github.com/datahub-project/datahub/pull/13671
* [@bmaquet](https://github.com/bmaquet) made their first contribution in https://github.com/datahub-project/datahub/pull/13897
* [@Deseao](https://github.com/Deseao) made their first contribution in https://github.com/datahub-project/datahub/pull/13910
* [@gabriel-morais-rokos](https://github.com/gabriel-morais-rokos) made their first contribution in https://github.com/datahub-project/datahub/pull/13813
* [@harishkesavarao](https://github.com/harishkesavarao) made their first contribution in https://github.com/datahub-project/datahub/pull/13619
* [@ofirNakdai](https://github.com/ofirNakdai) made their first contribution in https://github.com/datahub-project/datahub/pull/13966
* [@benjiaming](https://github.com/benjiaming) made their first contribution in https://github.com/datahub-project/datahub/pull/14030
* [@DiogoVala](https://github.com/DiogoVala) made their first contribution in https://github.com/datahub-project/datahub/pull/12637

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.1.0...v1.2.0

## [v1.1.0](https://github.com/datahub-project/datahub/releases/tag/v1.1.0) {#v1-1-0}

Released on 2025-05-28 by [@pedro93](https://github.com/pedro93).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.1.0) for v1.1.0 on GitHub.

## [v1.0.0](https://github.com/datahub-project/datahub/releases/tag/v1.0.0) {#v1-0-0}

Released on 2025-03-14 by [@pedro93](https://github.com/pedro93).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.0.0) for v1.0.0 on GitHub.

## [v0.15.0.1](https://github.com/datahub-project/datahub/releases/tag/v0.15.0.1) {#v0-15-0-1}

Released on 2025-01-21 by [@pedro93](https://github.com/pedro93).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.15.0.1) for v0.15.0.1 on GitHub.

## [V0.15.0](https://github.com/datahub-project/datahub/releases/tag/v0.15.0) {#v0-15-0}

Released on 2025-01-15 by [@pedro93](https://github.com/pedro93).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.15.0) for V0.15.0 on GitHub.

## [v0.14.1](https://github.com/datahub-project/datahub/releases/tag/v0.14.1) {#v0-14-1}

Released on 2024-09-17 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.14.1) for v0.14.1 on GitHub.

## [v0.14.0.2](https://github.com/datahub-project/datahub/releases/tag/v0.14.0.2) {#v0-14-0-2}

Released on 2024-08-21 by [@RyanHolstien](https://github.com/RyanHolstien).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.14.0.2) for v0.14.0.2 on GitHub.

## [v0.14.0](https://github.com/datahub-project/datahub/releases/tag/v0.14.0) {#v0-14-0}

Released on 2024-08-13 by [@RyanHolstien](https://github.com/RyanHolstien).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.14.0) for v0.14.0 on GitHub.

## [v0.13.3](https://github.com/datahub-project/datahub/releases/tag/v0.13.3) {#v0-13-3}

Released on 2024-05-23 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.13.3) for v0.13.3 on GitHub.

## [v0.13.2](https://github.com/datahub-project/datahub/releases/tag/v0.13.2) {#v0-13-2}

Released on 2024-04-16 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.13.2) for v0.13.2 on GitHub.

## [v0.13.1](https://github.com/datahub-project/datahub/releases/tag/v0.13.1) {#v0-13-1}

Released on 2024-04-02 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.13.1) for v0.13.1 on GitHub.

## [v0.13.0](https://github.com/datahub-project/datahub/releases/tag/v0.13.0) {#v0-13-0}

Released on 2024-02-29 by [@RyanHolstien](https://github.com/RyanHolstien).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.13.0) for v0.13.0 on GitHub.

## [DataHub v0.12.1](https://github.com/datahub-project/datahub/releases/tag/v0.12.1) {#v0-12-1}

Released on 2023-12-08 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.12.1) for DataHub v0.12.1 on GitHub.

## [v0.12.0](https://github.com/datahub-project/datahub/releases/tag/v0.12.0) {#v0-12-0}

Released on 2023-10-25 by [@pedro93](https://github.com/pedro93).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.12.0) for v0.12.0 on GitHub.

## [v0.11.0](https://github.com/datahub-project/datahub/releases/tag/v0.11.0) {#v0-11-0}

Released on 2023-09-08 by [@iprentic](https://github.com/iprentic).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v0.11.0) for v0.11.0 on GitHub.

