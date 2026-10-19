package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.ContainerProperties;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.loadindices.AspectIndexLoader;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesArgs;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BackfillEntityIndicesStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesIncrementalStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexCatchUpStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.BackfillCheckpoint;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.CatchUpStatus;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.Status;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.tag.TagProperties;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The Search V3 zero-downtime cutover, end to end, for one {@link
 * SearchV3MigrationHarness.Topology}: Phase 1 creates one empty aliased V3 index per entity type,
 * live writes reach them during the rollout, the fill populates them from primary storage and
 * resumes after an interruption, catch-up replays the window, and the read flip moves keyword reads
 * to V3. Three later deploys follow: a V3 mapping change is copied with {@code _reindex} and the
 * swapped-out index stays current for a swap-back, an index built before document ids were hashed
 * is detected and rebuilt from primary storage into a fresh index, and V3 is moved to a cluster
 * that never held it, which fills it there from primary storage.
 *
 * <p>Subclasses pick the topology. The scenarios build on each other in order.
 */
abstract class SearchV3MigrationScenarios {

  /** Each system-update run is a deploy with its own version, and so its own state rows. */
  private static final String CUTOVER_DEPLOY = "0.0.0-split-1";

  private static final String MAPPING_CHANGE_DEPLOY = "0.0.0-split-2";
  private static final String ID_REBUILD_DEPLOY = "0.0.0-split-3";
  private static final String CLUSTER_MOVE_DEPLOY = "0.0.0-split-4";
  private static final String UNCHANGED_DEPLOY = "0.0.0-split-5";
  private static final String CONCRETE_REBUILD_DEPLOY = "0.0.0-split-6";

  private static final List<String> SEEDED_ENTITY_TYPES = List.of("container", "dataset", "tag");
  private static final Urn LIVE_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.live,PROD)");
  private static final Urn POST_SWAP_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.postswap,PROD)");
  private static final Urn POST_MOVE_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.postmove,PROD)");

  private SearchV3MigrationHarness harness;

  /** The dataset index: the one the later deploys evolve, rebuild and move. */
  private String datasetAlias;

  protected abstract SearchV3MigrationHarness.Topology topology();

  @BeforeClass(alwaysRun = true)
  public void startClusters() throws Exception {
    SearchV3MigrationHarness.requireOptIn();
    harness =
        SearchV3MigrationHarness.start(topology(), SearchV3MigrationHarness.Generations.V2_AND_V3);
    datasetAlias = harness.v3Alias("dataset");
  }

  @AfterClass(alwaysRun = true)
  public void stopClusters() {
    if (harness != null) {
      harness.close();
    }
  }

  @Test
  public void phase1PutsV2AndEmptyAliasedV3IndicesWhereTheyBelong() throws Exception {
    UpgradeStepResult result = run(phase1(CUTOVER_DEPLOY));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> state = harness.phase1State(CUTOVER_DEPLOY);
    for (String entity : SEEDED_ENTITY_TYPES) {
      String v2Index = harness.v2Index(entity);
      String v3Alias = harness.v3Alias(entity);
      assertTrue(
          harness.indexExists(harness.v2Client, v2Index),
          entity + " V2 index belongs on the V2 cluster");
      assertTrue(
          v2Index.startsWith(SearchV3MigrationHarness.DEPLOY_INDEX_PREFIX + "_"),
          "V2 names carry the deploy-wide prefix: " + v2Index);
      if (harness.separateClusters()) {
        assertTrue(
            v3Alias.startsWith(SearchV3MigrationHarness.SECONDARY_INDEX_PREFIX + "_"),
            "V3 names follow the V3 cluster's own prefix: " + v3Alias);
        assertFalse(
            harness.indexExists(harness.v3Client, v2Index),
            entity + " V2 index must not appear on the V3 cluster");
        assertFalse(
            harness.indexExists(harness.v2Client, v3Alias),
            entity + " V3 alias must not appear on the V2 cluster");
      } else {
        assertTrue(
            v3Alias.startsWith(SearchV3MigrationHarness.DEPLOY_INDEX_PREFIX + "_"),
            "V3 shares the deploy-wide prefix when it shares the cluster: " + v3Alias);
      }
      assertTrue(
          harness.indexExists(harness.v3Client, v3Alias),
          entity + " V3 alias belongs on the V3 cluster");
      assertEquals(harness.count(harness.v3Client, v3Alias), 0L);
      assertEquals(
          IncrementalReindexState.getStatus(state, v3Alias), Optional.of(Status.BACKFILL_PENDING));
      assertEquals(IncrementalReindexState.getIndexVersion(state, v3Alias), EntityIndexVersion.V3);
      assertEquals(
          IncrementalReindexState.get(
              state, v3Alias, IncrementalReindexState.BACKFILL_TARGET_INDEX),
          Optional.of(v3Alias));
    }
  }

  @Test(dependsOnMethods = "phase1PutsV2AndEmptyAliasedV3IndicesWhereTheyBelong")
  public void liveWritesReachV3BeforeTheFill() throws Exception {
    // The corpus that existed before V3: three entity types, in V2 only.
    harness.deliverToV2OnlyConsumers();
    for (int i = 1; i <= 3; i++) {
      harness.ingest(
          UrnUtils.getUrn("urn:li:container:split-c" + i),
          "containerProperties",
          new ContainerProperties().setName("container " + i));
    }
    for (int i = 1; i <= 4; i++) {
      harness.ingest(
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.d" + i + ",PROD)"),
          "datasetProperties",
          new DatasetProperties().setName("dataset " + i));
    }
    for (int i = 1; i <= 2; i++) {
      harness.ingest(
          UrnUtils.getUrn("urn:li:tag:split-t" + i),
          "tagProperties",
          new TagProperties().setName("tag " + i));
    }
    harness.syncWrites();
    assertEquals(harness.count(harness.v2Client, harness.v2Index("dataset")), 4L);
    assertEquals(harness.count(harness.v3Client, datasetAlias), 0L);

    // The rollout is done: consumers write both generations.
    harness.deliverToUpgradedConsumers();
    harness.ingest(LIVE_DATASET, "datasetProperties", new DatasetProperties().setName("live"));
    harness.syncWrites();

    assertEquals(harness.count(harness.v2Client, harness.v2Index("dataset")), 5L);
    List<SearchHit> v3Hits = harness.hits(harness.v3Client, datasetAlias);
    assertEquals(v3Hits.size(), 1);
    assertEquals(
        v3Hits.get(0).getId(),
        harness.hashedDocumentId(LIVE_DATASET),
        "a live write lands under the hashed document id");
  }

  @Test(dependsOnMethods = "liveWritesReachV3BeforeTheFill")
  public void fillResumesOnTheInterruptedIndexAndFinishesTheRest() throws Exception {
    // The fill takes the pending rows in index-name order: container, dataset, tag. Stop the run
    // after the dataset index's first page has landed, as a budget would.
    AspectIndexLoader realLoader = harness.aspectIndexLoader();
    AspectIndexLoader loader = spy(realLoader);
    AtomicBoolean interrupted = new AtomicBoolean(false);
    doAnswer(
            invocation -> {
              LoadIndicesArgs args = invocation.getArgument(1);
              if (!"urn:li:dataset:%".equals(args.urnLike) || interrupted.get()) {
                return invocation.callRealMethod();
              }
              AspectIndexLoader.BatchListener listener = invocation.getArgument(3);
              AspectIndexLoader.BatchListener stopAfterThisBatch =
                  (lastUrn, rows, urns) -> {
                    listener.onBatch(lastUrn, rows, urns);
                    interrupted.set(true);
                    throw new AspectIndexLoader.DeadlineExceeded(lastUrn, rows);
                  };
              return realLoader.load(
                  invocation.getArgument(0),
                  args,
                  invocation.getArgument(2),
                  stopAfterThisBatch,
                  invocation.getArgument(4));
            })
        .when(loader)
        .load(any(), any(), any(), any(), any());
    BackfillEntityIndicesStep fill = fill(CUTOVER_DEPLOY, loader);

    UpgradeStepResult firstRun = run(fill);
    harness.syncWrites();

    // A budget pause ends the run successfully so the rest of the non-blocking job proceeds; the
    // rows are what record that the fill is unfinished.
    assertEquals(firstRun.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> afterInterruption = harness.phase1State(CUTOVER_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(afterInterruption, harness.v3Alias("container")),
        Optional.of(Status.COMPLETED),
        "the index before the interruption was filled");
    assertEquals(
        IncrementalReindexState.getStatus(afterInterruption, datasetAlias),
        Optional.of(Status.BACKFILL_IN_PROGRESS));
    BackfillCheckpoint checkpoint =
        IncrementalReindexState.getBackfillCheckpoint(afterInterruption, datasetAlias)
            .orElseThrow();
    assertEquals(checkpoint.cursorEntity(), "dataset", "resume point is the interrupted index");
    assertTrue(
        checkpoint.cursorUrn().startsWith("urn:li:dataset:"), "cursor on the last dataset written");
    assertTrue(
        harness.count(harness.v3Client, datasetAlias) > 0, "the interrupted run's page landed");
    assertEquals(
        IncrementalReindexState.getStatus(afterInterruption, harness.v3Alias("tag")),
        Optional.of(Status.BACKFILL_PENDING),
        "the index after the interruption waits for the next run");
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("container")), 3L);
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("tag")), 0L);

    // Catch-up runs after the fill in the same job. With the fill paused it leaves the rows alone,
    // persists its own success for the other rows, and must not thereby let a later run skip the
    // V3 rows: the catch-up after the fill completes (next scenario) still has to replay them.
    UpgradeStepResult pausedCatchUp = run(catchUp(CUTOVER_DEPLOY));
    assertEquals(pausedCatchUp.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> pausedCatchUpState = harness.catchUpState(CUTOVER_DEPLOY);
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(pausedCatchUpState, datasetAlias),
        Optional.empty());
    assertFalse(
        IncrementalReindexState.isCatchUpCompleteForAllIndices(
            harness.phase1State(CUTOVER_DEPLOY), pausedCatchUpState),
        "a paused fill keeps catch-up incomplete");

    UpgradeStepResult resumed = run(fill);
    harness.syncWrites();

    assertEquals(resumed.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> completed = harness.phase1State(CUTOVER_DEPLOY);
    // The container index completed before the pause, so the paused catch-up already replayed it
    // and, with rollback dual-writes off, closed its row; the other two finished in this run.
    assertEquals(
        IncrementalReindexState.getStatus(completed, harness.v3Alias("container")),
        Optional.of(Status.DUAL_WRITE_DISABLED));
    for (String entity : List.of("dataset", "tag")) {
      assertEquals(
          IncrementalReindexState.getStatus(completed, harness.v3Alias(entity)),
          Optional.of(Status.COMPLETED));
    }
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("container")), 3L);
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("tag")), 2L);
    List<SearchHit> hits = harness.hits(harness.v3Client, datasetAlias);
    assertEquals(hits.size(), 5, "4 datasets filled, plus the live write");
    for (SearchHit hit : hits) {
      assertEquals(
          hit.getId(),
          harness.hashedDocumentId(UrnUtils.getUrn(SearchV3MigrationHarness.urnOf(hit))),
          "every filled document carries the hashed id");
      assertTrue(
          hit.getSourceAsMap().get("systemCreated") instanceof Number,
          "every filled document records the entity's creation time");
    }
  }

  @Test(dependsOnMethods = "fillResumesOnTheInterruptedIndexAndFinishesTheRest")
  public void catchUpReplaysTheFillWindowIntoV3() throws Exception {
    UpgradeStepResult result = run(catchUp(CUTOVER_DEPLOY));
    harness.syncWrites();

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> phase1State = harness.phase1State(CUTOVER_DEPLOY);
    Map<String, String> catchUpState = harness.catchUpState(CUTOVER_DEPLOY);
    for (String entity : SEEDED_ENTITY_TYPES) {
      assertEquals(
          IncrementalReindexState.getCatchUpStatus(catchUpState, harness.v3Alias(entity)),
          Optional.of(CatchUpStatus.COMPLETED));
    }
    assertTrue(IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, catchUpState));

    // The replay rewrites what the window holds; it adds nothing and loses nothing.
    assertEquals(harness.count(harness.v3Client, datasetAlias), 5L);
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("container")), 3L);
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("tag")), 2L);
  }

  @Test(dependsOnMethods = "catchUpReplaysTheFillWindowIntoV3")
  public void readFlipServesV3() throws Exception {
    withKeywordReadsOnV3(
        () -> {
          // Deploy N+1 routes keyword reads to V3.
          ESSearchDAO dao =
              new ESSearchDAO(
                  false,
                  harness.config,
                  null,
                  QueryFilterRewriteChain.EMPTY,
                  SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG);
          assertEquals(dao.docCount(harness.opContext, "dataset"), 5L);

          // A write that reaches V2 only does not move the count: the read path is V3's.
          harness.deliverToV2OnlyConsumers();
          harness.ingest(
              UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.v2only,PROD)"),
              "datasetProperties",
              new DatasetProperties().setName("v2 only"));
          harness.syncWrites();
          assertEquals(harness.count(harness.v2Client, harness.v2Index("dataset")), 6L);
          assertEquals(dao.docCount(harness.opContext, "dataset"), 5L);
        });
    // Back on a V2-serving deployment: the later deploys below run before any read flip.
  }

  @Test(dependsOnMethods = "readFlipServesV3")
  public void deployWithoutMappingChangesLeavesPopulatedV3IndicesAlone() throws Exception {
    // The steady state: a release that changes nothing about the V3 mapping must plan neither a
    // copy nor a fill. The builder's mapping and the one the cluster reports back differ only in
    // serialisation (array vs list values, implicit vs explicit object types), and the comparison
    // has to see through that or every deploy copies every V3 index.
    Set<String> backingBefore = harness.backingIndices(datasetAlias);
    Map<String, String> idsBefore = harness.idsByUrn(harness.v3Client, datasetAlias);

    UpgradeStepResult result = run(phase1(UNCHANGED_DEPLOY));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> state = harness.phase1State(UNCHANGED_DEPLOY);
    for (String entity : SEEDED_ENTITY_TYPES) {
      assertEquals(
          IncrementalReindexState.getStatus(state, harness.v3Alias(entity)),
          Optional.empty(),
          "an unchanged, populated V3 index gets no reindex or fill row: " + state);
    }
    assertEquals(harness.backingIndices(datasetAlias), backingBefore);
    assertEquals(harness.idsByUrn(harness.v3Client, datasetAlias), idsBefore);
  }

  @Test(dependsOnMethods = "deployWithoutMappingChangesLeavesPopulatedV3IndicesAlone")
  public void mappingChangeIsCopiedAndTheOldIndexStaysCurrentForRollback() throws Exception {
    String oldBackingIndex = only(harness.backingIndices(datasetAlias));
    Map<String, String> idsBefore = harness.idsByUrn(harness.v3Client, datasetAlias);
    // A release retypes a V3 root field on the dataset index. Put-mapping cannot change a type in
    // place, so Phase 1 must copy.
    harness.overrideV3FieldMapping(datasetAlias, "systemCreated", Map.of("type", "keyword"));

    UpgradeStepResult result = run(phase1(MAPPING_CHANGE_DEPLOY));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> state = harness.phase1State(MAPPING_CHANGE_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(state, datasetAlias), Optional.of(Status.COMPLETED));
    assertEquals(
        IncrementalReindexState.getIndexVersion(state, datasetAlias), EntityIndexVersion.V3);
    String nextIndex =
        IncrementalReindexState.get(state, datasetAlias, IncrementalReindexState.NEXT_INDEX_NAME)
            .orElseThrow();
    assertNotEquals(nextIndex, oldBackingIndex);
    assertEquals(
        IncrementalReindexState.get(
            state, datasetAlias, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of(oldBackingIndex));
    assertEquals(harness.backingIndices(datasetAlias), Set.of(nextIndex));
    assertTrue(harness.indexExists(harness.v3Client, nextIndex));
    if (harness.separateClusters()) {
      assertFalse(
          harness.indexExists(harness.v2Client, nextIndex),
          "the copy runs on the cluster that owns V3");
    }
    assertEquals(
        harness.idsByUrn(harness.v3Client, nextIndex),
        idsBefore,
        "the copy keeps every document under its id");
    for (String entity : SEEDED_ENTITY_TYPES) {
      assertEquals(
          IncrementalReindexState.getStatus(state, harness.v2Index(entity)),
          Optional.empty(),
          "an unchanged V2 index is left alone by the V3 mapping change");
    }
    for (String entity : List.of("container", "tag")) {
      assertEquals(
          IncrementalReindexState.getStatus(state, harness.v3Alias(entity)),
          Optional.empty(),
          "an unchanged V3 index is left alone by another index's mapping change");
    }

    // Consumers restarted after the swap repeat each V3 write into the swapped-out index.
    harness.restartDualWritingConsumers(MAPPING_CHANGE_DEPLOY);
    harness.ingest(
        POST_SWAP_DATASET, "datasetProperties", new DatasetProperties().setName("post swap"));
    harness.syncWrites();
    SearchHit live = document(nextIndex, POST_SWAP_DATASET);
    SearchHit rollbackCopy = document(oldBackingIndex, POST_SWAP_DATASET);
    assertEquals(rollbackCopy.getId(), live.getId());
    assertEquals(rollbackCopy.getSourceAsMap().get("urn"), live.getSourceAsMap().get("urn"));

    // Swap back: the old index serves everything, including what arrived after the swap.
    harness.swapAlias(datasetAlias, oldBackingIndex);
    assertEquals(harness.backingIndices(datasetAlias), Set.of(oldBackingIndex));
    assertEquals(
        harness.count(harness.v3Client, datasetAlias), harness.count(harness.v3Client, nextIndex));
    assertEquals(
        harness.count(
            harness.v3Client,
            datasetAlias,
            QueryBuilders.termQuery("urn", POST_SWAP_DATASET.toString())),
        1L);
  }

  @Test(dependsOnMethods = "mappingChangeIsCopiedAndTheOldIndexStaysCurrentForRollback")
  public void indexBuiltBeforeIdHashingIsRebuiltIntoANextIndex() throws Exception {
    harness.deliverToUpgradedConsumers();
    // A physical name of the V3 family, as a pre-hashing index renamed behind the alias would be;
    // index settings are resolved per index service from the name, so it must still read as V3.
    String legacyIndex = datasetAlias + "_legacy_1600000000000";
    long legacyDocuments = harness.count(harness.v3Client, datasetAlias);
    harness.replaceWithUnstampedIndex(
        datasetAlias, legacyIndex, SearchV3MigrationHarness::legacyDocumentId);
    assertEquals(harness.backingIndices(datasetAlias), Set.of(legacyIndex));
    assertEquals(harness.count(harness.v3Client, legacyIndex), legacyDocuments);

    // This deploy's consumers are serving before its Phase 1 runs, as they are whenever the
    // blocking upgrade has steps ahead of this one: they find no state for their version and have
    // to pick the fill up by polling once it is planned.
    harness.restartDualWritingConsumers(ID_REBUILD_DEPLOY);

    UpgradeStepResult phase1Result = run(phase1(ID_REBUILD_DEPLOY));

    assertEquals(phase1Result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> pending = harness.phase1State(ID_REBUILD_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(pending, datasetAlias),
        Optional.of(Status.BACKFILL_PENDING));
    String nextIndex =
        IncrementalReindexState.get(
                pending, datasetAlias, IncrementalReindexState.BACKFILL_TARGET_INDEX)
            .orElseThrow();
    assertNotEquals(nextIndex, datasetAlias, "a rebuild fills a fresh index, never the live one");
    assertEquals(
        IncrementalReindexState.get(
            pending, datasetAlias, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of(legacyIndex));
    assertEquals(
        harness.backingIndices(datasetAlias),
        Set.of(legacyIndex),
        "reads stay on the old index until the fill has finished");
    assertEquals(harness.count(harness.v3Client, nextIndex), 0L);
    for (String entity : List.of("container", "tag")) {
      assertEquals(
          IncrementalReindexState.getStatus(pending, harness.v3Alias(entity)),
          Optional.empty(),
          "an index whose ids are current is left alone");
    }

    // Stop the fill after its first dataset page, as a budget would. The resume re-reads that
    // page's last entity and writes it again, so the entities-read figure runs ahead of the index;
    // the swap must not be gated on it.
    UpgradeStepResult interruptedRun =
        run(fill(ID_REBUILD_DEPLOY, loaderStoppingAfterTheFirstDatasetPage()));
    harness.syncWrites();

    assertEquals(interruptedRun.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> midType = harness.phase1State(ID_REBUILD_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(midType, datasetAlias),
        Optional.of(Status.BACKFILL_IN_PROGRESS));
    BackfillCheckpoint cursor =
        IncrementalReindexState.getBackfillCheckpoint(midType, datasetAlias).orElseThrow();
    assertEquals(cursor.cursorEntity(), "dataset");
    assertTrue(
        cursor.cursorUrn().startsWith("urn:li:dataset:"), "cursor on the last dataset written");
    assertTrue(
        harness.count(harness.v3Client, nextIndex) > 0, "the interrupted run's pages landed");
    assertEquals(
        harness.backingIndices(datasetAlias), Set.of(legacyIndex), "no swap before completion");

    // A hard delete while the rebuild is in flight. The live writer deletes from the alias, which
    // still serves the old index, and catch-up cannot replay what storage no longer holds; the
    // consumers, whose poll has picked the fill up since Phase 1 planned it, also delete from the
    // index the fill is building.
    Urn deletedDataset = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.d1,PROD)");
    assertEquals(
        harness.count(
            harness.v3Client, nextIndex, QueryBuilders.termQuery("urn", deletedDataset.toString())),
        1L,
        "the interrupted run wrote the entity about to be deleted");
    harness.awaitFillTarget(datasetAlias);
    harness.entityService.deleteUrn(harness.opContext, deletedDataset);
    harness.syncWrites();
    // The alias still serves the legacy index, whose documents carry the old ids a live delete
    // cannot address; what the swap must not bring back is the copy the fill wrote.
    assertEquals(
        harness.count(
            harness.v3Client, nextIndex, QueryBuilders.termQuery("urn", deletedDataset.toString())),
        0L,
        "the live delete reached the index the fill is building");

    UpgradeStepResult fillResult = run(fill(ID_REBUILD_DEPLOY, harness.aspectIndexLoader()));
    harness.syncWrites();

    assertEquals(fillResult.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> completed = harness.phase1State(ID_REBUILD_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(completed, datasetAlias), Optional.of(Status.COMPLETED));
    assertEquals(harness.backingIndices(datasetAlias), Set.of(nextIndex));
    assertTrue(
        harness.indexExists(harness.v3Client, legacyIndex),
        "the swapped-out index is kept for rollback");
    assertTrue(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(completed, datasetAlias),
        "its documents keep the old ids, so live writes stay out of it after the swap");

    // The rebuild re-derives the corpus from primary storage: every dataset V2 knows, hashed ids.
    long expected = harness.count(harness.v2Client, harness.v2Index("dataset"));
    List<SearchHit> rebuilt = harness.hits(harness.v3Client, nextIndex);
    assertEquals(rebuilt.size(), expected);
    for (SearchHit hit : rebuilt) {
      String urn = SearchV3MigrationHarness.urnOf(hit);
      assertEquals(hit.getId(), harness.hashedDocumentId(UrnUtils.getUrn(urn)));
      assertNotEquals(hit.getId(), SearchV3MigrationHarness.legacyDocumentId(urn));
    }
    assertEquals(
        harness.count(
            harness.v3Client, nextIndex, QueryBuilders.termQuery("urn", deletedDataset.toString())),
        0L,
        "the resumed fill did not bring the deleted entity back");

    UpgradeStepResult catchUpResult = run(catchUp(ID_REBUILD_DEPLOY));
    harness.syncWrites();

    assertEquals(catchUpResult.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(
        harness.count(
            harness.v3Client, nextIndex, QueryBuilders.termQuery("urn", deletedDataset.toString())),
        0L,
        "catch-up re-derives its window from storage, where the entity no longer exists");
  }

  @Test(dependsOnMethods = "indexBuiltBeforeIdHashingIsRebuiltIntoANextIndex")
  public void concreteNamedIndexIsRetainedForRollbackWhenRebuilt() throws Exception {
    harness.deliverToUpgradedConsumers();
    // A V3 index created before indices were aliased owns the convention name itself. The alias
    // needs that name and nothing renames an index in place, so the swap has to delete it; the
    // fill first copies it to a physical name the rollback dual-write can keep current. Its
    // documents carry the running hasher's ids with no stamp recording that, as a writer-created
    // index does: rebuilt once to record the scheme, with rollback writes continuing after.
    long legacyDocuments = harness.count(harness.v3Client, datasetAlias);
    harness.replaceWithUnstampedIndex(
        datasetAlias, datasetAlias, urn -> harness.hashedDocumentId(UrnUtils.getUrn(urn)));
    assertEquals(
        harness.backingIndices(datasetAlias), Set.of(), "the live name is an index, not an alias");
    assertEquals(harness.count(harness.v3Client, datasetAlias), legacyDocuments);

    UpgradeStepResult phase1Result = run(phase1(CONCRETE_REBUILD_DEPLOY));

    assertEquals(phase1Result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> pending = harness.phase1State(CONCRETE_REBUILD_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(pending, datasetAlias),
        Optional.of(Status.BACKFILL_PENDING));
    assertFalse(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(pending, datasetAlias),
        "the sampled ids are the running hasher's, so rollback writes address its documents");
    String nextIndex =
        IncrementalReindexState.get(
                pending, datasetAlias, IncrementalReindexState.BACKFILL_TARGET_INDEX)
            .orElseThrow();
    assertEquals(
        IncrementalReindexState.get(
            pending, datasetAlias, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.empty(),
        "a concrete index has no backing index to record yet");

    // A live write during the fill lands on the concrete index; the retained copy must carry it.
    Urn writtenDuringRebuild =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.retained,PROD)");
    harness.ingest(
        writtenDuringRebuild,
        "datasetProperties",
        new DatasetProperties().setName("written during the rebuild"));
    harness.syncWrites();
    assertEquals(
        harness.count(
            harness.v3Client,
            datasetAlias,
            QueryBuilders.termQuery("urn", writtenDuringRebuild.toString())),
        1L);

    // Stop the fill after its first dataset page, which holds every dataset, then change one of
    // them: its properties are removed and a tag is added. The consumers delivering here carry no
    // write targets, as consumers that have not read this deploy's state do, so nothing mirrors
    // the removal into the index the fill is building and the page's copy of the properties stays
    // there; the tag write leaves a row in the catch-up window, so catch-up replays the entity and
    // must drop what storage no longer holds.
    UpgradeStepResult interruptedRun =
        run(fill(CONCRETE_REBUILD_DEPLOY, loaderStoppingAfterTheFirstDatasetPage()));
    harness.syncWrites();
    assertEquals(interruptedRun.result(), DataHubUpgradeState.SUCCEEDED);
    Urn changedDuringFill =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.d2,PROD)");
    assertEquals(
        sourceOf(harness.hits(harness.v3Client, nextIndex), changedDuringFill).get("name"),
        "dataset 2",
        "the interrupted run wrote the entity with its properties");
    harness.entityService.deleteAspect(
        harness.opContext, changedDuringFill.toString(), "datasetProperties", Map.of(), true);
    harness.ingest(
        changedDuringFill,
        "globalTags",
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    new TagAssociation().setTag(TagUrn.createFromString("urn:li:tag:split-t1")))));
    harness.syncWrites();
    assertNull(
        sourceOf(harness.hits(harness.v3Client, datasetAlias), changedDuringFill).get("name"),
        "the live index dropped the properties");

    UpgradeStepResult fillResult = run(fill(CONCRETE_REBUILD_DEPLOY, harness.aspectIndexLoader()));
    harness.syncWrites();

    assertEquals(fillResult.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> completed = harness.phase1State(CONCRETE_REBUILD_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(completed, datasetAlias), Optional.of(Status.COMPLETED));
    String retained =
        IncrementalReindexState.get(
                completed, datasetAlias, IncrementalReindexState.OLD_BACKING_INDEX_NAME)
            .orElseThrow();
    assertNotEquals(retained, datasetAlias);
    assertTrue(
        retained.startsWith(datasetAlias + "_"), "the copy is a physical index of the family");
    assertEquals(harness.backingIndices(datasetAlias), Set.of(nextIndex));
    assertTrue(
        harness.indexExists(harness.v3Client, retained),
        "the swap keeps the copy the alias can be swapped back to");
    assertEquals(
        harness.count(harness.v3Client, retained),
        legacyDocuments + 1,
        "the copy holds the legacy documents and the write that arrived during the fill");
    assertEquals(
        harness.count(
            harness.v3Client,
            retained,
            QueryBuilders.termQuery("urn", writtenDuringRebuild.toString())),
        1L);

    // The copy holds each entity under the running hasher's ids, so consumers brought up after
    // the swap keep it current and a rollback to it returns to the present, not to the swap.
    assertFalse(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(completed, datasetAlias),
        "the row records that the copy may receive live writes");
    harness.restartDualWritingConsumers(CONCRETE_REBUILD_DEPLOY);
    Urn afterSwap =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,split.postretain,PROD)");
    harness.ingest(
        afterSwap, "datasetProperties", new DatasetProperties().setName("after the swap"));
    harness.syncWrites();
    assertEquals(
        harness.count(
            harness.v3Client, nextIndex, QueryBuilders.termQuery("urn", afterSwap.toString())),
        1L);
    assertEquals(
        harness.count(
            harness.v3Client, retained, QueryBuilders.termQuery("urn", afterSwap.toString())),
        1L,
        "the rollback write reaches the retained copy under the same id");
    assertEquals(
        harness.count(harness.v3Client, retained),
        legacyDocuments + 2,
        "the retained copy holds the pre-swap content and the write since");

    // The page's copy of the removed properties outlived the swap: nothing mirrored the removal,
    // and the resume rewrote only the page's last entity. Catch-up replays the entity for the tag
    // it gained, and the replay drops the properties from the document along with the root field
    // they projected. The retained copy was taken after the change and needs no repair.
    assertEquals(
        sourceOf(harness.hits(harness.v3Client, nextIndex), changedDuringFill).get("name"),
        "dataset 2",
        "before catch-up the next index still holds the properties the page wrote");
    UpgradeStepResult catchUpResult = run(catchUp(CONCRETE_REBUILD_DEPLOY));
    harness.syncWrites();

    assertEquals(catchUpResult.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, Object> caughtUp =
        sourceOf(harness.hits(harness.v3Client, nextIndex), changedDuringFill);
    Map<?, ?> aspects = (Map<?, ?>) caughtUp.get("_aspects");
    assertNull(caughtUp.get("name"), "the removed aspect's root field is gone");
    assertNull(aspects.get("datasetProperties"), "and its entry under _aspects");
    assertNotNull(aspects.get("globalTags"), "the aspect it gained is there");
  }

  @Test(dependsOnMethods = "concreteNamedIndexIsRetainedForRollbackWhenRebuilt")
  public void movingV3ToAnotherClusterFillsItThereAndLeavesTheOldClusterAlone() throws Exception {
    SearchV3MigrationHarness before = harness;
    SearchClientShim<?> clusterV3Leaves = before.v3Client;
    long documentsLeftBehind = before.count(clusterV3Leaves, datasetAlias);
    long expected = before.count(before.v2Client, before.v2Index("dataset"));

    // A deploy routes searchV3 at a cluster that has never held it. Reads stay on V2 throughout.
    harness = before.redeployWithV3OnTheOtherCluster();
    // The destination cluster names V3 under its own prefix, so the alias can differ from the one
    // the corpus lived under; everything below on the new cluster uses the new name.
    String movedAlias = harness.v3Alias("dataset");
    assertFalse(
        harness.indexExists(harness.v3Client, movedAlias),
        "the cluster V3 moves to starts without the index");

    UpgradeStepResult phase1Result = run(phase1(CLUSTER_MOVE_DEPLOY));

    assertEquals(phase1Result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> pending = harness.phase1State(CLUSTER_MOVE_DEPLOY);
    for (String entity : SEEDED_ENTITY_TYPES) {
      String alias = harness.v3Alias(entity);
      assertEquals(
          IncrementalReindexState.getStatus(pending, alias), Optional.of(Status.BACKFILL_PENDING));
      assertEquals(
          IncrementalReindexState.get(
              pending, alias, IncrementalReindexState.BACKFILL_TARGET_INDEX),
          Optional.of(alias));
      assertTrue(harness.indexExists(harness.v3Client, alias));
      assertEquals(harness.count(harness.v3Client, alias), 0L);
      assertEquals(
          IncrementalReindexState.getStatus(pending, harness.v2Index(entity)),
          Optional.empty(),
          "moving V3 does not touch the V2 indices");
    }

    UpgradeStepResult fillResult = run(fill(CLUSTER_MOVE_DEPLOY, harness.aspectIndexLoader()));
    harness.syncWrites();
    UpgradeStepResult catchUpResult = run(catchUp(CLUSTER_MOVE_DEPLOY));
    harness.syncWrites();

    assertEquals(fillResult.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(catchUpResult.result(), DataHubUpgradeState.SUCCEEDED);
    assertTrue(
        IncrementalReindexState.isCatchUpCompleteForAllIndices(
            harness.phase1State(CLUSTER_MOVE_DEPLOY), harness.catchUpState(CLUSTER_MOVE_DEPLOY)));
    List<SearchHit> moved = harness.hits(harness.v3Client, movedAlias);
    assertEquals(
        moved.size(), expected, "the new cluster holds every dataset primary storage knows");
    for (SearchHit hit : moved) {
      assertEquals(
          hit.getId(),
          harness.hashedDocumentId(UrnUtils.getUrn(SearchV3MigrationHarness.urnOf(hit))));
    }
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("container")), 3L);
    assertEquals(harness.count(harness.v3Client, harness.v3Alias("tag")), 2L);

    // Live writes now land on the new cluster only.
    harness.deliverToUpgradedConsumers();
    harness.ingest(
        POST_MOVE_DATASET, "datasetProperties", new DatasetProperties().setName("post move"));
    harness.syncWrites();
    assertEquals(
        harness.count(
            harness.v3Client,
            movedAlias,
            QueryBuilders.termQuery("urn", POST_MOVE_DATASET.toString())),
        1L);

    // The cluster V3 left keeps its indices as they were, for the runbook to remove.
    assertEquals(before.count(clusterV3Leaves, datasetAlias), documentsLeftBehind);
    assertEquals(
        before.count(
            clusterV3Leaves,
            datasetAlias,
            QueryBuilders.termQuery("urn", POST_MOVE_DATASET.toString())),
        0L);
  }

  /**
   * Phase 1 of a deploy: a fresh system-update process that sees the live clusters, not a cache.
   */
  private BuildIndicesIncrementalStep phase1(String deployVersion) {
    IndexUtils.clearReindexConfigCache();
    return new BuildIndicesIncrementalStep(
        harness.opContext,
        List.of(harness.searchService),
        Set.of(),
        harness.entityService,
        deployVersion,
        harness.buildIndicesConfig,
        harness.updateIndicesService,
        new V3DocumentIdScheme(new Sha256UrnEntityDocumentIdHasher()),
        true);
  }

  /** A loader that stops after the first dataset page it writes, as a budget would. */
  private AspectIndexLoader loaderStoppingAfterTheFirstDatasetPage() {
    AspectIndexLoader realLoader = harness.aspectIndexLoader();
    AspectIndexLoader interruptingLoader = spy(realLoader);
    AtomicBoolean stopped = new AtomicBoolean(false);
    doAnswer(
            invocation -> {
              LoadIndicesArgs args = invocation.getArgument(1);
              if (!"urn:li:dataset:%".equals(args.urnLike) || stopped.get()) {
                return invocation.callRealMethod();
              }
              AspectIndexLoader.BatchListener listener = invocation.getArgument(3);
              AspectIndexLoader.BatchListener stopAfterThisBatch =
                  (lastUrn, rows, urns) -> {
                    listener.onBatch(lastUrn, rows, urns);
                    stopped.set(true);
                    throw new AspectIndexLoader.DeadlineExceeded(lastUrn, rows);
                  };
              return realLoader.load(
                  invocation.getArgument(0),
                  args,
                  invocation.getArgument(2),
                  stopAfterThisBatch,
                  invocation.getArgument(4));
            })
        .when(interruptingLoader)
        .load(any(), any(), any(), any(), any());
    return interruptingLoader;
  }

  /** The source of {@code urn}'s document among {@code hits}. */
  private static Map<String, Object> sourceOf(List<SearchHit> hits, Urn urn) {
    return hits.stream()
        .filter(hit -> urn.toString().equals(SearchV3MigrationHarness.urnOf(hit)))
        .map(SearchHit::getSourceAsMap)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no document for " + urn));
  }

  private BackfillEntityIndicesStep fill(String deployVersion, AspectIndexLoader loader) {
    return new BackfillEntityIndicesStep(
        harness.opContext,
        harness.entityService,
        List.of(harness.searchService),
        Set.of(),
        deployVersion,
        harness.buildIndicesConfig,
        loader,
        harness.updateIndicesService);
  }

  /** Runs {@code check} with keyword reads routed to V3, then puts the flag back. */
  private void withKeywordReadsOnV3(Check check) throws Exception {
    harness.config.getEntityIndex().getV3().setKeywordReadEnabled(true);
    try {
      check.run();
    } finally {
      harness.config.getEntityIndex().getV3().setKeywordReadEnabled(false);
    }
  }

  @FunctionalInterface
  private interface Check {
    void run() throws Exception;
  }

  private IncrementalReindexCatchUpStep catchUp(String deployVersion) {
    return new IncrementalReindexCatchUpStep(
        harness.opContext,
        harness.entityService,
        harness.aspectDao,
        List.of(harness.searchService),
        Set.of(),
        deployVersion,
        harness.buildIndicesConfig);
  }

  private UpgradeStepResult run(UpgradeStep step) {
    return step.executable().apply(harness.upgradeContext());
  }

  private SearchHit document(String index, Urn urn) throws Exception {
    return only(
        harness.hits(harness.v3Client, index).stream()
            .filter(hit -> urn.toString().equals(SearchV3MigrationHarness.urnOf(hit)))
            .collect(Collectors.toList()));
  }

  private static <T> T only(Collection<T> items) {
    assertEquals(items.size(), 1, "expected exactly one of " + items);
    return items.iterator().next();
  }
}
