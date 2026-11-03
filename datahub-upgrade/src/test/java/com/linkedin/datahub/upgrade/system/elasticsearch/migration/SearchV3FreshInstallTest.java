package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BackfillEntityIndicesStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesIncrementalStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexCatchUpStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.CatchUpStatus;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.Status;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A new install that never had V2: V3 is written and read from the first boot. There is no
 * historical corpus, so Phase 1 creates each aliased V3 index as a fill target, the fill and
 * catch-up complete with nothing to copy, and reads route to V3 from the first boot.
 */
public class SearchV3FreshInstallTest {

  private static final String FIRST_DEPLOY = "0.0.0-fresh-1";
  private static final List<String> SEARCHABLE_ENTITY_TYPES =
      List.of("container", "dataset", "tag");
  private static final Urn FIRST_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fresh.first,PROD)");

  private SearchV3MigrationHarness harness;
  private String datasetAlias;

  @BeforeClass(alwaysRun = true)
  public void startCluster() throws Exception {
    SearchV3MigrationHarness.requireOptIn();
    harness =
        SearchV3MigrationHarness.start(
            SearchV3MigrationHarness.Topology.SAME_CLUSTER,
            SearchV3MigrationHarness.Generations.V3_ONLY);
    datasetAlias = harness.v3Alias("dataset");
  }

  @AfterClass(alwaysRun = true)
  public void stopCluster() {
    if (harness != null) {
      harness.close();
    }
  }

  @Test
  public void phase1CreatesOnlyTheAliasedV3Indices() throws Exception {
    IndexUtils.clearReindexConfigCache();
    UpgradeStepResult result =
        run(
            new BuildIndicesIncrementalStep(
                harness.opContext,
                List.of(harness.searchService),
                Set.of(),
                harness.entityService,
                FIRST_DEPLOY,
                harness.buildIndicesConfig,
                harness.updateIndicesService,
                new V3DocumentIdScheme(new Sha256UrnEntityDocumentIdHasher()),
                true));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> state = harness.phase1State(FIRST_DEPLOY);
    for (String entity : SEARCHABLE_ENTITY_TYPES) {
      String alias = harness.v3Alias(entity);
      assertTrue(harness.indexExists(harness.v3Client, alias));
      assertEquals(harness.count(harness.v3Client, alias), 0L);
      assertFalse(
          harness.indexExists(harness.v2Client, harness.v2Index(entity)),
          "a V3-only install never creates " + entity + "'s V2 index");
      assertEquals(
          IncrementalReindexState.getStatus(state, alias), Optional.of(Status.BACKFILL_PENDING));
      assertEquals(IncrementalReindexState.getIndexVersion(state, alias), EntityIndexVersion.V3);
      assertEquals(
          IncrementalReindexState.get(state, alias, IncrementalReindexState.BACKFILL_TARGET_INDEX),
          Optional.of(alias));
    }
  }

  @Test(dependsOnMethods = "phase1CreatesOnlyTheAliasedV3Indices")
  public void fillAndCatchUpCompleteWithNothingToCopy() throws Exception {
    UpgradeStepResult fillResult =
        run(
            new BackfillEntityIndicesStep(
                harness.opContext,
                harness.entityService,
                List.of(harness.searchService),
                Set.of(),
                FIRST_DEPLOY,
                harness.buildIndicesConfig,
                harness.aspectIndexLoader(),
                harness.updateIndicesService));
    harness.syncWrites();

    assertEquals(fillResult.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> phase1State = harness.phase1State(FIRST_DEPLOY);
    assertEquals(
        IncrementalReindexState.getStatus(phase1State, datasetAlias),
        Optional.of(Status.COMPLETED));
    assertEquals(
        IncrementalReindexState.getBackfillUrnsProcessed(phase1State, datasetAlias).orElse(0L), 0L);
    assertEquals(harness.count(harness.v3Client, datasetAlias), 0L);

    UpgradeStepResult catchUpResult =
        run(
            new IncrementalReindexCatchUpStep(
                harness.opContext,
                harness.entityService,
                harness.aspectDao,
                List.of(harness.searchService),
                Set.of(),
                FIRST_DEPLOY,
                harness.buildIndicesConfig));

    assertEquals(catchUpResult.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> catchUpState = harness.catchUpState(FIRST_DEPLOY);
    assertTrue(
        IncrementalReindexState.getCatchUpStatus(catchUpState, datasetAlias)
            .map(CatchUpStatus::isTerminal)
            .orElse(false),
        "catch-up over an empty window reaches a terminal status");
    assertTrue(
        IncrementalReindexState.isCatchUpCompleteForAllIndices(
            harness.phase1State(FIRST_DEPLOY), catchUpState));
  }

  @Test(dependsOnMethods = "fillAndCatchUpCompleteWithNothingToCopy")
  public void readsServeV3FromBoot() throws Exception {
    ESSearchDAO dao =
        new ESSearchDAO(
            false,
            harness.config,
            null,
            QueryFilterRewriteChain.EMPTY,
            SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG);
    assertEquals(dao.docCount(harness.opContext, "dataset"), 0L, "an empty V3 index is readable");

    harness.deliverToUpgradedConsumers();
    harness.ingest(FIRST_DATASET, "datasetProperties", new DatasetProperties().setName("first"));
    harness.syncWrites();

    assertEquals(dao.docCount(harness.opContext, "dataset"), 1L);
    assertEquals(harness.count(harness.v3Client, datasetAlias), 1L);
    assertFalse(
        harness.indexExists(harness.v2Client, harness.v2Index("dataset")),
        "no V2 index appears as a side effect of the write");
  }

  private UpgradeStepResult run(UpgradeStep step) {
    return step.executable().apply(harness.upgradeContext());
  }
}
