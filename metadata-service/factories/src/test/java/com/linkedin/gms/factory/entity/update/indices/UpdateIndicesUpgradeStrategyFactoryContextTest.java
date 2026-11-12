package com.linkedin.gms.factory.entity.update.indices;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.search.SettingsBuilderFactory;
import com.linkedin.gms.factory.search.SingleClusterSearchTestConfiguration;
import com.linkedin.gms.factory.search.filter.QueryFilterRewriterChainFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Loads {@link UpdateIndicesUpgradeStrategyFactory} with the production {@code application.yaml},
 * the real search factories it pulls in, and a {@link SystemEntityClient} holding the Phase 1 state
 * a zero-downtime upgrade persists. Checks what {@code
 * elasticsearch.buildIndices.rollbackDualWriteEnabled} does to the MCL rollback dual-write path.
 */
public class UpdateIndicesUpgradeStrategyFactoryContextTest {

  private static final String ROLLBACK_DUAL_WRITE =
      "elasticsearch.buildIndices.rollbackDualWriteEnabled";

  private static final GitVersion GIT_VERSION =
      new GitVersion("1.2.3", "abcdef0", Optional.empty());

  /** Swapped by Phase 1, old backing index still available for rollback. */
  private static final String SWAPPED_INDEX = "datasetindex_v2";

  private static final String OLD_BACKING_INDEX = "datasetindex_v2_1700000000000";

  /** Fill in flight, building a new index for its alias. */
  private static final String FILLING_INDEX = "chartindex_v2";

  private static final String FILL_TARGET_INDEX = "chartindex_v2_1700000000001";

  private static final OperationContext SYSTEM_OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private SystemEntityClient systemEntityClient;

  @BeforeMethod
  public void setUp() throws Exception {
    systemEntityClient = mock(SystemEntityClient.class);
    when(systemEntityClient.batchGetV2NoCache(
            any(OperationContext.class),
            eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
            eq(Set.of(upgradeUrn())),
            anySet()))
        .thenReturn(Map.of(upgradeUrn(), phase1StateResponse()));
  }

  private ApplicationContextRunner contextRunner() {
    EntityRegistry entityRegistry = SYSTEM_OP_CONTEXT.getEntityRegistry();
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            UpdateIndicesUpgradeStrategyFactory.class,
            IndexConventionFactory.class,
            SettingsBuilderFactory.class,
            MappingsBuilderFactory.class,
            QueryFilterRewriterChainFactory.class,
            SearchDocumentTransformerFactory.class,
            SingleClusterSearchTestConfiguration.class)
        .withBean("systemEntityClient", SystemEntityClient.class, () -> systemEntityClient)
        .withBean("systemOperationContext", OperationContext.class, () -> SYSTEM_OP_CONTEXT)
        .withBean("entityRegistry", EntityRegistry.class, () -> entityRegistry)
        .withBean("gitVersion", GitVersion.class, () -> GIT_VERSION)
        .withBean(
            "searchClientShim",
            SearchClientShim.class,
            UpdateIndicesUpgradeStrategyFactoryContextTest::searchClient)
        .withBean(
            "baseElasticSearchComponents",
            BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class,
            () ->
                mock(
                    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class,
                    RETURNS_MOCKS));
  }

  @Test
  public void testRollbackDualWriteOff() {
    // Set explicitly: the yaml default reads
    // ELASTICSEARCH_BUILD_INDICES_ROLLBACK_DUAL_WRITE_ENABLED
    // and ZDU_STAGE_20 from the environment.
    contextRunner()
        .withPropertyValues(ROLLBACK_DUAL_WRITE + "=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean("updateIndicesUpgradeStrategy");

              IncrementalReindexWriteTargets targets =
                  context.getBean(
                      "incrementalReindexWriteTargets", IncrementalReindexWriteTargets.class);
              // Rollback targets are not loaded, but deletes are still mirrored into a fill.
              assertThat(targets.oldBackingIndexFor(SWAPPED_INDEX)).isEmpty();
              assertThat(targets.fillTargetFor(FILLING_INDEX)).contains(FILL_TARGET_INDEX);
            });
  }

  @Test
  public void testRollbackDualWriteOnLoadsTheSwappedIndexAndPersistsItsStartTime()
      throws Exception {
    contextRunner()
        .withPropertyValues(ROLLBACK_DUAL_WRITE + "=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              IncrementalReindexWriteTargets targets =
                  context.getBean(
                      "incrementalReindexWriteTargets", IncrementalReindexWriteTargets.class);
              UpdateIndicesStrategy strategy =
                  context.getBean("updateIndicesUpgradeStrategy", UpdateIndicesStrategy.class);

              assertThat(strategy).isInstanceOf(UpdateIndicesUpgradeStrategy.class);
              assertThat(strategy.isEnabled()).isTrue();
              assertThat(targets.oldBackingIndexFor(SWAPPED_INDEX)).contains(OLD_BACKING_INDEX);
              assertThat(targets.fillTargetFor(FILLING_INDEX)).contains(FILL_TARGET_INDEX);

              // The first rollback write records dualWriteStartTime on the same upgrade result,
              // which Phase 2 reads as the end of its catch-up window.
              long before = System.currentTimeMillis();
              targets.recordDualWriteStarted(SWAPPED_INDEX);
              long after = System.currentTimeMillis();
              ArgumentCaptor<MetadataChangeProposal> proposal =
                  ArgumentCaptor.forClass(MetadataChangeProposal.class);
              verify(systemEntityClient)
                  .ingestProposal(any(OperationContext.class), proposal.capture(), anyBoolean());
              assertThat(proposal.getValue().getEntityUrn()).isEqualTo(upgradeUrn());
              DataHubUpgradeResult written =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getValue().getAspect().getValue(),
                      proposal.getValue().getAspect().getContentType(),
                      DataHubUpgradeResult.class);
              assertThat(
                      Long.parseLong(
                          written
                              .getResult()
                              .get(
                                  IncrementalReindexState.key(
                                      SWAPPED_INDEX,
                                      IncrementalReindexState.DUAL_WRITE_START_TIME))))
                  .isBetween(before, after);
              assertThat(written.getResult())
                  .containsEntry(
                      IncrementalReindexState.key(
                          SWAPPED_INDEX, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
                      OLD_BACKING_INDEX);
            });
  }

  /** The id system-update's incremental BuildIndices step persists its Phase 1 state under. */
  private static Urn upgradeUrn() {
    String revision = Optional.ofNullable(System.getenv("DATAHUB_REVISION")).orElse("0");
    return BootstrapStep.getUpgradeUrn(
        IncrementalReindexState.UPGRADE_ID_PREFIX
            + "_"
            + GIT_VERSION.getVersion()
            + "-"
            + revision);
  }

  private static EntityResponse phase1StateResponse() {
    StringMap state = new StringMap();
    state.put(
        IncrementalReindexState.key(SWAPPED_INDEX, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.COMPLETED.name());
    state.put(
        IncrementalReindexState.key(SWAPPED_INDEX, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        OLD_BACKING_INDEX);
    state.put(
        IncrementalReindexState.key(FILLING_INDEX, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.BACKFILL_IN_PROGRESS.name());
    state.put(
        IncrementalReindexState.key(FILLING_INDEX, IncrementalReindexState.BACKFILL_TARGET_INDEX),
        FILL_TARGET_INDEX);

    DataHubUpgradeResult result =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.SUCCEEDED).setResult(state);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(result.data()))
            .setSystemMetadata(new SystemMetadata().setVersion("1")));
    return new EntityResponse()
        .setEntityName(Constants.DATA_HUB_UPGRADE_ENTITY_NAME)
        .setUrn(upgradeUrn())
        .setAspects(aspects);
  }

  private static SearchClientShim<?> searchClient() {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(client.partialNgramConfig()).thenReturn(Es8SearchClientShim.PARTIAL_NGRAM_CONFIG);
    return client;
  }
}
