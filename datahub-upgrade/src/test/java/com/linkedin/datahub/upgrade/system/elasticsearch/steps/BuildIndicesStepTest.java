package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BuildIndicesStepTest {

  @BeforeMethod
  @AfterMethod
  public void clearCache() {
    IndexUtils.clearReindexConfigCache();
  }

  @Test
  public void testParallelReindexUsesEachBuilderWhenClientsAreReused() throws Exception {
    SearchClientShim<?> sharedClient = mock(SearchClientShim.class);
    ESIndexBuilder v2Builder = mock(ESIndexBuilder.class);
    ESIndexBuilder v3Builder = mock(ESIndexBuilder.class);
    when(v2Builder.getSearchClient()).thenAnswer(invocation -> sharedClient);
    when(v3Builder.getSearchClient()).thenAnswer(invocation -> sharedClient);
    when(v2Builder.buildIndex(any(), any()))
        .thenReturn(ReindexResult.NOT_REINDEXED_NOTHING_APPLIED);
    when(v3Builder.buildIndex(any(), any()))
        .thenReturn(ReindexResult.NOT_REINDEXED_NOTHING_APPLIED);

    ReindexConfig v2Config = config("datasetindex_v2");
    ReindexConfig v3Config = config("datasetindex_v3");
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    UpgradeStepResult result =
        runParallelStep(
            opContext,
            service(opContext, "datasetindex_v2", v2Config, v2Builder),
            service(opContext, "datasetindex_v3", v3Config, v3Builder));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(v2Builder).buildIndex(opContext, v2Config);
    verify(v3Builder).buildIndex(opContext, v3Config);
    verify(v2Builder, never()).buildIndex(opContext, v3Config);
    verify(v3Builder, never()).buildIndex(opContext, v2Config);
  }

  @Test
  public void testParallelReindexRunsDistinctClientsConcurrently() throws Exception {
    SearchClientShim<?> clientA = mock(SearchClientShim.class);
    SearchClientShim<?> clientB = mock(SearchClientShim.class);
    ESIndexBuilder builderA = mock(ESIndexBuilder.class);
    ESIndexBuilder builderB = mock(ESIndexBuilder.class);
    when(builderA.getSearchClient()).thenAnswer(invocation -> clientA);
    when(builderB.getSearchClient()).thenAnswer(invocation -> clientB);

    CyclicBarrier barrier = new CyclicBarrier(2);
    when(builderA.buildIndex(any(), any()))
        .thenAnswer(
            invocation -> {
              barrier.await(5, TimeUnit.SECONDS);
              return ReindexResult.NOT_REINDEXED_NOTHING_APPLIED;
            });
    when(builderB.buildIndex(any(), any()))
        .thenAnswer(
            invocation -> {
              barrier.await(5, TimeUnit.SECONDS);
              return ReindexResult.NOT_REINDEXED_NOTHING_APPLIED;
            });

    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    UpgradeStepResult result =
        runParallelStep(
            opContext,
            service(opContext, "datasetindex_v2", config("datasetindex_v2"), builderA),
            service(opContext, "datasetindex_v3", config("datasetindex_v3"), builderB));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testParallelReindexKeepsSharedClientBuildersSequential() throws Exception {
    SearchClientShim<?> clientA = mock(SearchClientShim.class);
    SearchClientShim<?> clientB = mock(SearchClientShim.class);
    ESIndexBuilder a1 = mock(ESIndexBuilder.class);
    ESIndexBuilder a2 = mock(ESIndexBuilder.class);
    ESIndexBuilder b1 = mock(ESIndexBuilder.class);
    when(a1.getSearchClient()).thenAnswer(invocation -> clientA);
    when(a2.getSearchClient()).thenAnswer(invocation -> clientA);
    when(b1.getSearchClient()).thenAnswer(invocation -> clientB);

    AtomicInteger concurrentA = new AtomicInteger();
    AtomicInteger maxConcurrentA = new AtomicInteger();
    AtomicInteger concurrentAll = new AtomicInteger();
    AtomicInteger maxConcurrentAll = new AtomicInteger();

    org.mockito.stubbing.Answer<ReindexResult> track =
        invocation -> {
          int all = concurrentAll.incrementAndGet();
          maxConcurrentAll.accumulateAndGet(all, Math::max);
          try {
            Thread.sleep(150);
            return ReindexResult.NOT_REINDEXED_NOTHING_APPLIED;
          } finally {
            concurrentAll.decrementAndGet();
          }
        };
    org.mockito.stubbing.Answer<ReindexResult> trackA =
        invocation -> {
          int onA = concurrentA.incrementAndGet();
          maxConcurrentA.accumulateAndGet(onA, Math::max);
          try {
            return track.answer(invocation);
          } finally {
            concurrentA.decrementAndGet();
          }
        };

    when(a1.buildIndex(any(), any())).thenAnswer(trackA);
    when(a2.buildIndex(any(), any())).thenAnswer(trackA);
    when(b1.buildIndex(any(), any())).thenAnswer(track);

    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    UpgradeStepResult result =
        runParallelStep(
            opContext,
            service(opContext, "datasetindex_v2", config("datasetindex_v2"), a1),
            service(opContext, "chartindex_v2", config("chartindex_v2"), a2),
            service(opContext, "datasetindex_v3", config("datasetindex_v3"), b1));

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(maxConcurrentA.get(), 1);
    assertTrue(maxConcurrentAll.get() >= 2);
  }

  private static ReindexConfig config(String name) {
    ReindexConfig reindexConfig = mock(ReindexConfig.class);
    when(reindexConfig.name()).thenReturn(name);
    when(reindexConfig.requiresReindex()).thenReturn(false);
    return reindexConfig;
  }

  private static ElasticSearchIndexed service(
      OperationContext opContext,
      String indexName,
      ReindexConfig reindexConfig,
      ESIndexBuilder builder)
      throws Exception {
    ElasticSearchIndexed indexed = mock(ElasticSearchIndexed.class);
    when(indexed.buildReindexConfigs(eq(opContext), any())).thenReturn(List.of(reindexConfig));
    when(indexed.getIndexBuilder(indexName)).thenReturn(builder);
    return indexed;
  }

  private static UpgradeStepResult runParallelStep(
      OperationContext opContext, ElasticSearchIndexed... services) {
    BuildIndicesConfiguration parallel =
        BuildIndicesConfiguration.builder().enableParallelReindex(true).build();
    ElasticSearchConfiguration esConfig =
        ElasticSearchConfiguration.builder().buildIndices(parallel).build();
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getElasticSearch()).thenReturn(esConfig);
    UpgradeContext upgradeContext = mock(UpgradeContext.class);
    when(upgradeContext.opContext()).thenReturn(opContext);
    return new BuildIndicesStep(List.of(services), Set.of(), configurationProvider)
        .executable()
        .apply(upgradeContext);
  }
}
