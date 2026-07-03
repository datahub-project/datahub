package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CleanIndicesStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String OLD_BACKING = "dataset_datasetprofileaspect_v1_old";

  @BeforeMethod
  public void setup() {
    IndexUtils.clearReindexConfigCache();
  }

  @AfterMethod
  public void teardown() {
    IndexUtils.clearReindexConfigCache();
  }

  @Test
  public void testIncrementalReindexDisabledUsesEmptyExcludeSet() {
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    SearchClientShim<?> searchClient = mock(SearchClientShim.class);
    ElasticSearchConfiguration esConfig = mock(ElasticSearchConfiguration.class);
    EntityService<?> entityService = mock(EntityService.class);
    UpgradeContext upgradeContext = mock(UpgradeContext.class);
    Upgrade upgrade = mock(Upgrade.class);

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);

    BuildIndicesConfiguration buildIndicesConfig = new BuildIndicesConfiguration();
    buildIndicesConfig.setIncrementalReindexEnabled(false);

    CleanIndicesStep step =
        new CleanIndicesStep(
            searchClient,
            esConfig,
            List.of(),
            Set.of(),
            entityService,
            UPGRADE_VERSION,
            buildIndicesConfig);

    try (MockedStatic<ESIndexBuilder> esIndexBuilder = Mockito.mockStatic(ESIndexBuilder.class)) {
      UpgradeStepResult result = step.executable().apply(upgradeContext);
      assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
      verify(upgrade, never()).getUpgradeResult(any(), any(), any());
      esIndexBuilder.verifyNoInteractions();
    }
  }

  @Test
  public void testIncrementalReindexEnabledLoadsProtectedIndices() throws Exception {
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    SearchClientShim<?> searchClient = mock(SearchClientShim.class);
    ElasticSearchConfiguration esConfig = mock(ElasticSearchConfiguration.class);
    EntityService<?> entityService = mock(EntityService.class);
    UpgradeContext upgradeContext = mock(UpgradeContext.class);
    Upgrade upgrade = mock(Upgrade.class);
    ElasticSearchIndexed indexedService = mock(ElasticSearchIndexed.class);
    ReindexConfig reindexConfig = mock(ReindexConfig.class);

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(reindexConfig));

    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            "dataset_datasetprofileaspect_v1",
            "dataset_datasetprofileaspect_v1_next",
            OLD_BACKING,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(
            phase1State, "dataset_datasetprofileaspect_v1", 2000L);

    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));

    when(upgrade.getUpgradeResult(any(), any(), eq(entityService)))
        .thenAnswer(
            invocation -> {
              String urn = invocation.getArgument(1, com.linkedin.common.urn.Urn.class).toString();
              if (urn.contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              return Optional.empty();
            });

    BuildIndicesConfiguration buildIndicesConfig = new BuildIndicesConfiguration();
    buildIndicesConfig.setIncrementalReindexEnabled(true);

    CleanIndicesStep step =
        new CleanIndicesStep(
            searchClient,
            esConfig,
            List.of(indexedService),
            Set.of(),
            entityService,
            UPGRADE_VERSION,
            buildIndicesConfig);

    try (MockedStatic<ESIndexBuilder> esIndexBuilder = Mockito.mockStatic(ESIndexBuilder.class)) {
      UpgradeStepResult result = step.executable().apply(upgradeContext);
      assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
      esIndexBuilder.verify(
          () ->
              ESIndexBuilder.cleanOrphanedIndices(
                  eq(searchClient),
                  eq(opContext),
                  eq(esConfig),
                  eq(reindexConfig),
                  eq(Set.of(OLD_BACKING))));
    }
  }
}
