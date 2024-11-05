package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeManager;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.testng.SystemStub;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
public class DataHubUsageReportingTest {
  static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final String TEST_VALUES_ENV = "DATAHUB_USAGE_REPORTING_BOOTSTRAP_VALUES";

  @SystemStub private EnvironmentVariables environmentVariables;

  @Test
  public void testUsageReportingTemplateMCP() throws Exception {
    environmentVariables.set(
        TEST_VALUES_ENV,
        OP_CONTEXT
            .getObjectMapper()
            .writeValueAsString(
                Map.of(
                    "ingestion",
                        Map.of(
                            "name",
                            "test-usage-reporting",
                            "extra_pip_requirements",
                            "[\"foo\",\"bar\"]"),
                    "schedule", Map.of("timezone", "America/Chicago", "interval", "9 9 * * *"),
                    "freshness_factors",
                        List.of(
                            Map.of("age_in_days", "[0, 1]", "value", "1"),
                            Map.of("age_in_days", "[1, 2]", "value", "2"),
                            Map.of("age_in_days", "[2, 3]", "value", "3")),
                    "usage_percentile_factors",
                        List.of(
                            Map.of("percentile", "[0, 10]", "value", "4"),
                            Map.of("percentile", "[10, 80]", "value", "5"),
                            Map.of("percentile", "[80, 100]", "value", "6")),
                    "regexp_based_factors",
                        List.of(
                            Map.of("regexp", ".*A.*", "value", "0.1"),
                            Map.of("regexp", ".*B.*", "value", "0.2"),
                            Map.of("regexp", ".*C.*", "value", "0.3")))));

    final EntityService<?> entityService = mock(EntityService.class);
    final UpgradeManager upgradeManager =
        loadContext("bootstrapmcp_usagereporting/test.yaml", entityService);

    // run the upgrade
    upgradeManager.execute(OP_CONTEXT, "BootstrapMCP", List.of());

    ArgumentCaptor<AspectsBatchImpl> batchArgumentCaptor =
        ArgumentCaptor.forClass(AspectsBatchImpl.class);

    verify(entityService, times(1))
        .ingestProposal(any(OperationContext.class), batchArgumentCaptor.capture(), eq(true));

    AspectsBatchImpl actualBatch = batchArgumentCaptor.getValue();
    assertEquals(actualBatch.getMCPItems().size(), 1);

    DataHubIngestionSourceInfo actualInfo =
        actualBatch.getMCPItems().get(0).getAspect(DataHubIngestionSourceInfo.class);

    assertEquals(actualInfo.getName(), "test-usage-reporting");
    assertEquals(actualInfo.getType(), "datahub-usage-reporting");
    assertEquals(actualInfo.getSchedule().getTimezone(), "America/Chicago");
    assertEquals(actualInfo.getSchedule().getInterval(), "9 9 * * *");

    Map<String, Object> recipe =
        OP_CONTEXT
            .getObjectMapper()
            .readValue(
                actualInfo.getConfig().getRecipe(), new TypeReference<Map<String, Object>>() {});

    Map<String, Object> rankingPolicy =
        (Map<String, Object>)
            ((Map<String, Object>) ((Map<String, Object>) recipe.get("source")).get("config"))
                .get("ranking_policy");

    assertEquals(
        rankingPolicy.get("freshness_factors"),
        List.of(
            Map.of("age_in_days", List.of(0, 1), "value", 1),
            Map.of("age_in_days", List.of(1, 2), "value", 2),
            Map.of("age_in_days", List.of(2, 3), "value", 3)));

    assertEquals(
        rankingPolicy.get("usage_percentile_factors"),
        List.of(
            Map.of("percentile", List.of(0, 10), "value", 4),
            Map.of("percentile", List.of(10, 80), "value", 5),
            Map.of("percentile", List.of(80, 100), "value", 6)));

    assertEquals(
        rankingPolicy.get("regexp_based_factors"),
        List.of(
            Map.of("regexp", ".*A.*", "value", 0.1),
            Map.of("regexp", ".*B.*", "value", 0.2),
            Map.of("regexp", ".*C.*", "value", 0.3)));

    assertEquals(
        actualInfo.getConfig().getExtraArgs().get("extra_pip_requirements"), "[\"foo\",\"bar\"]");
  }

  @Test
  public void testAcrylCloudPackageTemplateMCP() throws Exception {
    environmentVariables.set(
        TEST_VALUES_ENV,
        OP_CONTEXT
            .getObjectMapper()
            .writeValueAsString(
                Map.of(
                    "ingestion",
                    Map.of(
                        "acryl-cloud-package",
                        "acryl-datahub-cloud[datahub-usage-reporting]==0.3.7rc2"))));

    final EntityService<?> entityService = mock(EntityService.class);
    final UpgradeManager upgradeManager =
        loadContext("bootstrapmcp_usagereporting/test.yaml", entityService);

    // run the upgrade
    upgradeManager.execute(OP_CONTEXT, "BootstrapMCP", List.of());

    ArgumentCaptor<AspectsBatchImpl> batchArgumentCaptor =
        ArgumentCaptor.forClass(AspectsBatchImpl.class);

    verify(entityService, times(1))
        .ingestProposal(any(OperationContext.class), batchArgumentCaptor.capture(), eq(true));

    AspectsBatchImpl actualBatch = batchArgumentCaptor.getValue();
    assertEquals(actualBatch.getMCPItems().size(), 1);

    DataHubIngestionSourceInfo actualInfo =
        actualBatch.getMCPItems().get(0).getAspect(DataHubIngestionSourceInfo.class);

    assertEquals(
        actualInfo.getConfig().getExtraArgs().get("extra_pip_requirements"),
        "[\"acryl-datahub-cloud[datahub-usage-reporting]==0.3.7rc2\",\"polars\",\"elasticsearch==7.13.4\",\"numpy<2\",\"scipy\",\"opensearch-py==2.4.2\",\"pyarrow\"]");
  }

  private static UpgradeManager loadContext(String configFile, EntityService<?> entityService)
      throws IOException {
    // hasn't run
    when(entityService.exists(
            any(OperationContext.class), any(Urn.class), eq("dataHubUpgradeResult"), anyBoolean()))
        .thenReturn(false);

    Upgrade bootstrapUpgrade = new BootstrapMCP(OP_CONTEXT, configFile, entityService, false);
    assertFalse(bootstrapUpgrade.steps().isEmpty());
    return new DefaultUpgradeManager().register(bootstrapUpgrade);
  }
}
