package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.testng.SystemStub;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
public class BootstrapMCPStepValuesTrackingTest {
  private static final OperationContext OP_CONTEXT = BootstrapMCPUtilTest.OP_CONTEXT;
  private static final String DOCUMENTS_VALUES_ENV = "DATAHUB_DOCUMENTS_BOOTSTRAP_VALUES";
  private static final String DOCUMENTS_REVISION_ENV = "DATAHUB_DOCUMENTS_REVISION";
  private static final String GC_VALUES_ENV = "DATAHUB_GC_BOOTSTRAP_VALUES";
  private static final String ENABLED = "{\"enabled\": true}";
  private static final String DISABLED = "{\"enabled\": false}";

  @SystemStub private EnvironmentVariables environmentVariables;

  @BeforeMethod
  private void resetEnvironment() {
    environmentVariables.remove(DOCUMENTS_VALUES_ENV);
    environmentVariables.remove(GC_VALUES_ENV);
    environmentVariables.remove(DOCUMENTS_REVISION_ENV);
  }

  @Test
  public void testToggleOffAndBackOnRunsEachChangeAndSkipsAnUnchangedRun() throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    assertFalse(records.skips(documentsTemplate()));
    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));

    environmentVariables.set(DOCUMENTS_VALUES_ENV, DISABLED);
    assertFalse(records.skips(documentsTemplate()));
    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));

    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    assertFalse(records.skips(documentsTemplate()));
    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));
  }

  @Test
  public void testToggleOffAndBackOnAtOneVersionRunsWhenTheChartPutsTheValuesInTheId()
      throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    applyChartValues(ENABLED, "2.0.147");
    records.run(documentsTemplate());
    String firstId =
        new BootstrapMCPStep(OP_CONTEXT, mock(EntityService.class), documentsTemplate()).id();

    applyChartValues(DISABLED, "2.0.147");
    assertFalse(records.skips(documentsTemplate()));
    records.run(documentsTemplate());

    applyChartValues(ENABLED, "2.0.147");
    assertEquals(
        new BootstrapMCPStep(OP_CONTEXT, mock(EntityService.class), documentsTemplate()).id(),
        firstId);
    assertFalse(records.skips(documentsTemplate()));
    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));
  }

  @Test
  public void testAFailedWriteOfTheIdsResultStillRunsWhenTheValuesAreToggledBack()
      throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    applyChartValues(ENABLED, "2.0.147");
    records.run(documentsTemplate());

    applyChartValues(DISABLED, "2.0.147");
    String disabledId =
        new BootstrapMCPStep(OP_CONTEXT, mock(EntityService.class), documentsTemplate()).id();
    records.failWritesTo(BootstrapStep.getUpgradeUrn(disabledId));
    assertThrows(RuntimeException.class, () -> records.run(documentsTemplate()));
    records.failWritesTo(null);

    applyChartValues(ENABLED, "2.0.147");
    assertFalse(records.skips(documentsTemplate()));
  }

  @Test
  public void testANewDataHubVersionRunsEvenWhenTheValuesAreUnchanged() throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    applyChartValues(ENABLED, "2.0.147");
    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));

    applyChartValues(ENABLED, "2.0.148");
    assertFalse(records.skips(documentsTemplate()));
  }

  @Test
  public void testAResultWithoutTheLastAppliedRecordRunsOnceThenSkips() throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    records.run(documentsTemplate());
    records.forgetLastApplied();

    assertFalse(records.skips(documentsTemplate()));
    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));
  }

  @Test
  public void testAV4ResultWithoutAHashRunsOnceThenSkips() throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    records.recordWithoutHash(documentsTemplate());

    assertFalse(records.skips(documentsTemplate()));

    records.run(documentsTemplate());
    assertTrue(records.skips(documentsTemplate()));
  }

  @Test
  public void testUnsetValuesMatchAnEmptyObject() throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    records.run(documentsTemplate());

    environmentVariables.set(DOCUMENTS_VALUES_ENV, "{}");
    assertTrue(records.skips(documentsTemplate()));
  }

  @Test
  public void testAForcedRunIgnoresTheStoredHash() throws IOException {
    UpgradeRecords records = new UpgradeRecords();
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    records.run(documentsTemplate());
    environmentVariables.set(DOCUMENTS_REVISION_ENV, "{\"force\": true}");

    assertFalse(records.skips(documentsTemplate()));
  }

  @Test
  public void testTheHashIgnoresKeyOrderAndWhitespace() throws IOException {
    BootstrapMCPConfigFile.MCPTemplate template = documentsTemplate();

    environmentVariables.set(DOCUMENTS_VALUES_ENV, "{\"a\":1,\"b\":{\"x\":1,\"y\":[1,2]}}");
    String compact = BootstrapMCPUtil.valuesHash(OP_CONTEXT, template);
    environmentVariables.set(
        DOCUMENTS_VALUES_ENV, "{ \"b\" : {\"y\": [1, 2], \"x\": 1},\n \"a\": 1 }");
    assertEquals(BootstrapMCPUtil.valuesHash(OP_CONTEXT, template), compact);

    environmentVariables.set(DOCUMENTS_VALUES_ENV, "{\"a\":1,\"b\":{\"x\":1,\"y\":[2,1]}}");
    assertNotEquals(BootstrapMCPUtil.valuesHash(OP_CONTEXT, template), compact);
  }

  @Test
  public void testTemplatesWithoutTheFlagKeepRunOncePerVersion() throws IOException {
    BootstrapMCPConfigFile.MCPTemplate gc = shippedTemplate("ingestion-datahub-gc");
    assertFalse(gc.isValuesTrackedAcrossRuns());

    EntityService<?> ranBefore = mock(EntityService.class);
    when(ranBefore.exists(any(OperationContext.class), any(Urn.class), anyString(), anyBoolean()))
        .thenReturn(true);
    environmentVariables.set(GC_VALUES_ENV, "{\"truncate_indices\": false}");
    assertTrue(new BootstrapMCPStep(OP_CONTEXT, ranBefore, gc).skip(context()));

    UpgradeRecords records = new UpgradeRecords();
    records.run(gc);
    assertEquals(records.recorded.size(), 1);
    assertFalse(records.recorded.values().iterator().next().hasResult());
  }

  @Test
  public void testOnlyTheDocumentsSourceIsTrackedAndItKeepsItsVersion() throws IOException {
    BootstrapMCPConfigFile.MCPTemplate documents = documentsTemplate();

    assertTrue(documents.isValuesTrackedAcrossRuns());
    assertEquals(documents.getVersion(), "v4");
  }

  /** The chart renders the revision override from the version and a hash of the values. */
  private void applyChartValues(String values, String datahubVersion) {
    environmentVariables.set(DOCUMENTS_VALUES_ENV, values);
    environmentVariables.set(
        DOCUMENTS_REVISION_ENV,
        "{\"version\":\"" + datahubVersion + "-" + Integer.toHexString(values.hashCode()) + "\"}");
  }

  /** The upgrade results GMS holds, by urn, as the step reads and writes them. */
  private static class UpgradeRecords {
    final Map<Urn, DataHubUpgradeResult> recorded = new HashMap<>();
    final EntityService<?> entityService = mock(EntityService.class);
    private Urn failingUrn;

    UpgradeRecords() {
      when(entityService.getLatestAspect(
              any(OperationContext.class), any(Urn.class), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME)))
          .thenAnswer(invocation -> recorded.get(invocation.<Urn>getArgument(1)));
      doAnswer(
              invocation -> {
                MetadataChangeProposal proposal = invocation.getArgument(1);
                if (proposal.getEntityUrn().equals(failingUrn)) {
                  throw new IllegalStateException("write failed");
                }
                recorded.put(
                    proposal.getEntityUrn(),
                    GenericRecordUtils.deserializeAspect(
                        proposal.getAspect().getValue(),
                        proposal.getAspect().getContentType(),
                        DataHubUpgradeResult.class));
                return null;
              })
          .when(entityService)
          .ingestProposal(
              any(OperationContext.class),
              any(MetadataChangeProposal.class),
              any(AuditStamp.class),
              eq(false));
    }

    boolean skips(BootstrapMCPConfigFile.MCPTemplate template) {
      return new BootstrapMCPStep(OP_CONTEXT, entityService, template).skip(context());
    }

    void run(BootstrapMCPConfigFile.MCPTemplate template) {
      assertEquals(
          new BootstrapMCPStep(OP_CONTEXT, entityService, template)
              .executable()
              .apply(context())
              .result(),
          DataHubUpgradeState.SUCCEEDED);
    }

    void failWritesTo(Urn urn) {
      failingUrn = urn;
    }

    void forgetLastApplied() {
      recorded.keySet().removeIf(urn -> urn.toString().contains("last-applied-values"));
    }

    void recordWithoutHash(BootstrapMCPConfigFile.MCPTemplate template) {
      Urn urn =
          BootstrapStep.getUpgradeUrn(
              new BootstrapMCPStep(OP_CONTEXT, entityService, template).id());
      recorded.put(
          urn,
          new DataHubUpgradeResult().setTimestampMs(1L).setState(DataHubUpgradeState.SUCCEEDED));
    }
  }

  private static UpgradeContext context() {
    UpgradeContext context = mock(UpgradeContext.class);
    when(context.opContext()).thenReturn(OP_CONTEXT);
    return context;
  }

  private static BootstrapMCPConfigFile.MCPTemplate documentsTemplate() throws IOException {
    return shippedTemplate("ingestion-datahub-documents");
  }

  private static BootstrapMCPConfigFile.MCPTemplate shippedTemplate(String name)
      throws IOException {
    return BootstrapMCPUtil.resolveYamlConf(
            OP_CONTEXT, "bootstrap_mcps.yaml", BootstrapMCPConfigFile.class)
        .getBootstrap()
        .getTemplates()
        .stream()
        .filter(template -> name.equals(template.getName()))
        .findFirst()
        .orElseThrow()
        .withOverride(OP_CONTEXT.getObjectMapper());
  }
}
