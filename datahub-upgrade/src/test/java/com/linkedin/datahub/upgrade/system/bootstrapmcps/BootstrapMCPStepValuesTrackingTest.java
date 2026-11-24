package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import org.mockito.ArgumentCaptor;
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
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    DataHubUpgradeResult firstOn = run(documentsTemplate());
    assertTrue(skips(firstOn));

    environmentVariables.set(DOCUMENTS_VALUES_ENV, DISABLED);
    assertFalse(skips(firstOn));
    DataHubUpgradeResult off = run(documentsTemplate());
    assertTrue(skips(off));

    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    assertFalse(skips(off));
    DataHubUpgradeResult secondOn = run(documentsTemplate());
    assertEquals(secondOn.getResult(), firstOn.getResult());
  }

  @Test
  public void testAV4ResultWithoutAHashRunsOnceThenSkips() throws IOException {
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    DataHubUpgradeResult recordedBeforeTracking =
        new DataHubUpgradeResult().setTimestampMs(1L).setState(DataHubUpgradeState.SUCCEEDED);

    assertFalse(skips(recordedBeforeTracking));

    DataHubUpgradeResult afterRun = run(documentsTemplate());
    assertTrue(skips(afterRun));
  }

  @Test
  public void testUnsetValuesMatchAnEmptyObject() throws IOException {
    DataHubUpgradeResult unset = run(documentsTemplate());

    environmentVariables.set(DOCUMENTS_VALUES_ENV, "{}");
    assertTrue(skips(unset));
  }

  @Test
  public void testAForcedRunIgnoresTheStoredHash() throws IOException {
    environmentVariables.set(DOCUMENTS_VALUES_ENV, ENABLED);
    DataHubUpgradeResult stored = run(documentsTemplate());
    environmentVariables.set(DOCUMENTS_REVISION_ENV, "{\"force\": true}");

    assertFalse(skips(stored));
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

    DataHubUpgradeResult stored = run(gc);
    assertFalse(stored.hasResult());
  }

  @Test
  public void testOnlyTheDocumentsSourceIsTrackedAndItKeepsItsVersion() throws IOException {
    BootstrapMCPConfigFile.MCPTemplate documents = documentsTemplate();

    assertTrue(documents.isValuesTrackedAcrossRuns());
    assertEquals(documents.getVersion(), "v4");
  }

  private static boolean skips(DataHubUpgradeResult stored) throws IOException {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.getLatestAspect(
            any(OperationContext.class), any(Urn.class), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME)))
        .thenReturn(stored);
    return new BootstrapMCPStep(OP_CONTEXT, entityService, documentsTemplate()).skip(context());
  }

  private static DataHubUpgradeResult run(BootstrapMCPConfigFile.MCPTemplate template) {
    EntityService<?> entityService = mock(EntityService.class);
    assertEquals(
        new BootstrapMCPStep(OP_CONTEXT, entityService, template)
            .executable()
            .apply(context())
            .result(),
        DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<MetadataChangeProposal> proposal =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService)
        .ingestProposal(
            any(OperationContext.class), proposal.capture(), any(AuditStamp.class), eq(false));
    assertEquals(proposal.getValue().getAspectName(), DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    return GenericRecordUtils.deserializeAspect(
        proposal.getValue().getAspect().getValue(),
        proposal.getValue().getAspect().getContentType(),
        DataHubUpgradeResult.class);
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
