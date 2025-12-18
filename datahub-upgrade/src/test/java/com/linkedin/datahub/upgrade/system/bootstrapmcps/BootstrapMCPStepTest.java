package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import org.testng.annotations.Test;

public class BootstrapMCPStepTest {
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testIdGeneration() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    BootstrapMCPConfigFile.MCPTemplate template =
        BootstrapMCPUtil.resolveYamlConf(
                OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .get(0);

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template);

    assertEquals(step.id(), "bootstrap-datahub-test-v10");
  }

  @Test
  public void testIsOptionalFalse() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    BootstrapMCPConfigFile.MCPTemplate template =
        BootstrapMCPUtil.resolveYamlConf(
                OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .get(0);

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template);

    assertFalse(step.isOptional());
  }

  @Test
  public void testIsOptionalTrue() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    BootstrapMCPConfigFile.MCPTemplate template =
        BootstrapMCPUtil.resolveYamlConf(
                OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .get(1); // Second template has optional: true

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template);

    assertTrue(step.isOptional());
  }

  @Test
  public void testSkipNotPreviouslyRun() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);

    BootstrapMCPConfigFile.MCPTemplate template =
        BootstrapMCPUtil.resolveYamlConf(
                OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .get(0);

    // Mock that the step has NOT been run before
    when(mockEntityService.exists(
            any(OperationContext.class),
            eq(BootstrapStep.getUpgradeUrn("bootstrap-datahub-test-v10")),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(false);

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template);

    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipPreviouslyRun() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);

    BootstrapMCPConfigFile.MCPTemplate template =
        BootstrapMCPUtil.resolveYamlConf(
                OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .get(0);

    // Mock that the step HAS been run before
    when(mockEntityService.exists(
            any(OperationContext.class),
            eq(BootstrapStep.getUpgradeUrn("bootstrap-datahub-test-v10")),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(true);

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template);

    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipForceMode() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);

    BootstrapMCPConfigFile.MCPTemplate template =
        BootstrapMCPUtil.resolveYamlConf(
                OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
            .getBootstrap()
            .getTemplates()
            .get(2); // Third template has force: true

    // Even if previously run, force mode should not skip
    when(mockEntityService.exists(
            any(OperationContext.class), any(), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME), eq(true)))
        .thenReturn(true);

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template);

    assertFalse(step.skip(mockUpgradeContext));
  }
}
