package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.model.BootstrapMCPConfigFile;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import org.testng.annotations.Test;

public class BootstrapMCPStepTest {
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  /** Index of the template in test.yaml carrying {@code enabledEnvVar: "MY_FEATURE_ENABLED"}. */
  private static final int GATED_TEMPLATE_INDEX = 1;

  /** Same kill switch, but with {@code force: true} as well. */
  private static final int GATED_FORCED_TEMPLATE_INDEX = 2;

  private static BootstrapMCPConfigFile.MCPTemplate template(int index) throws IOException {
    return BootstrapMCPUtil.resolveYamlConf(
            OP_CONTEXT, "bootstrapmcp/test.yaml", BootstrapMCPConfigFile.class)
        .getBootstrap()
        .getTemplates()
        .get(index);
  }

  private static BootstrapMCPStep stepWithEnv(
      BootstrapMCPConfigFile.MCPTemplate template,
      EntityService<?> entityService,
      String envValue) {
    return new BootstrapMCPStep(OP_CONTEXT, entityService, template) {
      @Override
      protected String resolveEnvValue(String name) {
        return envValue;
      }
    };
  }

  @Test
  public void testEnabledEnvVarSkipsWhenFalse() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);

    BootstrapMCPConfigFile.MCPTemplate template = template(GATED_TEMPLATE_INDEX);

    assertTrue(stepWithEnv(template, mockEntityService, "false").skip(mockUpgradeContext));
    assertTrue(stepWithEnv(template, mockEntityService, "FALSE").skip(mockUpgradeContext));

    // Nothing is read or written while switched off. This is what makes the switch
    // reversible: no DataHubUpgradeResult is recorded, so the step is not considered
    // "already run" and will execute normally once the variable flips to true.
    verifyNoInteractions(mockEntityService);
  }

  /**
   * These variables are usually the same ones Spring binds to boolean properties, so the kill
   * switch has to read them the way Spring does. Otherwise {@code =0} disables the feature while
   * leaving its bootstrap enabled.
   */
  @Test
  public void testEnabledEnvVarAcceptsSpringFalseyForms() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);

    BootstrapMCPConfigFile.MCPTemplate template = template(GATED_TEMPLATE_INDEX);

    for (String value : new String[] {"off", "no", "0", " false ", "\tFALSE\n"}) {
      assertTrue(
          stepWithEnv(template, mockEntityService, value).skip(mockUpgradeContext),
          "expected '" + value + "' to be treated as off");
    }
  }

  /**
   * The kill switch has to win over {@code force: true}, otherwise a forced template could not be
   * switched off at all.
   */
  @Test
  public void testEnabledEnvVarBeatsForce() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);

    BootstrapMCPConfigFile.MCPTemplate template = template(GATED_FORCED_TEMPLATE_INDEX);
    assertTrue(template.isForce());

    assertTrue(stepWithEnv(template, mockEntityService, "false").skip(mockUpgradeContext));
    // Switched on, force still means "run even if previously run".
    when(mockEntityService.exists(any(OperationContext.class), any(), any(), eq(true)))
        .thenReturn(true);
    assertFalse(stepWithEnv(template, mockEntityService, "true").skip(mockUpgradeContext));
  }

  @Test
  public void testEnabledEnvVarDoesNotSkipWhenTrue() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockEntityService.exists(any(OperationContext.class), any(), any(), eq(true)))
        .thenReturn(false);

    assertFalse(
        stepWithEnv(template(GATED_TEMPLATE_INDEX), mockEntityService, "true")
            .skip(mockUpgradeContext));
  }

  /**
   * An unset variable must leave the template enabled — templates that never opt in have no env var
   * set either, and skipping them would disable the whole bootstrap.
   */
  @Test
  public void testEnabledEnvVarDoesNotSkipWhenUnset() throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockEntityService.exists(any(OperationContext.class), any(), any(), eq(true)))
        .thenReturn(false);

    assertFalse(
        stepWithEnv(template(GATED_TEMPLATE_INDEX), mockEntityService, null)
            .skip(mockUpgradeContext));
  }

  @Test
  public void testTemplateWithoutEnabledEnvVarFallsThroughToPreviouslyRunCheck()
      throws IOException {
    EntityService<?> mockEntityService = mock(EntityService.class);
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockEntityService.exists(any(OperationContext.class), any(), any(), eq(true)))
        .thenReturn(true);

    BootstrapMCPStep step = new BootstrapMCPStep(OP_CONTEXT, mockEntityService, template(0));

    assertTrue(step.skip(mockUpgradeContext));
  }
}
