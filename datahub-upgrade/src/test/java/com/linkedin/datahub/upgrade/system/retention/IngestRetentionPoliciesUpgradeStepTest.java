package com.linkedin.datahub.upgrade.system.retention;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestRetentionPoliciesUpgradeStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private RetentionService<?> mockRetentionService;
  private EntityService<?> mockEntityService;
  private UpgradeContext mockContext;

  @BeforeMethod
  public void setup() {
    mockRetentionService = mock(RetentionService.class);
    mockEntityService = mock(EntityService.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.opContext()).thenReturn(OP_CONTEXT);
  }

  private IngestRetentionPoliciesUpgradeStep createStep(
      final boolean enabled, final boolean applyAfterIngest, final String pluginPath) {
    return new IngestRetentionPoliciesUpgradeStep(
        enabled,
        mockRetentionService,
        mockEntityService,
        applyAfterIngest,
        pluginPath,
        new PathMatchingResourcePatternResolver());
  }

  @Test
  public void testId() {
    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, "");
    assertEquals(step.id(), "IngestRetentionPolicies");
  }

  @Test
  public void testSkipWhenDisabled() {
    IngestRetentionPoliciesUpgradeStep step = createStep(false, false, "");
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testNoSkipWhenEnabled() {
    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, "");
    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testExecutableSucceedsAndSetsRetention() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
    verify(mockRetentionService, never()).batchApplyRetention(any(), any());
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testExecutableCallsBatchApplyWhenApplyAfterIngestTrue() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);
    when(mockRetentionService.setRetention(any(), any(), any(), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, true, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1)).batchApplyRetention(null, null);
  }

  @Test
  public void testExecutableSkipsWhenUpgradeAlreadyAppliedAndNoForceOverwrite() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, never()).setRetention(any(), any(), any(), any());
    verify(mockEntityService, never()).ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testExecutableReturnsFailedWhenYamlInvalid() throws IOException {
    Resource invalidResource = new ByteArrayResource("key: value".getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(invalidResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutableSkipsUnknownEntityInYamlAndContinues() throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);
    String yamlWithUnknownEntity =
        "- entity: \"NonExistentEntity\"\n"
            + "  aspect: \"someAspect\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 1\n"
            + "- entity: \"*\"\n"
            + "  aspect: \"*\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 20\n";
    Resource yamlResource =
        new ByteArrayResource(yamlWithUnknownEntity.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(yamlResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testExecutableReappliesWhenUpgradeAlreadyAppliedAndForceOverwrite()
      throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    String yamlWithForceOverwrite =
        "- entity: \"dataHubExecutionRequest\"\n"
            + "  aspect: \"dataHubExecutionRequestResult\"\n"
            + "  forceOverwrite: true\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 1\n";
    Resource yamlResource =
        new ByteArrayResource(yamlWithForceOverwrite.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(yamlResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any());
  }

  @Test
  public void testExecutableMergesPluginPathResources() throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);
    String pluginYaml =
        "- entity: \"dataset\"\n"
            + "  aspect: \"schemaMetadata\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 5\n";
    String defaultYaml =
        "- entity: \"*\"\n"
            + "  aspect: \"*\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 20\n";
    Resource defaultResource = new ByteArrayResource(defaultYaml.getBytes(StandardCharsets.UTF_8));
    Resource pluginResource = new ByteArrayResource(pluginYaml.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(defaultResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {pluginResource});

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, "/custom/plugins", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("dataset"), eq("schemaMetadata"), any());
  }
}
