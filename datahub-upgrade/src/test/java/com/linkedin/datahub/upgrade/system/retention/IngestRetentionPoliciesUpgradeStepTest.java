package com.linkedin.datahub.upgrade.system.retention;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestRetentionPoliciesUpgradeStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private static final DataHubRetentionConfig DEFAULT_RETENTION_CONFIG =
      new DataHubRetentionConfig()
          .setRetention(new Retention().setVersion(new VersionBasedRetention().setMaxVersions(20)));

  private RetentionService<?> mockRetentionService;
  private EntityService<?> mockEntityService;
  private UpgradeContext mockContext;

  @BeforeMethod
  public void setup() {
    mockRetentionService = mock(RetentionService.class);
    mockEntityService = mock(EntityService.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockRetentionService.getRetentionConfigAtExactKey(any(), any(), any()))
        .thenReturn(Optional.empty());
    when(mockRetentionService.retentionConfigEquals(any(), any())).thenReturn(false);
    when(mockRetentionService.isRetentionPolicySystemManaged(any(), any(), any())).thenReturn(true);
  }

  private IngestRetentionPoliciesUpgradeStep createStep(
      final boolean enabled,
      final boolean applyAfterIngest,
      final boolean applyOnPolicyChange,
      final boolean overwriteNonSystemPolicies,
      final String pluginPath) {
    return new IngestRetentionPoliciesUpgradeStep(
        enabled,
        mockRetentionService,
        mockEntityService,
        applyAfterIngest,
        applyOnPolicyChange,
        overwriteNonSystemPolicies,
        pluginPath,
        new PathMatchingResourcePatternResolver());
  }

  private IngestRetentionPoliciesUpgradeStep createStep(
      final boolean enabled, final boolean applyAfterIngest, final String pluginPath) {
    return createStep(enabled, applyAfterIngest, true, false, pluginPath);
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
    verify(mockRetentionService, never()).batchApplyRetention(any(), any());
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), eq(false));
    verify(mockRetentionService, atLeastOnce()).setRetention(any(), any(), any(), any());
    verify(mockRetentionService).setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
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
  public void testExecutableCallsBatchApplyOnPolicyChangeWithoutBootstrap() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);
    when(mockRetentionService.setRetention(any(), any(), any(), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, true, false, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1)).batchApplyRetention(null, null);
  }

  @Test
  public void testExecutableSkipsBatchWhenApplyOnPolicyChangeDisabled() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);
    when(mockRetentionService.setRetention(any(), any(), any(), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, false, false, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, never()).batchApplyRetention(any(), any());
  }

  @Test
  public void testExecutableBatchOnBootstrapWhenNoPolicyUpdates() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    when(mockRetentionService.getRetentionConfigAtExactKey(any(), any(), any()))
        .thenReturn(Optional.of(DEFAULT_RETENTION_CONFIG));
    when(mockRetentionService.retentionConfigEquals(any(), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, true, false, false, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, never()).setRetention(any(), any(), any(), any());
    verify(mockRetentionService, times(1)).batchApplyRetention(null, null);
  }

  @Test
  public void testExecutableSkipsWhenUpgradeAlreadyAppliedAndConfigUnchanged() {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    when(mockRetentionService.getRetentionConfigAtExactKey(any(), any(), any()))
        .thenReturn(Optional.of(DEFAULT_RETENTION_CONFIG));
    when(mockRetentionService.retentionConfigEquals(any(), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step = createStep(true, false, "");
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, never()).setRetention(any(), any(), any(), any());
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testExecutableReappliesWhenConfigDiffersOnReUpgrade() throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    String yaml =
        "- entity: \"*\"\n"
            + "  aspect: \"*\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 10\n";
    Resource yamlResource = new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(yamlResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});
    when(mockRetentionService.getRetentionConfigAtExactKey(any(), eq("*"), eq("*")))
        .thenReturn(
            Optional.of(
                new DataHubRetentionConfig()
                    .setRetention(
                        new Retention()
                            .setVersion(new VersionBasedRetention().setMaxVersions(20)))));
    when(mockRetentionService.retentionConfigEquals(any(), any())).thenReturn(false);
    when(mockRetentionService.setRetention(any(), eq("*"), eq("*"), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, true, false, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
    verify(mockRetentionService, times(1)).batchApplyRetention(null, null);
  }

  @Test
  public void testExecutableSkipsNonSystemManagedDriftWithoutOverwrite() throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    String yaml =
        "- entity: \"*\"\n"
            + "  aspect: \"*\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 10\n";
    Resource yamlResource = new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(yamlResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});
    when(mockRetentionService.getRetentionConfigAtExactKey(any(), eq("*"), eq("*")))
        .thenReturn(
            Optional.of(
                new DataHubRetentionConfig()
                    .setRetention(
                        new Retention()
                            .setVersion(new VersionBasedRetention().setMaxVersions(20)))));
    when(mockRetentionService.retentionConfigEquals(any(), any())).thenReturn(false);
    when(mockRetentionService.isRetentionPolicySystemManaged(any(), eq("*"), eq("*")))
        .thenReturn(false);
    when(mockRetentionService.getRetentionPolicyLastWriter(any(), eq("*"), eq("*")))
        .thenReturn(Optional.of(UrnUtils.getUrn("urn:li:corpuser:operator")));

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, true, false, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, never()).setRetention(any(), any(), any(), any());
    verify(mockRetentionService, never()).batchApplyRetention(any(), any());
  }

  @Test
  public void testExecutableAppliesNonSystemDriftWhenOverwriteEnabled() throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    String yaml =
        "- entity: \"*\"\n"
            + "  aspect: \"*\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 10\n";
    Resource yamlResource = new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(yamlResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});
    when(mockRetentionService.getRetentionConfigAtExactKey(any(), eq("*"), eq("*")))
        .thenReturn(
            Optional.of(
                new DataHubRetentionConfig()
                    .setRetention(
                        new Retention()
                            .setVersion(new VersionBasedRetention().setMaxVersions(20)))));
    when(mockRetentionService.retentionConfigEquals(any(), any())).thenReturn(false);
    when(mockRetentionService.isRetentionPolicySystemManaged(any(), eq("*"), eq("*")))
        .thenReturn(false);
    when(mockRetentionService.setRetention(any(), eq("*"), eq("*"), any())).thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, true, true, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
  }

  @Test
  public void testExecutableReturnsFailedWhenYamlInvalid() throws IOException {
    Resource invalidResource = new ByteArrayResource("key: value".getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(invalidResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, true, false, "", mockResolver);

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
            true, mockRetentionService, mockEntityService, false, true, false, "", mockResolver);

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
            true, mockRetentionService, mockEntityService, false, true, false, "", mockResolver);

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
            true,
            mockRetentionService,
            mockEntityService,
            false,
            true,
            false,
            "/custom/plugins",
            mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("*"), eq("*"), any());
    verify(mockRetentionService, times(1))
        .setRetention(any(OperationContext.class), eq("dataset"), eq("schemaMetadata"), any());
  }

  @Test
  public void testExecutableScopedBatchForSpecificEntityAspect() throws IOException {
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(false);
    String yaml =
        "- entity: \"dataset\"\n"
            + "  aspect: \"schemaMetadata\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 5\n";
    Resource yamlResource = new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8));
    ResourcePatternResolver mockResolver = mock(ResourcePatternResolver.class);
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(yamlResource);
    when(mockResolver.getResources(any())).thenReturn(new Resource[] {});
    when(mockRetentionService.setRetention(any(), eq("dataset"), eq("schemaMetadata"), any()))
        .thenReturn(true);

    IngestRetentionPoliciesUpgradeStep step =
        new IngestRetentionPoliciesUpgradeStep(
            true, mockRetentionService, mockEntityService, false, true, false, "", mockResolver);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockRetentionService, times(1)).batchApplyRetention("dataset", "schemaMetadata");
    verify(mockRetentionService, never()).batchApplyRetention(null, null);
  }
}
