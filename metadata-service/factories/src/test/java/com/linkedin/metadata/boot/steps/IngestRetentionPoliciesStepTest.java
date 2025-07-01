package com.linkedin.metadata.boot.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.retention.DataHubRetentionConfig;
import io.datahubproject.metadata.context.OperationContext;
import java.io.ByteArrayInputStream;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.annotations.Test;

public class IngestRetentionPoliciesStepTest {

  private static final String UPGRADE_ID = "ingest-retention-policies";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private static final String DEFAULT_RETENTION_YAML =
      "- entity: \"dataHubExecutionRequest\"\n"
          + "  aspect: \"dataHubExecutionRequestResult\"\n"
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

  @Test
  public void testExecuteWhenRetentionIsDisabled() throws Exception {
    final EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    final RetentionService<?> mockRetentionService = Mockito.mock(RetentionService.class);
    final OperationContext mockContext = mock(OperationContext.class);
    final PathMatchingResourcePatternResolver mockResolver =
        mock(PathMatchingResourcePatternResolver.class);

    final IngestRetentionPoliciesStep retentionStep =
        new IngestRetentionPoliciesStep(
            mockRetentionService, mockEntityService, false, false, "", mockResolver);

    retentionStep.execute(mockContext);

    // Verify that no interactions occur with retention service when retention is disabled
    Mockito.verify(mockRetentionService, Mockito.times(0)).setRetention(any(), any(), any(), any());
    Mockito.verify(mockRetentionService, Mockito.times(0)).batchApplyRetention(any(), any());
  }

  @Test
  public void testExecuteWhenAlreadyApplied() throws Exception {
    final EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    final RetentionService<?> mockRetentionService = Mockito.mock(RetentionService.class);
    final OperationContext mockContext = mock(OperationContext.class);
    final PathMatchingResourcePatternResolver mockResolver =
        mock(PathMatchingResourcePatternResolver.class);

    when(mockEntityService.exists(mockContext, UPGRADE_ID_URN, true)).thenReturn(true);

    final IngestRetentionPoliciesStep retentionStep =
        new IngestRetentionPoliciesStep(
            mockRetentionService, mockEntityService, true, true, "", mockResolver);

    retentionStep.execute(mockContext);

    Mockito.verify(mockRetentionService, Mockito.times(0)).setRetention(any(), any(), any(), any());
    Mockito.verify(mockRetentionService, Mockito.times(0)).batchApplyRetention(any(), any());
  }

  @Test
  public void testExecuteWithDefaultRetentionOnly() throws Exception {
    final EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    final RetentionService<?> mockRetentionService = Mockito.mock(RetentionService.class);
    final OperationContext mockContext = mock(OperationContext.class);
    final PathMatchingResourcePatternResolver mockResolver =
        mock(PathMatchingResourcePatternResolver.class);
    final Resource mockResource = mock(Resource.class);

    when(mockEntityService.exists(mockContext, UPGRADE_ID_URN, true)).thenReturn(false);

    // Mock resource loading
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(mockResource);
    when(mockResource.exists()).thenReturn(true);
    when(mockResource.getInputStream())
        .thenReturn(new ByteArrayInputStream(DEFAULT_RETENTION_YAML.getBytes()));

    when(mockRetentionService.setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    when(mockRetentionService.setRetention(
            any(OperationContext.class), eq("*"), eq("*"), any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    final IngestRetentionPoliciesStep retentionStep =
        new IngestRetentionPoliciesStep(
            mockRetentionService, mockEntityService, true, true, "", mockResolver);

    retentionStep.execute(mockContext);

    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any(DataHubRetentionConfig.class));

    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class), eq("*"), eq("*"), any(DataHubRetentionConfig.class));

    Mockito.verify(mockRetentionService, Mockito.times(1)).batchApplyRetention(null, null);

    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(any(), any(), any(), Mockito.eq(false));
  }

  @Test
  public void testExecuteWithoutBatchApply() throws Exception {
    final EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    final RetentionService<?> mockRetentionService = Mockito.mock(RetentionService.class);
    final OperationContext mockContext = mock(OperationContext.class);
    final PathMatchingResourcePatternResolver mockResolver =
        mock(PathMatchingResourcePatternResolver.class);
    final Resource mockResource = mock(Resource.class);

    when(mockEntityService.exists(mockContext, UPGRADE_ID_URN, true)).thenReturn(false);

    // Mock resource loading
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(mockResource);
    when(mockResource.exists()).thenReturn(true);
    when(mockResource.getInputStream())
        .thenReturn(new ByteArrayInputStream(DEFAULT_RETENTION_YAML.getBytes()));

    when(mockRetentionService.setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    when(mockRetentionService.setRetention(
            any(OperationContext.class), eq("*"), eq("*"), any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    final IngestRetentionPoliciesStep retentionStep =
        new IngestRetentionPoliciesStep(
            mockRetentionService, mockEntityService, true, false, "", mockResolver);

    retentionStep.execute(mockContext);

    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any(DataHubRetentionConfig.class));

    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class), eq("*"), eq("*"), any(DataHubRetentionConfig.class));

    Mockito.verify(mockRetentionService, Mockito.times(0)).batchApplyRetention(any(), any());
  }

  @Test
  public void testExecuteWithPluginPath() throws Exception {
    final EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    final RetentionService<?> mockRetentionService = Mockito.mock(RetentionService.class);
    final OperationContext mockContext = mock(OperationContext.class);
    final PathMatchingResourcePatternResolver mockResolver =
        mock(PathMatchingResourcePatternResolver.class);
    final Resource mockDefaultResource = mock(Resource.class);
    final Resource mockPluginResource = mock(Resource.class);

    when(mockEntityService.exists(mockContext, UPGRADE_ID_URN, true)).thenReturn(false);

    // Mock resource loading
    when(mockResolver.getResource("classpath:boot/retention.yaml")).thenReturn(mockDefaultResource);
    when(mockResolver.getResources("file:/test/plugins/**/*.{yaml,yml}"))
        .thenReturn(new Resource[] {mockPluginResource});

    // Mock default resource loading
    when(mockDefaultResource.exists()).thenReturn(true);
    when(mockDefaultResource.getInputStream())
        .thenReturn(new ByteArrayInputStream(DEFAULT_RETENTION_YAML.getBytes()));

    // Mock plugin resource loading
    String pluginYaml =
        "- entity: \"container\"\n"
            + "  aspect: \"containerProperties\"\n"
            + "  config:\n"
            + "    retention:\n"
            + "      version:\n"
            + "        maxVersions: 5\n";

    when(mockPluginResource.exists()).thenReturn(true);
    when(mockPluginResource.getInputStream())
        .thenReturn(new ByteArrayInputStream(pluginYaml.getBytes()));

    when(mockRetentionService.setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    when(mockRetentionService.setRetention(
            any(OperationContext.class), eq("*"), eq("*"), any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    when(mockRetentionService.setRetention(
            any(OperationContext.class),
            eq("container"),
            eq("containerProperties"),
            any(DataHubRetentionConfig.class)))
        .thenReturn(true);

    final IngestRetentionPoliciesStep retentionStep =
        new IngestRetentionPoliciesStep(
            mockRetentionService, mockEntityService, true, true, "/test/plugins", mockResolver);

    retentionStep.execute(mockContext);

    // Verify retention operations occur for both default and plugin configs
    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class),
            eq("dataHubExecutionRequest"),
            eq("dataHubExecutionRequestResult"),
            any(DataHubRetentionConfig.class));
    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class), eq("*"), eq("*"), any(DataHubRetentionConfig.class));
    Mockito.verify(mockRetentionService, Mockito.times(1))
        .setRetention(
            any(OperationContext.class),
            eq("container"),
            eq("containerProperties"),
            any(DataHubRetentionConfig.class));

    Mockito.verify(mockRetentionService, Mockito.times(1)).batchApplyRetention(null, null);
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(any(), any(), any(), Mockito.eq(false));
  }
}
